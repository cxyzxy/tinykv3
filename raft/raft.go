// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"math/rand"
	"os"
	"sort"
	"time"

	"github.com/pingcap-incubator/tinykv/log"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

type VoteResult uint8

const (
	// VotePending indicates that the decision of the vote depends on future
	// votes, i.e. neither "yes" or "no" has reached quorum yet.
	VotePending VoteResult = 1 + iota
	// VoteLost indicates that the quorum has voted "no".
	VoteLost
	// VoteWon indicates that the quorum has voted "yes".
	VoteWon
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout       int
	randomElectionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

	tick func()
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	IsEnableLog := os.Getenv("kv_debug")
	if IsEnableLog != "false" {
		log.SetLevel(log.LOG_LEVEL_DEBUG)
	} else {
		log.SetLevel(log.LOG_LEVEL_WARN)
	}
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	raftLog := newLog(c.Storage)
	hs, config, err := c.Storage.InitialState()
	if c.peers == nil {
		c.peers = config.Nodes
	}

	if err != nil {
		panic(err)
	}

	r := &Raft{
		id:               c.ID,
		RaftLog:          raftLog,
		Prs:              make(map[uint64]*Progress),
		State:            StateFollower,
		votes:            make(map[uint64]bool),
		msgs:             nil,
		Lead:             0,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		heartbeatElapsed: 0,
		electionElapsed:  0,
		leadTransferee:   0,
		PendingConfIndex: 0,
	}

	// set peer
	for _, peer := range c.peers {
		if peer == r.id {
			r.Prs[peer] = &Progress{
				Match: raftLog.LastIndex(),
				Next:  raftLog.LastIndex() + 1,
			}
		} else {
			r.Prs[peer] = &Progress{
				Match: 0,
				Next:  raftLog.LastIndex() + 1,
			}
		}
	}

	if !IsEmptyHardState(hs) {
		r.loadState(hs)
	}

	if c.Applied > 0 {
		raftLog.appliedTo(c.Applied)
	}
	r.resetRandomElectionTimeout()
	r.becomeFollower(r.Term, None)
	return r
}

func (r *Raft) loadState(state pb.HardState) {
	if state.Commit < r.RaftLog.committed || state.Commit > r.RaftLog.LastIndex() {
		log.Panicf("%x state.commit %d is out of range [%d, %d]", r.id, state.Commit, r.RaftLog.committed, r.RaftLog.LastIndex())
	}
	r.RaftLog.commitTo(state.Commit)
	r.Term = state.Term
	r.Vote = state.Vote
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) {
	// Your Code Here (2A).
	pr := r.Prs[to]
	m := pb.Message{}
	m.To = to
	prevTerm, err1 := r.RaftLog.Term(pr.Next - 1)
	ents, err2 := r.RaftLog.getEntries(pr.Next)
	if err1 == ErrCompacted || err2 == ErrCompacted {
		err1 = nil
		err2 = nil
		// send snapshot
		m.MsgType = pb.MessageType_MsgSnapshot
		snapshot, err := r.RaftLog.storage.Snapshot()
		if err != nil {
			return
		}

		m.Snapshot = &snapshot
		// snapshot 不需要附带 prevIndex, prevTerm，反正都直接覆盖
		r.Prs[to].Next = snapshot.Metadata.Index + 1
		sIndex, sTerm := snapshot.Metadata.Index, snapshot.Metadata.Term
		log.Infof("%x [firstIndex: %d, commit: %d] sent snapshot[index: %d, term: %d] to %x",
			r.id, r.RaftLog.firstIndex(), r.RaftLog.committed, sIndex, sTerm, to)
	} else {
		m.MsgType = pb.MessageType_MsgAppend
		m.Index = pr.Next - 1
		m.LogTerm = prevTerm
		// ents 可能为空，但是无所谓，就当做心跳
		m.Entries = ents
		m.Commit = r.RaftLog.committed
	}

	if err1 != nil {
		panic(err1)
	}
	if err2 != nil {
		panic(err2)
	}

	r.send(m)
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	commit := min(r.Prs[to].Match, r.RaftLog.committed)
	m := pb.Message{
		To:      to,
		MsgType: pb.MessageType_MsgHeartbeat,
		Commit:  commit,
	}
	r.send(m)
}

// tick advances the internal logical clock by a single tick.
//func (r *Raft) tick() {
//	// Your Code Here (2A).
//}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.reset(term)
	r.tick = r.tickElection
	r.Lead = lead
	r.State = StateFollower
	log.Infof("%x became follower at term %d", r.id, r.Term)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	if r.State == StateLeader {
		panic("invalid transition [leader -> candidate]")
	}
	r.reset(r.Term + 1)
	r.tick = r.tickElection
	r.Vote = r.id
	r.State = StateCandidate
	log.Infof("%x became candidate at term %d", r.id, r.Term)
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	if r.State == StateFollower {
		panic("invalid transition [follower -> leader]")
	}

	r.reset(r.Term)
	r.tick = r.tickHeartbeat
	r.Lead = r.id
	r.State = StateLeader

	emptyEnt := pb.Entry{Data: nil}
	if !r.appendEntry(&emptyEnt) {
		log.Panicf("empty entry was dropped")
	}
	log.Infof("%x became leader at term %d", r.id, r.Term)
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).

	switch {
	case m.Term == 0:
		// local message
		// 先不处理，后面统一处理
	case m.Term > r.Term:
		log.Debugf("%x [term: %d] received a %s message with higher term from %x [term: %d]", r.id, r.Term, m.MsgType, m.From, m.Term)
		if m.MsgType == pb.MessageType_MsgAppend || m.MsgType == pb.MessageType_MsgHeartbeat || m.MsgType == pb.MessageType_MsgSnapshot {
			r.becomeFollower(m.Term, m.From)
		} else {
			// 也许是投票信息，所以 leader 为 none
			r.becomeFollower(m.Term, None)
		}
	case m.Term < r.Term:
		// reject all
		switch m.MsgType {
		case pb.MessageType_MsgAppend:
			r.send(pb.Message{
				MsgType: pb.MessageType_MsgAppendResponse,
				To:      m.From,
				Reject:  true,
			})
		case pb.MessageType_MsgRequestVote:
			r.send(pb.Message{
				MsgType: pb.MessageType_MsgRequestVoteResponse,
				To:      m.From,
				Reject:  true,
			})
		case pb.MessageType_MsgHeartbeat:
			r.send(pb.Message{
				MsgType: pb.MessageType_MsgHeartbeatResponse,
				To:      m.From,
				Reject:  true,
			})
		default:
			// ignore other cases
			log.Debugf("%x [term: %d] ignored a %s message with lower term from %x [term: %d]", r.id, r.Term, m.MsgType, m.From, m.Term)
		}
		return nil
	}

	// handle message
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.hup()
	case pb.MessageType_MsgRequestVote:
		canVote := r.Vote == m.From || (r.Vote == None && r.Lead == None)
		if canVote && r.RaftLog.isUpToDate(m.Index, m.LogTerm) {
			log.Debugf("%x [logterm: %d, index: %d, vote: %x] cast %s for %x [logterm: %d, index: %d] at term %d",
				r.id, r.RaftLog.LastTerm(), r.RaftLog.LastIndex(), r.Vote, m.MsgType, m.From, m.LogTerm, m.Index, r.Term)
			r.send(pb.Message{To: m.From, Term: m.Term, MsgType: pb.MessageType_MsgRequestVoteResponse})
			r.electionElapsed = 0
			r.Vote = m.From
		} else {
			log.Debugf("%x [logterm: %d, index: %d, vote: %x] rejected %s from %x [logterm: %d, index: %d] at term %d",
				r.id, r.RaftLog.LastTerm(), r.RaftLog.LastIndex(), r.Vote, m.MsgType, m.From, m.LogTerm, m.Index, r.Term)
			r.send(pb.Message{To: m.From, Term: r.Term, MsgType: pb.MessageType_MsgRequestVoteResponse, Reject: true})
		}
	default:
		var err error
		switch r.State {
		case StateFollower:
			err = stepFollower(r, m)
		case StateCandidate:
			err = stepCandidate(r, m)
		case StateLeader:
			err = stepLeader(r, m)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	if m.Index < r.RaftLog.committed {
		// already append, ignore
		//log.Debugf("%x Already append %d, return committed %d", r.id, m.Index, r.RaftLog.committed)
		r.send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgAppendResponse, Index: r.RaftLog.committed})
		return
	}

	if mLastIndex, ok := r.RaftLog.maybeAppend(m.Index, m.LogTerm, m.Commit, m.Entries...); ok {
		//log.Debugf("%x [logterm: %d, index: %d] accept MsgApp [logterm: %d, index: %d] from %x",
		//	r.id, r.RaftLog.zeroTermOnErr(r.RaftLog.Term(m.Index)), m.Index, m.LogTerm, m.Index, m.From)
		r.send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgAppendResponse, Index: mLastIndex})
	} else {
		// reject
		log.Debugf("%x [logterm: %d, index: %d] rejected MsgApp [logterm: %d, index: %d] from %x",
			r.id, r.RaftLog.zeroTermOnErr(r.RaftLog.Term(m.Index)), m.Index, m.LogTerm, m.Index, m.From)
		r.send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgAppendResponse, Index: m.Index, Reject: true})
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	r.RaftLog.commitTo(m.Commit)
	r.send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgHeartbeatResponse})
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	sIndex, sTerm := m.Snapshot.Metadata.Index, m.Snapshot.Metadata.Term
	if r.restore(m.Snapshot) {
		log.Infof("%x [commit: %d] restored snapshot [index: %d, term: %d]", r.id, r.RaftLog.committed, sIndex, sTerm)
		// todo etcd 返回的LastIndex，我这里觉得返回 committed
		r.send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgAppendResponse, Index: r.RaftLog.committed})
	} else {
		log.Infof("%x [commit: %d] ignored snapshot [index: %d, term: %d]", r.id, r.RaftLog.committed, sIndex, sTerm)
		r.send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgAppendResponse, Index: r.RaftLog.committed})
	}
}

func (r *Raft) restore(s *pb.Snapshot) bool {
	if s.Metadata.Index <= r.RaftLog.committed {
		return false
	}

	if r.State != StateFollower {
		// This is defense-in-depth: if the leader somehow ended up applying a
		// snapshot, it could move into a new term without moving into a
		// follower state. This should never fire, but if it did, we'd have
		// prevented damage by returning early, so log only a loud warning.
		//
		// At the time of writing, the instance is guaranteed to be in follower
		// state when this method is called.
		log.Warningf("%x attempted to restore snapshot as leader; should never happen", r.id)
		r.becomeFollower(r.Term+1, None)
		return false
	}
	// todo defense config change

	// Now go ahead and actually restore.
	if r.RaftLog.matchTerm(s.Metadata.Index, s.Metadata.Term) {
		// 不合并，应为 snapshot 的数据已经有了，所以叫 fast-forwarded commit
		log.Infof("%x [commit: %d, lastIndex: %d, lastTerm: %d] fast-forwarded commit to snapshot [index: %d, term: %d]",
			r.id, r.RaftLog.committed, r.RaftLog.LastIndex(), r.RaftLog.LastTerm(), s.Metadata.Index, s.Metadata.Term)
		r.RaftLog.commitTo(s.Metadata.Index)
		return false
	}

	r.RaftLog.restore(s)

	// todo update conf
	r.Prs = make(map[uint64]*Progress)
	for _, peer := range s.Metadata.ConfState.Nodes {
		// 不需要初始化，在 becomeLeader 里面会 reset()
		r.Prs[peer] = &Progress{}
	}

	log.Infof("%x [commit: %d, lastIndex: %d, lastTerm: %d] restored snapshot [index: %d, term: %d]",
		r.id, r.RaftLog.committed, r.RaftLog.LastIndex(), r.RaftLog.LastTerm(), s.Metadata.Index, s.Metadata.Term)
	return true
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
	r.Prs[id] = &Progress{
		Match: 0,
		// New node start sync from 1
		Next: 1,
	}
}

// removeNode remove a node from raft group
// removeNode removes a node from the Raft group.
func (r *Raft) removeNode(id uint64) {
    // 检查节点是否存在于 Prs 中
    if _, ok := r.Prs[id]; !ok {
        // 如果节点不存在，则直接返回
        return
    }

    // 从 Prs 中删除节点
    delete(r.Prs, id)

    // 如果当前节点是 Leader，并且移除的不是 Leader 自己
    if id != r.id && r.State == StateLeader {
        // 调用 maybeCommit 尝试提交日志
        if r.maybeCommit() {
            // 广播 AppendEntries 消息
            r.bcastAppend()
        }
    }

    // 如果移除的是当前 Leader 的节点，并且 Raft 当前没有足够的节点，可能需要进行选举
    if len(r.Prs) <= 1 {
        // 如果剩下的节点只有一个或没有，重置为 Follower 状态，并且清空 Leader 信息
        r.State = StateFollower
        r.Lead = None
    }
}


func (r *Raft) reset(term uint64) {
	if r.Term != term {
		r.Term = term
		r.Vote = None
	}
	r.Lead = None
	r.electionElapsed = 0
	r.heartbeatTimeout = 0
	r.resetRandomElectionTimeout()

	r.abortLeaderTransfer()

	// set peers progress
	for peer := range r.Prs {
		if peer != r.id {
			// other
			r.Prs[peer] = &Progress{
				Next: r.RaftLog.LastIndex() + 1,
			}
		} else {
			// self
			r.Prs[peer] = &Progress{
				Match: r.RaftLog.LastIndex(),
				Next:  r.RaftLog.LastIndex() + 1,
			}
		}
	}

	// reset votes
	r.resetVotes()
}

func (r *Raft) send(m pb.Message) {
	if m.From == None {
		m.From = r.id
	}

	if m.Term == None {
		m.Term = r.Term
	}

	r.msgs = append(r.msgs, m)
}

func (r *Raft) hup() {
	if r.State == StateLeader {
		log.Debugf("%x ignoring MsgHup because already leader", r.id)
		return
	}

	if !r.promotable() {
		log.Warningf("%x is unPromotable and can not campaign", r.id)
		return
	}

	log.Debugf("%x is starting a new election round at term %d", r.id, r.Term)
	r.campaign()
}

// promotable indicates whether state machine can be promoted to leader,
// which is true when its own id is in progress list.
func (r *Raft) promotable() bool {
	pr := r.Prs[r.id]
	if pr == nil {
		log.Infof("%x is unPromotable because not in progress list", r.id)
		return false
	}
	if r.RaftLog.hasPendingSnapshot() {
		log.Infof("%x is unPromotable because not has pending snapshot", r.id)
		return false
	}
	return true
}

func (r *Raft) campaign() {
	r.becomeCandidate()

	// Here vote for himself
	if _, _, res := r.poll(r.id, pb.MessageType_MsgRequestVoteResponse, true); res == VoteWon {
		// won election
		r.becomeLeader()
		return
	}

	// send votes to peers
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		log.Debugf("%x [logterm: %d, index: %d] sent %s to %x at term %d", r.id, r.RaftLog.LastTerm(), r.RaftLog.LastIndex(), pb.MessageType_MsgRequestVote, id, r.Term)
		r.send(pb.Message{
			MsgType: pb.MessageType_MsgRequestVote,
			To:      id,
			LogTerm: r.RaftLog.LastTerm(),
			Index:   r.RaftLog.LastIndex(),
		})
	}
}

func (r *Raft) poll(id uint64, t pb.MessageType, v bool) (granted int, rejected int, result VoteResult) {
	// todo, maybe delete t
	if v {
		log.Debugf("%x received %s from %x at term %d", r.id, t, id, r.Term)
	} else {
		log.Debugf("%x received %s rejection from %x at term %d", r.id, t, id, r.Term)
	}
	r.recordVote(id, v)
	return r.tallyVotes()
}

func (r *Raft) recordVote(id uint64, v bool) {
	if r.votes[id] && !v {
		log.Panic("This should not happened in recordVote")
	}
	r.votes[id] = v
}

func (r *Raft) tallyVotes() (granted int, rejected int, voteResult VoteResult) {
	for id := range r.Prs {
		v, voted := r.votes[id]
		if !voted {
			continue
		}
		if v {
			granted++
		} else {
			rejected++
		}
	}

	target := len(r.Prs) / 2

	if granted <= target && rejected <= target {
		return granted, rejected, VotePending
	}

	if granted > target {
		return granted, rejected, VoteWon
	}

	if rejected > target {
		return granted, rejected, VoteLost
	}
	log.Debug("This should not happen in tallyVotes")
	return -1, -1, VotePending
}

func stepFollower(r *Raft, m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgPropose:
		if r.Lead == None {
			log.Debugf("%x no leader at term %d; dropping proposal", r.id, r.Term)
			return ErrProposalDropped
		}
		m.To = r.Lead
		r.send(m)
	case pb.MessageType_MsgAppend:
		r.electionElapsed = 0
		r.Lead = m.From
		r.handleAppendEntries(m)
	case pb.MessageType_MsgHeartbeat:
		r.electionElapsed = 0
		r.Lead = m.From
		r.handleHeartbeat(m)
	case pb.MessageType_MsgSnapshot:
		r.electionElapsed = 0
		r.Lead = m.From
		r.handleSnapshot(m)
	case pb.MessageType_MsgTransferLeader:
		if r.Lead == None {
			log.Infof("%x no leader at term %d; dropping leader transfer msg", r.id, r.Term)
			return nil
		}
		m.To = r.Lead
		r.send(m)
	case pb.MessageType_MsgTimeoutNow:
		log.Infof("%x [term %d] received MsgTimeoutNow from %x and starts an election to get leadership.", r.id, r.Term, m.From)
		r.hup()
	default:
		log.Warningf("%x ignore msg %v", r.id, m.MsgType)
	}
	return nil
}

func stepCandidate(r *Raft, m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgPropose:
		log.Debugf("%x no leader at term %d; dropping proposal", r.id, r.Term)
		return ErrProposalDropped
	case pb.MessageType_MsgAppend:
		r.becomeFollower(m.Term, m.From) // always m.Term == r.Term 前面step有保证
		r.handleAppendEntries(m)
	case pb.MessageType_MsgHeartbeat:
		r.becomeFollower(m.Term, m.From) // always m.Term == r.Term 前面step有保证
		r.handleHeartbeat(m)
	case pb.MessageType_MsgSnapshot:
		r.becomeFollower(m.Term, m.From) // always m.Term == r.Term 前面step有保证
		r.handleSnapshot(m)
	case pb.MessageType_MsgRequestVoteResponse:
		gr, rj, res := r.poll(m.From, m.MsgType, !m.Reject)
		log.Infof("%x has received %d %s votes and %d vote rejections", r.id, gr, m.MsgType, rj)
		switch res {
		case VoteWon:
			r.becomeLeader()
			// todo 源码 use bcastappend,
			// 如果使用 bcastheartbeat, maybe will need append later, why not use append directly?
			r.bcastAppend()
		case VoteLost:
			// todo 此处要注意，没有自增 term，而是复用了 term，和源码逻辑有不同
			r.becomeFollower(r.Term, None)
		}
	case pb.MessageType_MsgTimeoutNow:
		log.Debugf("%x [term %d state %v] ignored MsgTimeoutNow from %x", r.id, r.Term, r.State, m.From)
	default:
		log.Warningf("%x ignore msg %v", r.id, m.MsgType)
	}
	return nil
}

func stepLeader(r *Raft, m pb.Message) error {
	// These message types do not require any progress for m.From.
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		r.bcastHeartbeat()
		return nil
	case pb.MessageType_MsgPropose:
		if len(m.Entries) == 0 {
			log.Panicf("%x stepped empty MsgProp", r.id)
		}
		if r.Prs[r.id] == nil {
			// If we are not currently a member of the range (i.e. this node
			// was removed from the configuration while serving as leader),
			// drop any new proposals.
			return ErrProposalDropped
		}

		if r.leadTransferee != None {
			log.Infof("%x [term %d] transfer leadership to %x is in progress; dropping proposal", r.id, r.Term, r.leadTransferee)
			return ErrProposalDropped
		}

		for index, entry := range m.Entries {
			if entry.EntryType == pb.EntryType_EntryConfChange {
				// start conf change
				var configChange pb.ConfChange
				if err := configChange.Unmarshal(entry.Data); err != nil {
					panic(err)
				}
				alreadyPending := r.PendingConfIndex > r.RaftLog.applied
				if alreadyPending {
					log.Infof("There already a conf change at index %d (applied to %d)", r.PendingConfIndex, r.RaftLog.applied)
					// remove conf change contents
					m.Entries[index] = &pb.Entry{EntryType: pb.EntryType_EntryNormal}
				} else {
					r.PendingConfIndex = r.RaftLog.LastIndex() + uint64(index) + 1
					log.Infof("Add a pending conf change at index %d", r.PendingConfIndex)
				}

			}
		}
		if !r.appendEntry(m.Entries...) {
			return ErrProposalDropped
		}
		r.bcastAppend()
		return nil
	}

	// All other message types require a progress for m.From (pr).
	pr := r.Prs[m.From]
	if pr == nil {
		log.Debugf("%x no progress available for %x", r.id, m.From)
		return nil
	}
	switch m.MsgType {
	case pb.MessageType_MsgAppendResponse:
		if m.Reject {
			log.Debugf("%x received MsgAppResp(rejected) from %x for index %d",
				r.id, m.From, m.Index)
			nextProbeIdx := r.RaftLog.findConflictByIndex(m.Index)
			if pr.MaybeDecrTo(m.Index, nextProbeIdx) {
				log.Debugf("%x decreased next index progress of %x to [%d]", r.id, m.From, pr.Next)
				r.sendAppend(m.From)
			}
		} else {
			if pr.MaybeUpdate(m.Index) {
				if r.maybeCommit() {
					r.bcastAppend()
				} else if pr.Match < r.RaftLog.LastIndex() {
					r.sendAppend(m.From)
				}

				// Transfer leadership is in progress.
				if m.From == r.leadTransferee && pr.Match == r.RaftLog.LastIndex() {
					log.Infof("%x sent MsgTimeoutNow to %x after received MsgAppResp", r.id, m.From)
					r.sendTimeoutNow(m.From)
				}
			}
		}
	case pb.MessageType_MsgHeartbeatResponse:
		if pr.Match < r.RaftLog.LastIndex() {
			r.sendAppend(m.From)
		}
	case pb.MessageType_MsgTransferLeader:
		leadTransferee := m.From
		lastLeadTransferee := r.leadTransferee
		if lastLeadTransferee != None {
			if lastLeadTransferee == leadTransferee {
				log.Infof("%x [term %d] transfer leadership to %x is in progress, ignores request to same node %x",
					r.id, r.Term, leadTransferee, leadTransferee)
				return nil
			}
			r.abortLeaderTransfer()
			log.Infof("%x [term %d] abort previous transferring leadership to %x", r.id, r.Term, lastLeadTransferee)
		}

		if leadTransferee == r.id {
			log.Debugf("%x is already leader. Ignored transferring leadership to self", r.id)
			return nil
		}

		// start to transfer leadership
		log.Infof("%x [term %d] starts to transfer leadership to %x", r.id, r.Term, leadTransferee)
		// leadership change 要在一个 election 时间里面完成，不然就算失败
		r.electionElapsed = 0
		r.leadTransferee = leadTransferee
		if pr.Match == r.RaftLog.LastIndex() {
			r.sendTimeoutNow(leadTransferee)
			log.Infof("%x sends MsgTimeoutNow to %x immediately as %x already has up-to-date log", r.id, leadTransferee, leadTransferee)
		} else {
			log.Infof("%x can't sends MsgTimeoutNow to %x immediately as %x has outdated log, need append", r.id, leadTransferee, leadTransferee)
			r.sendAppend(leadTransferee)
		}
	case pb.MessageType_MsgRequestVoteResponse:
		// ignore for later request vote response, you are already be a leader.
	default:
		log.Warningf("%x ignore msg %v", r.id, m.MsgType)
	}
	return nil
}

func (r *Raft) sendTimeoutNow(to uint64) {
	r.send(pb.Message{To: to, MsgType: pb.MessageType_MsgTimeoutNow})
}

func (r *Raft) abortLeaderTransfer() {
	r.leadTransferee = None
}

func (r *Raft) Committed() uint64 {
	matches := make([]uint64, len(r.Prs))
	i := 0
	for _, pr := range r.Prs {
		matches[i] = pr.Match
		i++
	}
	sort.Slice(matches, func(i, j int) bool {
		return matches[i] < matches[j]
	})

	commit := matches[(len(matches)-1)/2]
	return commit
}

func (r *Raft) maybeCommit() bool {
	commit := r.Committed()
	return r.RaftLog.maybeCommit(commit, r.Term)
}

func (r *Raft) bcastHeartbeat() {
	for k := range r.Prs {
		if k == r.id {
			continue
		}
		r.sendHeartbeat(k)
	}
}

func (r *Raft) bcastAppend() {
	for peer := range r.Prs {
		if peer == r.id {
			continue
		}
		r.sendAppend(peer)
	}
}

func (r *Raft) resetRandomElectionTimeout() {
	ra := rand.New(rand.NewSource(time.Now().UnixNano()))
	r.randomElectionTimeout = r.electionTimeout + ra.Intn(r.electionTimeout)
}

func (r *Raft) tickElection() {
	r.electionElapsed++
	if r.pastElectionTimeout() {
		r.electionElapsed = 0
		r.Step(pb.Message{From: r.id, MsgType: pb.MessageType_MsgHup})
	}
}

// only leader use it.
func (r *Raft) tickHeartbeat() {
	r.heartbeatElapsed++
	// 用于检测 leader change 是否超时
	r.electionElapsed++

	if r.State != StateLeader {
		return
	}

	if r.electionElapsed >= r.electionTimeout {
		r.electionElapsed = 0
		// If current leader cannot transfer leadership in electionTimeout, it becomes leader again.
		if r.State == StateLeader && r.leadTransferee != None {
			r.abortLeaderTransfer()
		}
	}

	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		err := r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat})
		if err != nil {
			panic(err)
		}
	}
}

func (r *Raft) pastElectionTimeout() bool {
	return r.electionElapsed >= r.randomElectionTimeout
}

func (r *Raft) resetVotes() {
	r.votes = map[uint64]bool{}
}

func (r *Raft) appendEntry(ents ...*pb.Entry) (accepted bool) {
	li := r.RaftLog.LastIndex()
	for i := range ents {
		ents[i].Term = r.Term
		ents[i].Index = li + 1 + uint64(i)
	}

	li = r.RaftLog.append(ents...)

	r.Prs[r.id].MaybeUpdate(li)
	r.maybeCommit()
	return true
}

func (r *Raft) softState() *SoftState {
	return &SoftState{
		Lead:      r.Lead,
		RaftState: r.State,
	}
}

func (r *Raft) hardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
}

func (r *Raft) advance(rd Ready) {
	if newApplied := rd.appliedCursor(); newApplied > 0 {
		//oldApplied := r.RaftLog.applied
		r.RaftLog.appliedTo(newApplied)
	}

	if len(rd.Entries) > 0 {
		end := rd.Entries[len(rd.Entries)-1]
		r.RaftLog.stableTo(end.Index)
	}
	if !IsEmptySnap(&rd.Snapshot) {
		r.RaftLog.stableSnapTo(rd.Snapshot.Metadata.Index)
		r.RaftLog.pendingSnapshot = nil
	}
	r.RaftLog.maybeCompact()
}
