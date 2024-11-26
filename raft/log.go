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
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	// all index > dummyIndex
	dummyIndex uint64
}
func (log *RaftLog) allEntries() []pb.Entry {
    return log.entries
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	if storage == nil {
		log.Panic("storage must not be nil")
	}

	firstIndex, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}

	lastIndex, err := storage.LastIndex()
	if err != nil {
		panic(err)
	}

	entries, err := storage.Entries(firstIndex, lastIndex+1)
	if err != nil {
		panic(err)
	}

	raftLog := &RaftLog{
		storage:         storage,
		committed:       firstIndex - 1,
		applied:         firstIndex - 1,
		stabled:         lastIndex,
		entries:         entries,
		pendingSnapshot: nil,
		dummyIndex:      firstIndex - 1,
	}
	return raftLog
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
	first, err := l.storage.FirstIndex()
	if err != nil {
		panic("This should not happen.")
	}
	if first > l.firstIndex() && len(l.entries) > 0 {
		entries := l.entries[first-l.firstIndex():]
		l.entries = make([]pb.Entry, len(entries))
		copy(l.entries, entries)
		l.dummyIndex = first - 1
	}
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		return nil
	}

	unstableOffset := l.stabled - l.dummyIndex
	return l.entries[unstableOffset:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	off := max(l.applied+1, l.firstIndex())
	if l.committed >= off {
		ents, _, err := l.slice(off, l.committed+1)
		if err != nil {
			log.Panicf("unexpected error when getting un applied entries (%v)", err)
		}
		return ents
	}
	return nil
}

func (l *RaftLog) hasNextEnts() bool {
	off := max(l.applied+1, l.firstIndex())
	return l.committed+1 > off
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if i, ok := l.maybeLastIndex(); ok {
		return i
	}
	i, err := l.storage.LastIndex()
	if err != nil {
		panic(err)
	}
	return i
}

func (l *RaftLog) maybeLastIndex() (uint64, bool) {
	var index uint64 = 0
	existed := false
	if len(l.entries) > 0 {
		index = l.entries[len(l.entries)-1].Index
		existed = true
	}

	if l.pendingSnapshot != nil {
		index = max(index, l.pendingSnapshot.Metadata.Index)
		existed = true
	}
	return index, existed
}

func (l *RaftLog) LastTerm() uint64 {
	li := l.LastIndex()
	term, err := l.Term(li)
	if err != nil {
		panic(err)
	}
	return term
}

func (l *RaftLog) firstIndex() uint64 {
	// 不包含 dummyIndex
	return l.dummyIndex + 1
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if i == 0 {
		return 0, nil
	}

	if i < l.dummyIndex {
		return 0, ErrCompacted
	}

	if i > l.LastIndex() {
		return 0, ErrUnavailable
	}

	if l.pendingSnapshot != nil && l.pendingSnapshot.Metadata.Index == i {
		return l.pendingSnapshot.Metadata.Term, nil
	}

	if i == l.dummyIndex {
		term, err := l.storage.Term(i)
		if err != nil {
			return 0, err
		}
		return term, nil
	}

	offset := l.firstIndex()
	return l.entries[i-offset].Term, nil
}

func (l *RaftLog) getEntries(i uint64) ([]*pb.Entry, error) {
	if i > l.LastIndex() {
		return nil, nil
	}
	_, ents, err := l.slice(i, l.LastIndex()+1)
	return ents, err
}

// get  lo <= ents < hi
func (l *RaftLog) slice(lo, hi uint64) ([]pb.Entry, []*pb.Entry, error) {
	err := l.mustCheckOutOfBounds(lo, hi)
	if err != nil {
		return nil, nil, err
	}
	if lo == hi {
		return nil, nil, nil
	}

	offset := l.firstIndex()

	// [lo, hi)
	ents := l.entries[lo-offset : hi-offset]

	pEnts := make([]*pb.Entry, len(ents))
	for index := range ents {
		pEnts[index] = &ents[index]
	}
	return ents, pEnts, nil
}

func (l *RaftLog) mustCheckOutOfBounds(lo, hi uint64) error {
	if lo > hi {
		log.Panicf("invalid slice %d > %d", lo, hi)
	}
	fi := l.firstIndex()
	if lo < fi {
		return ErrCompacted
	}
	li := l.LastIndex()
	if hi > li+1 {
		return ErrUnavailable
	}
	return nil
}

func (l *RaftLog) isUpToDate(lastIndex uint64, term uint64) bool {
	return term > l.LastTerm() || (term == l.LastTerm() && lastIndex >= l.LastIndex())
}

func (l *RaftLog) appliedTo(i uint64) {
	if i == 0 {
		return
	}
	if l.committed < i || i < l.applied {
		log.Panicf("applied(%d) is out of range [prevApplied(%d), committed(%d)]", i, l.applied, l.committed)
	}
	l.applied = i
}

func (l *RaftLog) append(ents ...*pb.Entry) uint64 {
	if len(ents) == 0 {
		return l.LastIndex()
	}

	if after := ents[0].Index; after <= l.committed {
		log.Panicf("after(%d) <= committed(%d), it rewrites committed data", after, l.committed)
	}
	l.truncateAndAppend(ents)
	return l.LastIndex()
}

func (l *RaftLog) truncateAndAppend(ents []*pb.Entry) {
	after := ents[0].Index
	if after <= l.committed {
		log.Panicf("can't append entry(%d) before committed(%d)", after, l.committed)
		return
	}

	first := l.firstIndex()
	switch {
	case after-first == uint64(len(l.entries)):
		// directly append
		for i := range ents {
			l.entries = append(l.entries, *ents[i])
		}
	default:
		// 截断 after 后面所有，然后直接append
		// 符合的 ent 已经在前面的 findConflict 中移除，这里可以直接截断
		l.entries = l.entries[:after-first]
		for i := range ents {
			l.entries = append(l.entries, *ents[i])
		}
		if l.stabled >= after {
			l.stabled = after - 1
		}
	}
}

// maybeAppend returns (0, false) if the entries cannot be appended. Otherwise,
// it returns (last index of new entries, true).
func (l *RaftLog) maybeAppend(prevIndex, prevTerm, committed uint64, ents ...*pb.Entry) (lastNewIndex uint64, ok bool) {
	if l.matchTerm(prevIndex, prevTerm) {
		lastNewIndex = prevIndex + uint64(len(ents))
		ci := l.findConflict(ents)
		switch {
		case ci == 0:
			// do nothing, 说明 ents 里面所有的数据本来就有，不用重新写
		case ci <= l.committed:
			log.Panicf("entry %d conflict with committed entry [committed(%d)]", ci, l.committed)
		default:
			offset := prevIndex + 1
			l.append(ents[ci-offset:]...)
		}
		l.commitTo(min(committed, lastNewIndex))
		return lastNewIndex, true
	}
	return 0, false
}

// 找到第一个冲突的元素，
func (l *RaftLog) findConflict(ents []*pb.Entry) uint64 {
	for _, ent := range ents {
		if !l.matchTerm(ent.Index, ent.Term) {
			return ent.Index
		}
	}
	return 0
}

func (l *RaftLog) commitTo(toCommit uint64) {
	if l.committed < toCommit {
		if l.LastIndex() < toCommit {
			log.Panicf("toCommit(%d) is out of range [lastIndex(%d)]. Was the raft log corrupted, truncated, or lost?", toCommit, l.LastIndex())
		}
		l.committed = toCommit
	}
}

func (l *RaftLog) restore(s *pb.Snapshot) {
	log.Infof("log starts to restore snapshot [index: %d, term: %d]", s.Metadata.Index, s.Metadata.Term)
	//l.commitTo(s.Metadata.Index)
	// 不能使用 commitTo
	l.committed = s.Metadata.Index
	l.appliedTo(s.Metadata.Index)
	l.stableTo(s.Metadata.Index)
	l.dummyIndex = s.Metadata.Index
	l.entries = nil
	l.pendingSnapshot = s
}

// 找到该 index 的 term 的第一个 index
func (l *RaftLog) findConflictByIndex(index uint64) uint64 {
	if li := l.LastIndex(); index > li {
		log.Panicf("index(%d) is out of range [0, lastIndex(%d)] in findConflictByTerm",
			index, li)
		return index
	}
	term, err := l.Term(index)
	if err == ErrCompacted {
		return l.dummyIndex
	} else if err != nil {
		panic(err)
	}
	index--
	for {
		logTerm, err := l.Term(index)
		if logTerm < term || err != nil {
			break
		}
		index--
	}
	return index + 1
}

func (l *RaftLog) matchTerm(i, term uint64) bool {
	t, err := l.Term(i)
	if err != nil {
		return false
	}
	return term == t
}

func (l *RaftLog) maybeCommit(toCommit uint64, term uint64) bool {
	if toCommit > l.committed && l.zeroTermOnErr(l.Term(toCommit)) == term {
		if l.LastIndex() < toCommit {
			log.Panicf("tocommit(%d) is out of range [lastIndex(%d)]. Was the raft log corrupted, truncated, or lost?", toCommit, l.LastIndex())
		}
		l.committed = toCommit
		return true
	}
	return false
}

func (l *RaftLog) zeroTermOnErr(t uint64, err error) uint64 {
	if err == nil {
		return t
	}
	log.Infof("zeroTermOnErr, index: %d, err: %v", t, err)
	return 0
}

func (l *RaftLog) hasPendingSnapshot() bool {
	return l.pendingSnapshot != nil && l.pendingSnapshot.Metadata.Index != 0
}

func (l *RaftLog) stableTo(index uint64) {
	l.stabled = index
}

func (l *RaftLog) stableSnapTo(i uint64) {
	firstIndex := l.firstIndex()
	if i >= firstIndex {
		// can snapshot
		if len(l.entries) > 0 {
			// truncate entries
			newEntries := l.entries[i-firstIndex+1:]
			l.entries = make([]pb.Entry, len(newEntries))
			copy(l.entries, newEntries)
		}
		l.dummyIndex = i
	}
}
