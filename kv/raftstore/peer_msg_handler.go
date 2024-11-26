package raftstore

import (
	"encoding/hex"
	"fmt"
	"reflect"
	"time"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"

	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/btree"
	"github.com/pingcap/errors"
)

type PeerTick int

const (
	PeerTickRaft               PeerTick = 0
	PeerTickRaftLogGC          PeerTick = 1
	PeerTickSplitRegionCheck   PeerTick = 2
	PeerTickSchedulerHeartbeat PeerTick = 3
)

type peerMsgHandler struct {
	*peer
	ctx *GlobalContext
}

func newPeerMsgHandler(peer *peer, ctx *GlobalContext) *peerMsgHandler {
	return &peerMsgHandler{
		peer: peer,
		ctx:  ctx,
	}
}

func (d *peerMsgHandler) HandleRaftReady() {
	if d.stopped {
		return
	}

	// Your Code Here (2B).
	if d.RaftGroup.HasReady() {
		ready := d.RaftGroup.Ready()
		var applySnapResult *ApplySnapResult = nil
		var err error
		if applySnapResult, err = d.peerStorage.SaveReadyState(&ready); err != nil {
			panic(err)
		}
		if applySnapResult != nil && !reflect.DeepEqual(applySnapResult.PrevRegion, applySnapResult.Region) {
			log.Infof("%s Apply snapshot, from %v -> %v", d.Tag, applySnapResult.PrevRegion, applySnapResult.Region)
			storeMeta := d.ctx.storeMeta
			storeMeta.Lock()
			// todo remove prev region
			if len(applySnapResult.PrevRegion.Peers) > 0 {
				storeMeta.regionRanges.Delete(&regionItem{region: applySnapResult.PrevRegion})
			} else {
				log.Warningf("%s Don't delete regionRanges when apply snapshot, because it's a new node", d.Tag)
			}
			storeMeta.setRegion(applySnapResult.Region, d.peer)
			storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: applySnapResult.Region})
			storeMeta.Unlock()
		}

		// send data to peers, can use async?
		d.Send(d.ctx.trans, ready.Messages)

		// persist commit data
		if len(ready.CommittedEntries) > 0 {
			//d.proposals
			kvWB := new(engine_util.WriteBatch)
			for _, v := range ready.CommittedEntries {
				d.handleCommittedEntry(&v, kvWB)
				if d.stopped {
					return
				}
			}

			// update apply index
			d.peerStorage.applyState.AppliedIndex = ready.CommittedEntries[len(ready.CommittedEntries)-1].Index

			// persist apply state and kvs
			if err = kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState); err != nil {
				panic(err)
			}
			kvWB.MustWriteToDB(d.peerStorage.Engines.Kv)
		}

		d.RaftGroup.Advance(ready)
	}
}

func (d *peerMsgHandler) handleCommittedEntry(entry *pb.Entry, kvWB *engine_util.WriteBatch) {
	// todo maybe error 应该先持久化，再 response
	var resp *raft_cmdpb.RaftCmdResponse = nil
	var txn *badger.Txn = nil

	msg := &raft_cmdpb.RaftCmdRequest{}
	switch entry.EntryType {
	case pb.EntryType_EntryNormal:
		if err := msg.Unmarshal(entry.Data); err != nil {
			panic(err)
		}
	case pb.EntryType_EntryConfChange:
		confChange := &pb.ConfChange{}
		if err := confChange.Unmarshal(entry.Data); err != nil {
			panic(err)
		}
		if err := msg.Unmarshal(confChange.Context); err != nil {
			panic(err)
		}
	}

	if ok, errEpochNotMatching := maybeCheckRegionEpoch(msg, d.Region(), true); ok {
		log.Infof("%s entry %v does not match this region epoch when applying", d.Tag, entry.EntryType)
		resp = ErrResp(errEpochNotMatching)
		d.responseEntry(entry, resp, txn)
		return
	}

	if ok, key := maybeGetRequestKey(msg); ok {
		if err := util.CheckKeyInRegion(key, d.Region()); err != nil {
			log.Infof("%s key %v does not in this region when applying", d.Tag, key)
			resp = ErrResp(err)
			d.responseEntry(entry, resp, txn)
			return
		}
	}

	switch entry.EntryType {
	case pb.EntryType_EntryNormal:
		if msg.AdminRequest != nil {
			resp = d.handleCommittedAdminEntry(msg, entry, kvWB)
		} else {
			resp, txn = d.handleCommittedNormalEntry(msg, entry, kvWB)
		}
	case pb.EntryType_EntryConfChange:
		resp = d.handleCommittedConfChangeEntry(msg, entry, kvWB)
	default:
		log.Error("This should not happened")
	}
	d.responseEntry(entry, resp, txn)
}

func (d *peerMsgHandler) handleCommittedNormalEntry(msg *raft_cmdpb.RaftCmdRequest, entry *pb.Entry, kvWB *engine_util.WriteBatch) (*raft_cmdpb.RaftCmdResponse, *badger.Txn) {
	if len(msg.Requests) == 0 {
		log.Infof("%s Ignore an empty entry", d.Tag)
		return nil, nil
	}
	if len(msg.Requests) > 1 {
		panic("More requests")
	}

	// used for SnapGet
	var txn *badger.Txn = nil

	request := msg.Requests[0]
	response := &raft_cmdpb.RaftCmdResponse{}
	switch request.CmdType {
	case raft_cmdpb.CmdType_Invalid:
		log.Panicf("Invalid")
	case raft_cmdpb.CmdType_Put:
		kvWB.SetCF(request.Put.Cf, request.Put.Key, request.Put.Value)
		d.SizeDiffHint += uint64(len(request.Put.Key) + len(request.Put.Value))
		response.Responses = []*raft_cmdpb.Response{{
			CmdType: raft_cmdpb.CmdType_Put,
			Put:     &raft_cmdpb.PutResponse{},
		}}
	case raft_cmdpb.CmdType_Delete:
		kvWB.SetCF(request.Delete.Cf, request.Delete.Key, nil)
		d.SizeDiffHint += uint64(len(request.Delete.Key))
		response.Responses = []*raft_cmdpb.Response{{
			CmdType: raft_cmdpb.CmdType_Delete,
			Delete:  &raft_cmdpb.DeleteResponse{},
		}}
	case raft_cmdpb.CmdType_Get:
		// 开始 get 时要把之前的 WriteBatch 全部写入，并且更新 applyIndex
		d.peerStorage.applyState.AppliedIndex = entry.Index
		err := kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
		if err != nil {
			panic(err)
		}
		kvWB.MustWriteToDB(d.peerStorage.Engines.Kv)
		kvWB.Reset()
		value, err := engine_util.GetCF(d.peerStorage.Engines.Kv, request.Get.GetCf(), request.Get.GetKey())
		if err != nil {
			value = nil
		}
		response.Responses = []*raft_cmdpb.Response{{
			CmdType: raft_cmdpb.CmdType_Get,
			Get: &raft_cmdpb.GetResponse{
				Value: value,
			},
		}}
	case raft_cmdpb.CmdType_Snap:
		// 开始 snap 时要把之前的 WriteBatch 全部写入，并且更新 applyIndex
		d.peerStorage.applyState.AppliedIndex = entry.Index
		err := kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
		if err != nil {
			panic(err)
		}
		kvWB.MustWriteToDB(d.peerStorage.Engines.Kv)
		kvWB.Reset()

		response.Responses = []*raft_cmdpb.Response{{
			CmdType: raft_cmdpb.CmdType_Snap,
			Snap: &raft_cmdpb.SnapResponse{
				Region: d.Region(),
			},
		}}
		txn = d.peerStorage.Engines.Kv.NewTransaction(false)
	default:
		log.Panic("This should not happen.")
	}
	return response, txn
}

func (d *peerMsgHandler) handleCommittedConfChangeEntry(msg *raft_cmdpb.RaftCmdRequest, entry *pb.Entry, kvWB *engine_util.WriteBatch) *raft_cmdpb.RaftCmdResponse {
	changePeer := msg.AdminRequest.ChangePeer
	targetPeer := changePeer.Peer

	region := d.Region()
	peerIndex := findPeerIndex(region, targetPeer.Id)
	region.RegionEpoch.ConfVer++

	switch changePeer.ChangeType {
	case pb.ConfChangeType_AddNode:
		if peerIndex != -1 {
			// already in, ignore
			return nil
		}
		region.Peers = append(region.Peers, changePeer.Peer)

		d.insertPeerCache(targetPeer)
		log.Infof("%s Add peer %d", d.Tag, targetPeer.Id)
	case pb.ConfChangeType_RemoveNode:
		if peerIndex == -1 {
			// already remove, ignore
			return nil
		}
		if targetPeer.Id == d.PeerId() {
			log.Infof("%s remove peer %d", d.Tag, changePeer.Peer.Id)
			d.startToDestroyPeer()
			return nil
		}

		region.Peers = append(region.Peers[:peerIndex], region.Peers[peerIndex+1:]...)

		d.removePeerCache(targetPeer.Id)
		log.Infof("%s remove peer %d", d.Tag, changePeer.Peer.Id)
	}
	// persist
	meta.WriteRegionState(kvWB, region, rspb.PeerState_Normal)

	configChangeEntry := pb.ConfChange{}
	if err := configChangeEntry.Unmarshal(entry.Data); err != nil {
		panic(err)
	}
	d.RaftGroup.ApplyConfChange(configChangeEntry)
	log.Infof("%s apply conf change: %v", d.Tag, msg.AdminRequest)

	d.notifyHeartbeatScheduler(region, d.peer)

	return &raft_cmdpb.RaftCmdResponse{AdminResponse: &raft_cmdpb.AdminResponse{
		CmdType:    raft_cmdpb.AdminCmdType_ChangePeer,
		ChangePeer: &raft_cmdpb.ChangePeerResponse{},
	}}
}

func (d *peerMsgHandler) handleCommittedAdminEntry(msg *raft_cmdpb.RaftCmdRequest, entry *pb.Entry, kvWB *engine_util.WriteBatch) *raft_cmdpb.RaftCmdResponse {
	// handle admin raft
	switch msg.AdminRequest.CmdType {
	case raft_cmdpb.AdminCmdType_InvalidAdmin:
		log.Panicf("This should not happen.")
	case raft_cmdpb.AdminCmdType_ChangePeer:
		log.Panicf("This should not happen.")
	case raft_cmdpb.AdminCmdType_CompactLog:
		// do not need to respond entry
		compactLog := msg.AdminRequest.CompactLog
		applyState := d.peerStorage.applyState
		if compactLog.CompactIndex <= applyState.TruncatedState.Index {
			// already compacted
			return nil
		}
		if compactLog.CompactTerm < applyState.TruncatedState.Term {
			panic("This should not happen.")
		}
		applyState.TruncatedState.Index = compactLog.CompactIndex
		applyState.TruncatedState.Term = compactLog.CompactTerm
		err := kvWB.SetMeta(meta.ApplyStateKey(d.regionId), applyState)
		if err != nil {
			panic(err)
		}
		d.ScheduleCompactLog(applyState.TruncatedState.Index)
		return &raft_cmdpb.RaftCmdResponse{AdminResponse: &raft_cmdpb.AdminResponse{
			CmdType:    raft_cmdpb.AdminCmdType_CompactLog,
			CompactLog: &raft_cmdpb.CompactLogResponse{},
		}}
	case raft_cmdpb.AdminCmdType_TransferLeader:
		log.Panicf("This should not happen.")
	case raft_cmdpb.AdminCmdType_Split:
		splitReq := msg.AdminRequest.Split
		if len(d.Region().Peers) != len(splitReq.NewPeerIds) {
			// todo ugly code, fix bug
			log.Warningf("%s len(d.Region().Peers) != len(splitReq.NewPeerIds), region: %v, msg: %v", d.Tag, d.Region(), msg)
			return ErrRespStaleCommand(entry.Term)
		}
		if d.IsLeader() {
			log.Infof("%s start to commit split region request in store %d, split key is %v, msg %v", d.Tag, d.ctx.store.Id, hex.EncodeToString(splitReq.SplitKey), msg)
		}

		// copy region
		originRegion := d.Region()
		leftRegion := new(metapb.Region)
		if err := util.CloneMsg(originRegion, leftRegion); err != nil {
			panic(err)
		}
		rightRegion := new(metapb.Region)
		if err := util.CloneMsg(originRegion, rightRegion); err != nil {
			panic(err)
		}

		leftRegion.RegionEpoch.Version++
		rightRegion.RegionEpoch.Version++
		newPeers := make([]*metapb.Peer, len(splitReq.NewPeerIds))
		for i, peer := range leftRegion.Peers {
			newPeers[i] = &metapb.Peer{
				Id:      splitReq.NewPeerIds[i],
				StoreId: peer.StoreId,
			}
		}

		rightRegion.Id = splitReq.NewRegionId
		rightRegion.StartKey = splitReq.SplitKey
		rightRegion.EndKey = leftRegion.EndKey
		rightRegion.Peers = newPeers

		leftRegion.EndKey = splitReq.SplitKey

		// persist before setRegion
		meta.WriteRegionState(kvWB, leftRegion, rspb.PeerState_Normal)
		meta.WriteRegionState(kvWB, rightRegion, rspb.PeerState_Normal)

		// create new peer & register into router
		newPeer, err := createPeer(d.ctx.store.Id, d.ctx.cfg, d.ctx.regionTaskSender, d.ctx.engine, rightRegion)
		if err != nil {
			panic(err)
		}
		d.ctx.router.register(newPeer)
		if err := d.ctx.router.send(rightRegion.Id, message.Msg{
			Type: message.MsgTypeStart,
		}); err != nil {
			panic("This should not happen")
		}

		storeMeta := d.ctx.storeMeta
		storeMeta.Lock()
		defer storeMeta.Unlock()

		storeMeta.regionRanges.Delete(&regionItem{region: originRegion})
		storeMeta.setRegion(leftRegion, d.peer)
		storeMeta.setRegion(rightRegion, newPeer)
		storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: leftRegion})
		storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: rightRegion})

		if d.IsLeader() {
			log.Infof("%s split region into [%s - %s] & [%s - %s] success.", d.Tag,
				hex.EncodeToString(leftRegion.GetStartKey()), hex.EncodeToString(leftRegion.GetEndKey()),
				hex.EncodeToString(rightRegion.GetStartKey()), hex.EncodeToString(rightRegion.GetEndKey()))
		}

		// notify new region created
		d.notifyHeartbeatScheduler(leftRegion, d.peer)
		d.notifyHeartbeatScheduler(rightRegion, newPeer)

		return &raft_cmdpb.RaftCmdResponse{AdminResponse: &raft_cmdpb.AdminResponse{
			CmdType: raft_cmdpb.AdminCmdType_Split,
			Split: &raft_cmdpb.SplitResponse{
				Regions: []*metapb.Region{leftRegion, rightRegion},
			},
		}}
	default:
		log.Panicf("This should not happen.")
	}
	return nil
}

func (d *peerMsgHandler) notifyHeartbeatScheduler(region *metapb.Region, peer *peer) {
	clonedRegion := new(metapb.Region)
	err := util.CloneMsg(region, clonedRegion)
	if err != nil {
		return
	}
	d.ctx.schedulerTaskSender <- &runner.SchedulerRegionHeartbeatTask{
		Region:          clonedRegion,
		Peer:            peer.Meta,
		PendingPeers:    peer.CollectPendingPeers(),
		ApproximateSize: peer.ApproximateSize,
	}
}

func (d *peerMsgHandler) responseEntry(entry *pb.Entry, response *raft_cmdpb.RaftCmdResponse, txn *badger.Txn) {
	if response == nil && txn == nil {
		log.Infof("%s: It's a non-reply commit entry[%v]: %d-%d", d.Tag, entry.EntryType, entry.Index, entry.Term)
		return
	}

	for len(d.proposals) > 0 {
		proposal := d.proposals[0]
		if entry.Term < proposal.term {
			return
		}

		if entry.Term > proposal.term {
			log.Infof("%s: Committed entry[%d-%d] has larger term than proposal[%d-%d], reply stale", d.Tag, entry.Index, entry.Term, proposal.index, proposal.term)
			proposal.cb.Done(ErrRespStaleCommand(proposal.term))
			d.proposals = d.proposals[1:]
			continue
		}

		if entry.Term == proposal.term && entry.Index < proposal.index {
			return
		}

		if entry.Term == proposal.term && entry.Index > proposal.index {
			log.Infof("%s: Committed entry[%d-%d] has larger index than proposal[%d-%d], reply stale", d.Tag, entry.Index, entry.Term, proposal.index, proposal.term)
			proposal.cb.Done(ErrRespStaleCommand(proposal.term))
			d.proposals = d.proposals[1:]
			continue
		}

		if entry.Index == proposal.index && entry.Term == proposal.term {
			//log.Infof("reply %d-%d %v %d", entry.Index, entry.Term, proposal, len(d.proposals))
			if response.Header == nil {
				response.Header = &raft_cmdpb.RaftResponseHeader{}
			}
			proposal.cb.Txn = txn
			proposal.cb.Done(response)
			d.proposals = d.proposals[1:]
			return
		}

		panic("This should not happen.")
	}
}

func (d *peerMsgHandler) HandleMsg(msg message.Msg) {
	switch msg.Type {
	case message.MsgTypeRaftMessage:
		raftMsg := msg.Data.(*rspb.RaftMessage)
		if err := d.onRaftMsg(raftMsg); err != nil {
			log.Errorf("%s handle raft message error %v", d.Tag, err)
		}
	case message.MsgTypeRaftCmd:
		raftCMD := msg.Data.(*message.MsgRaftCmd)
		d.proposeRaftCommand(raftCMD.Request, raftCMD.Callback)
	case message.MsgTypeTick:
		d.onTick()
	case message.MsgTypeSplitRegion:
		split := msg.Data.(*message.MsgSplitRegion)
		log.Infof("%s propose a prepare split region request, split key is %v", d.Tag, split.SplitKey)
		d.onPrepareSplitRegion(split.RegionEpoch, split.SplitKey, split.Callback)
	case message.MsgTypeRegionApproximateSize:
		d.onApproximateRegionSize(msg.Data.(uint64))
	case message.MsgTypeGcSnap:
		gcSnap := msg.Data.(*message.MsgGCSnap)
		d.onGCSnap(gcSnap.Snaps)
	case message.MsgTypeStart:
		d.startTicker()
	}
}

func (d *peerMsgHandler) preProposeRaftCommand(req *raft_cmdpb.RaftCmdRequest) error {
	// Check store_id, make sure that the msg is dispatched to the right place.
	if err := util.CheckStoreID(req, d.storeID()); err != nil {
		return err
	}

	// Check whether the store has the right peer to handle the request.
	regionID := d.regionId
	leaderID := d.LeaderId()
	if !d.IsLeader() {
		leader := d.getPeerFromCache(leaderID)
		return &util.ErrNotLeader{RegionId: regionID, Leader: leader}
	}
	// peer_id must be the same as peer's.
	if err := util.CheckPeerID(req, d.PeerId()); err != nil {
		return err
	}
	// Check whether the term is stale.
	if err := util.CheckTerm(req, d.Term()); err != nil {
		return err
	}
	err := util.CheckRegionEpoch(req, d.Region(), true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		// Attach the region which might be split from the current region. But it doesn't
		// matter if the region is not split from the current region. If the region meta
		// received by the TiKV driver is newer than the meta cached in the driver, the meta is
		// updated.
		siblingRegion := d.findSiblingRegion()
		if siblingRegion != nil {
			errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
		}
		return errEpochNotMatching
	}
	return err
}

func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	if err := d.preProposeRaftCommand(msg); err != nil {
		cb.Done(ErrResp(err))
		return
	}

	if ok, key := maybeGetRequestKey(msg); ok {
		if err := util.CheckKeyInRegion(key, d.Region()); err != nil {
			cb.Done(ErrResp(err))
			return
		}
	}
	// Your Code Here (2B).
	// special case for remove node
	if msg.AdminRequest != nil && msg.AdminRequest.ChangePeer != nil {
		changePeer := msg.AdminRequest.ChangePeer
		if changePeer.ChangeType == pb.ConfChangeType_RemoveNode &&
			changePeer.Peer.Id == d.PeerId() &&
			len(d.Region().Peers) ==2 &&
			d.IsLeader() {
			log.Warningf("%s Can't propose remove node directly, transfer leader to another node and reject this.", d.Tag)
			var targetPeer uint64 = 0
			for _, peer := range d.Region().Peers {
				if peer.Id != d.PeerId() {
					targetPeer = peer.Id
					break
				}
			}
			if targetPeer == 0 {
				panic("This should not happen")
			}
			d.RaftGroup.TransferLeader(targetPeer)
			cb.Done(ErrResp(fmt.Errorf("can't propose it now")))
			return
		}
	}

	p := &proposal{
		index: d.nextProposalIndex(),
		term:  d.Term(),
		cb:    cb,
	}
	if msg.AdminRequest != nil {
		if err := d.proposeAdminRaftCommand(msg); err != nil {
			cb.Done(ErrResp(err))
			return
		}
	} else {
		if err := d.proposeNormalRaftCommand(msg); err != nil {
			cb.Done(ErrResp(err))
			return
		}
	}

	// transfer leader is a special case, it didn't need reply!!!
	if msg.AdminRequest != nil && msg.AdminRequest.TransferLeader != nil {
		resp := &raft_cmdpb.RaftCmdResponse{
			Header: &raft_cmdpb.RaftResponseHeader{},
			AdminResponse: &raft_cmdpb.AdminResponse{
				CmdType:        raft_cmdpb.AdminCmdType_TransferLeader,
				TransferLeader: &raft_cmdpb.TransferLeaderResponse{},
			},
		}
		cb.Done(resp)
		return
	}

	if cb == nil {
		// don't need to reply
		return
	}

	// normal case, need reply after commit
	d.proposals = append(d.proposals, p)
}

func (d *peerMsgHandler) proposeAdminRaftCommand(msg *raft_cmdpb.RaftCmdRequest) error {
	// handle admin raft
	req := msg.AdminRequest

	switch req.CmdType {
	case raft_cmdpb.AdminCmdType_InvalidAdmin:
		panic("This should not happen")
	case raft_cmdpb.AdminCmdType_ChangePeer:
		context, err := msg.Marshal()
		if err != nil {
			panic(err)
		}

		return d.RaftGroup.ProposeConfChange(pb.ConfChange{
			ChangeType: req.ChangePeer.ChangeType,
			NodeId:     req.ChangePeer.Peer.Id,
			Context:    context,
		})
	case raft_cmdpb.AdminCmdType_CompactLog:
		data, err := msg.Marshal()
		if err != nil {
			panic(err)
		}
		return d.RaftGroup.Propose(data)
	case raft_cmdpb.AdminCmdType_TransferLeader:
		d.RaftGroup.TransferLeader(msg.AdminRequest.TransferLeader.Peer.Id)
	case raft_cmdpb.AdminCmdType_Split:
		data, err := msg.Marshal()
		if err != nil {
			panic(err)
		}
		return d.RaftGroup.Propose(data)

	default:
		panic("This should not happen.")
	}
	return nil
}

func (d *peerMsgHandler) proposeNormalRaftCommand(msg *raft_cmdpb.RaftCmdRequest) error {
	// handle kv write/read/delete
	data, err := msg.Marshal()
	if err != nil {
		return err
	}
	return d.RaftGroup.Propose(data)
}

func (d *peerMsgHandler) onTick() {
	if d.stopped {
		return
	}
	d.ticker.tickClock()
	if d.ticker.isOnTick(PeerTickRaft) {
		d.onRaftBaseTick()
	}
	if d.ticker.isOnTick(PeerTickRaftLogGC) {
		d.onRaftGCLogTick()
	}
	if d.ticker.isOnTick(PeerTickSchedulerHeartbeat) {
		d.onSchedulerHeartbeatTick()
	}
	if d.ticker.isOnTick(PeerTickSplitRegionCheck) {
		d.onSplitRegionCheckTick()
	}
	d.ctx.tickDriverSender <- d.regionId
}

func (d *peerMsgHandler) startTicker() {
	d.ticker = newTicker(d.regionId, d.ctx.cfg)
	d.ctx.tickDriverSender <- d.regionId
	d.ticker.schedule(PeerTickRaft)
	d.ticker.schedule(PeerTickRaftLogGC)
	d.ticker.schedule(PeerTickSplitRegionCheck)
	d.ticker.schedule(PeerTickSchedulerHeartbeat)
}

func (d *peerMsgHandler) onRaftBaseTick() {
	d.RaftGroup.Tick()
	d.ticker.schedule(PeerTickRaft)
}

func (d *peerMsgHandler) ScheduleCompactLog(truncatedIndex uint64) {
	raftLogGCTask := &runner.RaftLogGCTask{
		RaftEngine: d.ctx.engine.Raft,
		RegionID:   d.regionId,
		StartIdx:   d.LastCompactedIdx,
		EndIdx:     truncatedIndex + 1,
	}
	d.LastCompactedIdx = raftLogGCTask.EndIdx
	d.ctx.raftLogGCTaskSender <- raftLogGCTask
}

func (d *peerMsgHandler) onRaftMsg(msg *rspb.RaftMessage) error {
	//log.Debugf("%s handle raft message %s from %d to %d",
	//	d.Tag, msg.GetMessage().GetMsgType(), msg.GetFromPeer().GetId(), msg.GetToPeer().GetId())
	if !d.validateRaftMessage(msg) {
		return nil
	}
	if d.stopped {
		return nil
	}
	if msg.GetIsTombstone() {
		// we receive a message tells us to remove self.
		d.handleGCPeerMsg(msg)
		return nil
	}
	if d.checkMessage(msg) {
		return nil
	}
	key, err := d.checkSnapshot(msg)
	if err != nil {
		return err
	}
	if key != nil {
		// If the snapshot file is not used again, then it's OK to
		// delete them here. If the snapshot file will be reused when
		// receiving, then it will fail to pass the check again, so
		// missing snapshot files should not be noticed.
		s, err1 := d.ctx.snapMgr.GetSnapshotForApplying(*key)
		if err1 != nil {
			return err1
		}
		d.ctx.snapMgr.DeleteSnapshot(*key, s, false)
		return nil
	}
	d.insertPeerCache(msg.GetFromPeer())
	err = d.RaftGroup.Step(*msg.GetMessage())
	if err != nil {
		return err
	}
	if d.AnyNewPeerCatchUp(msg.FromPeer.Id) {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
	return nil
}

// return false means the message is invalid, and can be ignored.
func (d *peerMsgHandler) validateRaftMessage(msg *rspb.RaftMessage) bool {
	regionID := msg.GetRegionId()
	//from := msg.GetFromPeer()
	to := msg.GetToPeer()
	//log.Debugf("[region %d] handle raft message %s from %d to %d", regionID, msg, from.GetId(), to.GetId())
	if to.GetStoreId() != d.storeID() {
		log.Warnf("[region %d] store not match, to store id %d, mine %d, ignore it",
			regionID, to.GetStoreId(), d.storeID())
		return false
	}
	if msg.RegionEpoch == nil {
		log.Errorf("[region %d] missing epoch in raft message, ignore it", regionID)
		return false
	}
	return true
}

/// Checks if the message is sent to the correct peer.
///
/// Returns true means that the message can be dropped silently.
func (d *peerMsgHandler) checkMessage(msg *rspb.RaftMessage) bool {
	fromEpoch := msg.GetRegionEpoch()
	isVoteMsg := util.IsVoteMessage(msg.Message)
	fromStoreID := msg.FromPeer.GetStoreId()

	// Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
	// a. 1 removes 2, 2 may still send MsgAppendResponse to 1.
	//  We should ignore this stale message and let 2 remove itself after
	//  applying the ConfChange log.
	// b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
	//  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
	// c. 2 is isolated but can communicate with 3. 1 removes 3.
	//  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
	// d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
	//  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
	// e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
	//  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
	//  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
	//  rejoin the raft group again.
	// f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
	//  unlike case e, 2 will be stale forever.
	// TODO: for case f, if 2 is stale for a long time, 2 will communicate with scheduler and scheduler will
	// tell 2 is stale, so 2 can remove itself.
	region := d.Region()
	if util.IsEpochStale(fromEpoch, region.RegionEpoch) && util.FindPeer(region, fromStoreID) == nil {
		// The message is stale and not in current region.
		handleStaleMsg(d.ctx.trans, msg, region.RegionEpoch, isVoteMsg)
		return true
	}
	target := msg.GetToPeer()
	if target.Id < d.PeerId() {
		log.Infof("%s target peer ID %d is less than %d, msg maybe stale", d.Tag, target.Id, d.PeerId())
		return true
	} else if target.Id > d.PeerId() {
		if d.MaybeDestroy() {
			log.Infof("%s is stale as received a larger peer %s, destroying", d.Tag, target)
			d.destroyPeer()
			d.ctx.router.sendStore(message.NewMsg(message.MsgTypeStoreRaftMessage, msg))
		}
		return true
	}
	return false
}

func handleStaleMsg(trans Transport, msg *rspb.RaftMessage, curEpoch *metapb.RegionEpoch,
	needGC bool) {
	regionID := msg.RegionId
	fromPeer := msg.FromPeer
	toPeer := msg.ToPeer
	msgType := msg.Message.GetMsgType()

	if !needGC {
		log.Infof("[region %d] raft message %s is stale, current %v ignore it",
			regionID, msgType, curEpoch)
		return
	}
	gcMsg := &rspb.RaftMessage{
		RegionId:    regionID,
		FromPeer:    toPeer,
		ToPeer:      fromPeer,
		RegionEpoch: curEpoch,
		IsTombstone: true,
	}
	if err := trans.Send(gcMsg); err != nil {
		log.Errorf("[region %d] send message failed %v", regionID, err)
	}
}

func (d *peerMsgHandler) handleGCPeerMsg(msg *rspb.RaftMessage) {
	fromEpoch := msg.RegionEpoch
	if !util.IsEpochStale(d.Region().RegionEpoch, fromEpoch) {
		return
	}
	if !util.PeerEqual(d.Meta, msg.ToPeer) {
		log.Infof("%s receive stale gc msg, ignore", d.Tag)
		return
	}
	log.Infof("%s peer %s receives gc message, trying to remove", d.Tag, msg.ToPeer)
	if d.MaybeDestroy() {
		d.destroyPeer()
	}
}

// Returns `None` if the `msg` doesn't contain a snapshot or it contains a snapshot which
// doesn't conflict with any other snapshots or regions. Otherwise a `snap.SnapKey` is returned.
func (d *peerMsgHandler) checkSnapshot(msg *rspb.RaftMessage) (*snap.SnapKey, error) {
	if msg.Message.Snapshot == nil {
		return nil, nil
	}
	regionID := msg.RegionId
	snapshot := msg.Message.Snapshot
	key := snap.SnapKeyFromRegionSnap(regionID, snapshot)
	snapData := new(rspb.RaftSnapshotData)
	err := snapData.Unmarshal(snapshot.Data)
	if err != nil {
		return nil, err
	}
	snapRegion := snapData.Region
	peerID := msg.ToPeer.Id
	var contains bool
	for _, peer := range snapRegion.Peers {
		if peer.Id == peerID {
			contains = true
			break
		}
	}
	if !contains {
		log.Infof("%s %s doesn't contains peer %d, skip", d.Tag, snapRegion, peerID)
		return &key, nil
	}
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	if !util.RegionEqual(meta.regions[d.regionId], d.Region()) {
		if !d.isInitialized() {
			log.Infof("%s stale delegate detected, skip", d.Tag)
			return &key, nil
		} else {
			panic(fmt.Sprintf("%s meta corrupted %s != %s", d.Tag, meta.regions[d.regionId], d.Region()))
		}
	}

	existRegions := meta.getOverlapRegions(snapRegion)
	for _, existRegion := range existRegions {
		if existRegion.GetId() == snapRegion.GetId() {
			continue
		}
		log.Infof("%s region overlapped %s %s", d.Tag, existRegion, snapRegion)
		return &key, nil
	}

	// check if snapshot file exists.
	_, err = d.ctx.snapMgr.GetSnapshotForApplying(key)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (d *peerMsgHandler) destroyPeer() {
	regionID := d.regionId
	// We can't destroy a peer which is applying snapshot.
	storeMeta := d.ctx.storeMeta
	storeMeta.Lock()
	defer storeMeta.Unlock()
	isInitialized := d.isInitialized()
	if err := d.Destroy(d.ctx.engine, false); err != nil {
		// If not panic here, the peer will be recreated in the next restart,
		// then it will be gc again. But if some overlap region is created
		// before restarting, the gc action will delete the overlap region's
		// data too.
		panic(fmt.Sprintf("%s destroy peer %v", d.Tag, err))
	}
	d.ctx.router.close(regionID)
	d.stopped = true
	if isInitialized && storeMeta.regionRanges.Delete(&regionItem{region: d.Region()}) == nil {
		log.Errorf("%s region %v, --- storeMeta regions %v", d.Tag, d.Region(), storeMeta.regions)
		panic(d.Tag + " storeMeta corruption detected in regionRanges")
	}
	if _, ok := storeMeta.regions[regionID]; !ok {
		panic(d.Tag + " storeMeta corruption detected in regions")
	}
	log.Infof("%s delete region %d in store %d's storeMeta", d.Tag, regionID, d.ctx.store.Id)
	delete(storeMeta.regions, regionID)
}

func (d *peerMsgHandler) startToDestroyPeer() {
	if len(d.Region().Peers) == 2 && d.IsLeader() {
		log.Warningf("%s this should not happen, can't destroy directly, transfer leader to another node and reject this.", d.Tag)
		var targetPeer uint64 = 0
		for _, peer := range d.Region().Peers {
			if peer.Id != d.PeerId() {
				targetPeer = peer.Id
				break
			}
		}
		if targetPeer == 0 {
			panic("This should not happen")
		}

		m := []pb.Message{{
			To:      targetPeer,
			MsgType: pb.MessageType_MsgHeartbeat,
			Commit:  d.peerStorage.raftState.HardState.Commit,
		}}
		for i := 0; i < 10; i++ {
			d.Send(d.ctx.trans, m)
			time.Sleep(100 * time.Millisecond)
		}
	}
	d.destroyPeer()
	// 存在一种情况，transfer leader 成功，收到他人的msgRequestVote，但是此时它还没有成功成为 leader，这时候还不能删除，要帮助他成功，不然对方永远无法成为leader
}

func (d *peerMsgHandler) findSiblingRegion() (result *metapb.Region) {
	meta := d.ctx.storeMeta
	meta.RLock()
	defer meta.RUnlock()
	item := &regionItem{region: d.Region()}
	meta.regionRanges.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*regionItem).region
		return true
	})
	return
}

func (d *peerMsgHandler) onRaftGCLogTick() {
	d.ticker.schedule(PeerTickRaftLogGC)
	if !d.IsLeader() {
		return
	}

	appliedIdx := d.peerStorage.AppliedIndex()
	firstIdx, _ := d.peerStorage.FirstIndex()
	var compactIdx uint64
	if appliedIdx > firstIdx && appliedIdx-firstIdx >= d.ctx.cfg.RaftLogGcCountLimit {
		compactIdx = appliedIdx
	} else {
		return
	}

	y.Assert(compactIdx > 0)
	compactIdx -= 1
	if compactIdx < firstIdx {
		// In case compact_idx == first_idx before subtraction.
		return
	}

	term, err := d.RaftGroup.Raft.RaftLog.Term(compactIdx)
	if err != nil {
		log.Fatalf("appliedIdx: %d, firstIdx: %d, compactIdx: %d", appliedIdx, firstIdx, compactIdx)
		panic(err)
	}

	// Create a compact log request and notify directly.
	regionID := d.regionId
	request := newCompactLogRequest(regionID, d.Meta, compactIdx, term)
	d.proposeRaftCommand(request, nil)
}

func (d *peerMsgHandler) onSplitRegionCheckTick() {
	d.ticker.schedule(PeerTickSplitRegionCheck)
	// To avoid frequent scan, we only add new scan tasks if all previous tasks
	// have finished.
	if len(d.ctx.splitCheckTaskSender) > 0 {
		return
	}

	if !d.IsLeader() {
		return
	}
	if d.ApproximateSize != nil && d.SizeDiffHint < d.ctx.cfg.RegionSplitSize/8 {
		return
	}
	d.ctx.splitCheckTaskSender <- &runner.SplitCheckTask{
		Region: d.Region(),
	}
	d.SizeDiffHint = 0
}

func (d *peerMsgHandler) onPrepareSplitRegion(regionEpoch *metapb.RegionEpoch, splitKey []byte, cb *message.Callback) {
	if err := d.validateSplitRegion(regionEpoch, splitKey); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	region := d.Region()
	d.ctx.schedulerTaskSender <- &runner.SchedulerAskSplitTask{
		Region:   region,
		SplitKey: splitKey,
		Peer:     d.Meta,
		Callback: cb,
	}
}

func (d *peerMsgHandler) validateSplitRegion(epoch *metapb.RegionEpoch, splitKey []byte) error {
	if len(splitKey) == 0 {
		err := errors.Errorf("%s split key should not be empty", d.Tag)
		log.Error(err)
		return err
	}

	if !d.IsLeader() {
		// region on this store is no longer leader, skipped.
		log.Infof("%s not leader, skip", d.Tag)
		return &util.ErrNotLeader{
			RegionId: d.regionId,
			Leader:   d.getPeerFromCache(d.LeaderId()),
		}
	}

	region := d.Region()
	latestEpoch := region.GetRegionEpoch()

	// This is a little difference for `check_region_epoch` in region split case.
	// Here we just need to check `version` because `conf_ver` will be update
	// to the latest value of the peer, and then send to Scheduler.
	if latestEpoch.Version != epoch.Version {
		log.Infof("%s epoch changed, retry later, prev_epoch: %s, epoch %s",
			d.Tag, latestEpoch, epoch)
		return &util.ErrEpochNotMatch{
			Message: fmt.Sprintf("%s epoch changed %s != %s, retry later", d.Tag, latestEpoch, epoch),
			Regions: []*metapb.Region{region},
		}
	}
	return nil
}

func (d *peerMsgHandler) onApproximateRegionSize(size uint64) {
	//log.Infof("%s Update approximate region size from %d -> %d", d.Tag, d.ApproximateSize, size)
	d.ApproximateSize = &size
}

func (d *peerMsgHandler) onSchedulerHeartbeatTick() {
	d.ticker.schedule(PeerTickSchedulerHeartbeat)

	if !d.IsLeader() {
		return
	}
	d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
}

func (d *peerMsgHandler) onGCSnap(snaps []snap.SnapKeyWithSending) {
	compactedIdx := d.peerStorage.truncatedIndex()
	compactedTerm := d.peerStorage.truncatedTerm()
	for _, snapKeyWithSending := range snaps {
		key := snapKeyWithSending.SnapKey
		if snapKeyWithSending.IsSending {
			snap, err := d.ctx.snapMgr.GetSnapshotForSending(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			if key.Term < compactedTerm || key.Index < compactedIdx {
				log.Infof("%s snap file %s has been compacted, delete", d.Tag, key)
				d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
			} else if fi, err1 := snap.Meta(); err1 == nil {
				modTime := fi.ModTime()
				if time.Since(modTime) > 4*time.Hour {
					log.Infof("%s snap file %s has been expired, delete", d.Tag, key)
					d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
				}
			}
		} else if key.Term <= compactedTerm &&
			(key.Index < compactedIdx || key.Index == compactedIdx) {
			log.Infof("%s snap file %s has been applied, delete", d.Tag, key)
			a, err := d.ctx.snapMgr.GetSnapshotForApplying(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			d.ctx.snapMgr.DeleteSnapshot(key, a, false)
		}
	}
}

func newAdminRequest(regionID uint64, peer *metapb.Peer) *raft_cmdpb.RaftCmdRequest {
	return &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			RegionId: regionID,
			Peer:     peer,
		},
	}
}

func newCompactLogRequest(regionID uint64, peer *metapb.Peer, compactIndex, compactTerm uint64) *raft_cmdpb.RaftCmdRequest {
	req := newAdminRequest(regionID, peer)
	req.AdminRequest = &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_CompactLog,
		CompactLog: &raft_cmdpb.CompactLogRequest{
			CompactIndex: compactIndex,
			CompactTerm:  compactTerm,
		},
	}
	return req
}

func findPeerIndex(region *metapb.Region, id uint64) int {
	for i, peer := range region.Peers {
		if peer.Id == id {
			return i
		}
	}
	return -1
}

func maybeGetRequestKey(req *raft_cmdpb.RaftCmdRequest) (bool, []byte) {
	// try to get split key
	if req.AdminRequest != nil && req.AdminRequest.Split != nil {
		return true, req.AdminRequest.Split.SplitKey
	}

	if req.Requests == nil {
		return false, nil
	}

	if len(req.Requests) == 0 {
		return false, nil
	}

	request := req.Requests[0]
	switch request.CmdType {
	case raft_cmdpb.CmdType_Invalid:
	case raft_cmdpb.CmdType_Get:
		return true, request.Get.Key
	case raft_cmdpb.CmdType_Put:
		return true, request.Put.Key
	case raft_cmdpb.CmdType_Delete:
		return true, request.Delete.Key
	case raft_cmdpb.CmdType_Snap:
		return false, nil
	}
	panic("This should not happen")
}

func maybeCheckRegionEpoch(msg *raft_cmdpb.RaftCmdRequest, region *metapb.Region, includeRegion bool) (bool, *util.ErrEpochNotMatch) {
	if msg.Header == nil {
		return false, nil
	}

	if err := util.CheckRegionEpoch(msg, region, includeRegion); err != nil {
		if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
			return true, errEpochNotMatching
		} else {
			panic("This should not happen")
		}
	}

	return false, nil
}
