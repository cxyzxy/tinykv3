package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4A/4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	reader, err := server.storage.Reader(req.Context)
	defer reader.Close()
	resp := &kvrpcpb.GetResponse{
		RegionError: nil,
		Error:       nil,
		Value:       nil,
		NotFound:    false,
	}

	if err != nil {
		// todo handle region error
		return nil, err
	}

	txn := mvcc.NewMvccTxn(reader, req.Version)
	server.Latches.AcquireLatches([][]byte{req.Key})
	defer server.Latches.ReleaseLatches([][]byte{req.Key})

	// check is locked
	lock, err := txn.GetLock(req.Key)
	if err != nil {
		return nil, err
	}
	if lock != nil && req.Version >= lock.Ts {
		// locked
		resp.Error = &kvrpcpb.KeyError{Locked: &kvrpcpb.LockInfo{
			PrimaryLock: lock.Primary,
			LockVersion: lock.Ts,
			Key:         req.Key,
			LockTtl:     lock.Ttl,
		}}
		return resp, nil
	}

	// get value directly
	value, err := txn.GetValue(req.Key)
	if err != nil {
		return nil, err
	}
	if value == nil {
		resp.NotFound = true
	} else {
		resp.Value = value
	}
	return resp, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	reader, err := server.storage.Reader(req.Context)
	defer reader.Close()
	resp := &kvrpcpb.PrewriteResponse{
		RegionError: nil,
		Errors:      nil,
	}
	if err != nil {
		// todo handle region error
		return nil, err
	}

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	server.Latches.AcquireLatches([][]byte{req.PrimaryLock})
	defer server.Latches.ReleaseLatches([][]byte{req.PrimaryLock})
	errors := make([]*kvrpcpb.KeyError, 0)

	// check
	for _, mutation := range req.Mutations {
		// Abort if existed writes after our start timestamp
		write, ts, err := txn.MostRecentWrite(mutation.Key)
		if err != nil {
			return nil, err
		}

		// write conflict
		if write != nil && ts >= txn.StartTS {
			errors = append(errors, &kvrpcpb.KeyError{Conflict: &kvrpcpb.WriteConflict{
				StartTs:    txn.StartTS,
				ConflictTs: ts,
				Key:        mutation.Key,
				Primary:    req.PrimaryLock,
			}})
			resp.Errors = errors
			return resp, nil
		}

		// get lock
		lock, err := txn.GetLock(mutation.Key)
		if err != nil {
			return nil, err
		}
		// already locked
		if lock != nil {
			errors = append(errors, &kvrpcpb.KeyError{Locked: &kvrpcpb.LockInfo{
				PrimaryLock: lock.Primary,
				LockVersion: lock.Ts,
				Key:         mutation.Key,
				LockTtl:     lock.Ttl,
			}})
			resp.Errors = errors
			return resp, nil
		}
	}

	// safe for writing
	for _, mutation := range req.Mutations {
		key := mutation.Key
		value := mutation.Value
		lock := &mvcc.Lock{
			Primary: req.PrimaryLock,
			Ts:      txn.StartTS,
			Ttl:     req.LockTtl,
			Kind:    0,
		}
		switch mutation.Op {
		case kvrpcpb.Op_Put:
			txn.PutValue(key, value)
			lock.Kind = mvcc.WriteKindPut
			txn.PutLock(key, lock)
		case kvrpcpb.Op_Del:
			txn.DeleteValue(key)
			lock.Kind = mvcc.WriteKindDelete
			txn.PutLock(key, lock)
		default:
			panic("This should not happened.")
		}
	}
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	reader, err := server.storage.Reader(req.Context)
	defer reader.Close()
	if err != nil {
		// todo handle region error
		return nil, err
	}
	resp := &kvrpcpb.CommitResponse{
		RegionError: nil,
		Error:       nil,
	}
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	server.Latches.AcquireLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)

	// check lock first
	for _, key := range req.Keys {
		lock, err := txn.GetLock(key)
		if err != nil {
			return nil, err
		}
		if lock == nil {
			// Committing a rolled back transaction. It should success directly.
			return resp, nil
		}

		if lock.Ts != txn.StartTS {
			// maybe your prewrite is too slow, rollback by other transaction
			resp.Error = &kvrpcpb.KeyError{Retryable: "true"}
			return resp, nil
		}
	}

	// Write and rm Lock
	for _, key := range req.Keys {
		lock, err := txn.GetLock(key)
		if err != nil {
			return nil, err
		}

		txn.PutWrite(key, req.CommitVersion, &mvcc.Write{
			StartTS: txn.StartTS,
			Kind:    lock.Kind,
		})
		txn.DeleteLock(key)
	}

	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	reader, err := server.storage.Reader(req.Context)
	defer reader.Close()
	if err != nil {
		// todo handle region error
		return nil, err
	}

	txn := mvcc.NewMvccTxn(reader, req.Version)
	scan := mvcc.NewScanner(req.StartKey, txn)
	defer scan.Close()
	resp := &kvrpcpb.ScanResponse{
		RegionError: nil,
		Pairs:       nil,
	}
	kvs := make([]*kvrpcpb.KvPair, 0)
	for i := uint32(0); i < req.Limit; i++ {
		key, value, err := scan.Next()
		if key == nil && value == nil && err == nil {
			// exhausted
			break
		}

		if err != nil {
			keyError := &kvrpcpb.KeyError{
				Abort: "true",
			}
			kvs = append(kvs, &kvrpcpb.KvPair{
				Error: keyError,
				Key:   key,
				Value: nil,
			})
			continue
		}

		// check for lock
		lock, err := txn.GetLock(key)
		if err != nil {
			keyError := &kvrpcpb.KeyError{
				Abort: "true",
			}
			kvs = append(kvs, &kvrpcpb.KvPair{
				Error: keyError,
				Key:   key,
				Value: nil,
			})
			continue
		}

		// locked
		if lock != nil && txn.StartTS > lock.Ts {
			keyError := &kvrpcpb.KeyError{
				Locked: &kvrpcpb.LockInfo{
					PrimaryLock: lock.Primary,
					LockVersion: lock.Ts,
					Key:         key,
					LockTtl:     lock.Ttl,
				},
			}
			kvs = append(kvs, &kvrpcpb.KvPair{
				Error: keyError,
				Key:   key,
				Value: nil,
			})
			continue
		}

		// everything is ok!!
		kvs = append(kvs, &kvrpcpb.KvPair{
			Error: nil,
			Key:   key,
			Value: value,
		})
	}
	resp.Pairs = kvs
	return resp, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	reader, err := server.storage.Reader(req.Context)
	defer reader.Close()
	resp := &kvrpcpb.CheckTxnStatusResponse{
		RegionError:   nil,
		LockTtl:       0,
		CommitVersion: 0,
		Action:        kvrpcpb.Action_NoAction,
	}
	if err != nil {
		// todo handle region error
		return nil, err
	}

	// 使用 req.LockTs, 报错返回的 lockTS，currentTs 用于检测超时
	txn := mvcc.NewMvccTxn(reader, req.LockTs)

	// start to check primary key, get write first
	write, ts, err := txn.CurrentWrite(req.PrimaryKey)
	if err != nil {
		return nil, err
	}
	if write != nil && write.Kind != mvcc.WriteKindRollback {
		// already committed
		resp.CommitVersion = ts
		return resp, nil
	}

	// not committed yet, check lock is expired
	lock, err := txn.GetLock(req.PrimaryKey)
	if err != nil {
		return nil, err
	}

	if lock != nil {
		// check lock is timeout
		if mvcc.PhysicalTime(lock.Ts)+lock.Ttl < mvcc.PhysicalTime(req.CurrentTs) {
			// timeout, remove lock and rollback
			txn.DeleteLock(req.PrimaryKey)
			txn.DeleteValue(req.PrimaryKey)
			txn.PutWrite(req.PrimaryKey, txn.StartTS, &mvcc.Write{
				StartTS: txn.StartTS,
				Kind:    mvcc.WriteKindRollback,
			})
			resp.Action = kvrpcpb.Action_TTLExpireRollback
		} else {
			// still locked
			resp.LockTtl = lock.Ttl
			return resp, nil
		}
	} else {
		// no lock, rollback directly
		txn.DeleteValue(req.PrimaryKey)
		txn.PutWrite(req.PrimaryKey, txn.StartTS, &mvcc.Write{
			StartTS: txn.StartTS,
			Kind:    mvcc.WriteKindRollback,
		})
		resp.Action = kvrpcpb.Action_LockNotExistRollback
	}

	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	reader, err := server.storage.Reader(req.Context)
	defer reader.Close()
	if err != nil {
		// todo handle region error
		return nil, err
	}
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	resp := &kvrpcpb.BatchRollbackResponse{
		RegionError: nil,
		Error:       nil,
	}
	for _, key := range req.Keys {
		// check is already rollback
		write, _, err := txn.CurrentWrite(key)
		if err != nil {
			return nil, err
		}
		if write != nil {
			// it must be Rollback
			if write.Kind != mvcc.WriteKindRollback {
				// This should not be happened. Abort immediately!
				resp.Error = &kvrpcpb.KeyError{Abort: "true"}
				return resp, nil
			}
			// already rollback
			continue
		}

		// check key is locked by other transaction
		lock, err := txn.GetLock(key)
		if err != nil {
			return nil, err
		}
		if lock != nil && lock.Ts > txn.StartTS {
			// locked by other transaction
			resp.Error = &kvrpcpb.KeyError{Abort: "true"}
			return resp, nil
		}
		// start to rollback
		// remove lock if it existed
		// 为什么lock.Ts == txn.StartTS？只remove属于你transaction的lock
		if lock != nil && lock.Ts == txn.StartTS {
			txn.DeleteLock(key)
		}
		// delete value
		txn.DeleteValue(key)
		// write rollback write
		txn.PutWrite(key, txn.StartTS, &mvcc.Write{
			StartTS: txn.StartTS,
			Kind:    mvcc.WriteKindRollback,
		})
	}

	writes := txn.Writes()
	err = server.storage.Write(req.Context, writes)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (server *Server) KvResolveLock(context context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	reader, err := server.storage.Reader(req.Context)
	defer reader.Close()
	if err != nil {
		// todo handle region error
		return nil, err
	}
	resp := &kvrpcpb.ResolveLockResponse{
		RegionError: nil,
		Error:       nil,
	}

	// get all locks first
	keys := make([][]byte, 0)
	iter := reader.IterCF(engine_util.CfLock)
	defer iter.Close()
	for ; iter.Valid(); iter.Next() {
		item := iter.Item()
		value, err := item.Value()
		if err != nil {
			return nil, err
		}
		lock, err := mvcc.ParseLock(value)
		if err != nil {
			return nil, err
		}
		if lock.Ts == req.StartVersion {
			keys = append(keys, item.Key())
		}
	}

	if len(keys) == 0 {
		// empty, return directly
		return resp, nil
	}

	if req.CommitVersion == 0 {
		// rollback all
		tmpResp, err := server.KvBatchRollback(context, &kvrpcpb.BatchRollbackRequest{
			Context:      req.Context,
			StartVersion: req.StartVersion,
			Keys:         keys,
		})
		if err != nil {
			return nil, err
		}
		resp.RegionError = tmpResp.RegionError
		resp.Error = tmpResp.Error
	} else {
		// commit
		tmpResp, err := server.KvCommit(context, &kvrpcpb.CommitRequest{
			Context:       req.Context,
			StartVersion:  req.StartVersion,
			Keys:          keys,
			CommitVersion: req.CommitVersion,
		})
		if err != nil {
			return nil, err
		}
		resp.RegionError = tmpResp.RegionError
		resp.Error = tmpResp.Error
	}
	return resp, nil
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
