package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	reader, err := server.storage.Reader(req.GetContext())
	defer reader.Close()
	reply := &kvrpcpb.RawGetResponse{}
	if err != nil {
		reply.Error = err.Error()
		return reply, err
	}
	res, err := reader.GetCF(req.GetCf(), req.GetKey())
	reply.Value = res
	if res == nil {
		reply.NotFound = true
	}
	return reply, err
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	put := storage.Put{
		Key:   req.GetKey(),
		Value: req.GetValue(),
		Cf:    req.GetCf(),
	}
	batch := storage.Modify{Data: put}
	err := server.storage.Write(req.GetContext(), []storage.Modify{batch})
	reply := &kvrpcpb.RawPutResponse{}
	if err != nil {
		reply.Error = err.Error()
		return reply, err
	}
	return reply, err
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	delete := storage.Delete{
		Key: req.GetKey(),
		Cf:  req.GetCf(),
	}
	batch := storage.Modify{Data: delete}
	err := server.storage.Write(req.GetContext(), []storage.Modify{batch})
	reply := &kvrpcpb.RawDeleteResponse{}
	if err != nil {
		reply.Error = err.Error()
		return reply, err
	}
	return reply, err
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	reader, err := server.storage.Reader(req.GetContext())
	defer reader.Close()
	reply := &kvrpcpb.RawScanResponse{}
	if err != nil {
		reply.Error = err.Error()
		return reply, err
	}
	iter := reader.IterCF(req.GetCf())
	var kvs []*kvrpcpb.KvPair
	var nums uint32 = 0
	for iter.Seek(req.StartKey); iter.Valid(); iter.Next() {
		if nums >= req.GetLimit() {
			break
		}
		item := iter.Item()
		k := item.Key()
		v, err := item.Value()
		if err != nil {
			panic(err)
		}
		kvs = append(kvs, &kvrpcpb.KvPair{
			Key:   k,
			Value: v,
		})
		nums++
	}
	iter.Close()
	reply.Kvs = kvs
	return reply, err
}
