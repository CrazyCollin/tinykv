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
	resp := &kvrpcpb.RawGetResponse{}
	reader, _ := server.storage.Reader(req.Context)
	//todo standalone err, to be finished
	defer reader.Close()
	val, err := reader.GetCF(req.GetCf(), req.GetKey())
	if err != nil {
		resp.Error = err.Error()
		return resp, nil
	}
	resp.Value = val
	// set response to key not found
	if val == nil {
		resp.NotFound = true
	}
	return resp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	resp := &kvrpcpb.RawPutResponse{}
	putReq := storage.Put{
		Key:   req.GetKey(),
		Value: req.GetValue(),
		Cf:    req.GetCf(),
	}
	modify := []storage.Modify{
		{putReq},
	}
	err := server.storage.Write(req.Context, modify)
	if err != nil {
		resp.Error = err.Error()
		return resp, nil
	}
	return resp, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	resp := &kvrpcpb.RawDeleteResponse{}
	delReq := storage.Delete{
		Key: req.GetKey(),
		Cf:  req.GetCf(),
	}
	modify := []storage.Modify{
		{delReq},
	}
	err := server.storage.Write(req.Context, modify)
	if err != nil {
		resp.Error = err.Error()
		return resp, nil
	}
	return resp, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	resp := &kvrpcpb.RawScanResponse{}

	//todo standalone err, to be finished

	reader, _ := server.storage.Reader(req.Context)
	defer reader.Close()
	iter := reader.IterCF(req.GetCf())
	var index uint32
	index = 0
	key := req.StartKey

	for iter.Seek(key); iter.Valid(); iter.Next() {
		if index >= req.Limit {
			break
		}
		index++
		item := iter.Item()
		key := item.Key()
		val, _ := item.Value()
		tmpKvPair := &kvrpcpb.KvPair{
			Key:   key,
			Value: val,
		}
		resp.Kvs = append(resp.Kvs, tmpKvPair)
	}

	iter.Close()
	return resp, nil
}
