package server

import (
	"context"
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	reader, err := server.storage.Reader(&kvrpcpb.Context{})
	if err != nil {
		return &kvrpcpb.RawGetResponse{}, err
	}
	val, err := reader.GetCF(req.GetCf(), req.GetKey())
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return &kvrpcpb.RawGetResponse{
				NotFound: true,
			}, nil
		}
		return &kvrpcpb.RawGetResponse{}, nil
	}
	if val == nil {
		return &kvrpcpb.RawGetResponse{
			NotFound: true,
		}, nil
	}
	return &kvrpcpb.RawGetResponse{
		Value: val,
	}, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	var batch []storage.Modify = []storage.Modify{
		storage.Modify{
			storage.Put{
				Key:   req.Key,
				Value: req.Value,
				Cf:    req.Cf,
			},
		},
	}
	err := server.storage.Write(req.Context, batch)
	if err != nil {
		return &kvrpcpb.RawPutResponse{}, err
	}
	return &kvrpcpb.RawPutResponse{}, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted

	var batch []storage.Modify = []storage.Modify{
		storage.Modify{
			storage.Put{
				Key:   req.Key,
				Value: nil,
				Cf:    req.Cf,
			},
		},
	}
	err := server.storage.Write(req.Context, batch)
	if err != nil {
		return &kvrpcpb.RawDeleteResponse{}, err
	}
	return &kvrpcpb.RawDeleteResponse{}, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	reader, err := server.storage.Reader(&kvrpcpb.Context{})
	if err != nil {
		return &kvrpcpb.RawScanResponse{}, nil
	}
	iter := reader.IterCF(req.Cf)
	kvs := []*kvrpcpb.KvPair{}
	limit := uint32(0)
	for iter.Seek(req.GetStartKey()); iter.Valid(); iter.Next() {
		limit = limit + 1
		if limit > req.GetLimit() {
			break
		}

		value, err := iter.Item().ValueCopy(nil)
		if err != nil {
			panic(err)
		}
		kvs = append(kvs, &kvrpcpb.KvPair{
			Key:   iter.Item().Key(),
			Value: value,
		})
	}
	return &kvrpcpb.RawScanResponse{
		Kvs: kvs,
	}, nil
}
