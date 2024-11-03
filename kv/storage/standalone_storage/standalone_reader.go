package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

type StandaloneStorageReader struct {
	storage *StandAloneStorage
}

func NewStandaloneStorageReader(storage *StandAloneStorage) *StandaloneStorageReader {
	return &StandaloneStorageReader{
		storage: storage,
	}
}

func (r *StandaloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCF(r.storage.db, cf, key)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return nil, nil
		}
		return nil, err
	}
	return val, nil
}

func (r *StandaloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	txn := r.storage.db.NewTransaction(false)
	return engine_util.NewCFIterator(cf, txn)
}

func (r *StandaloneStorageReader) Close() {
	r.storage.db.Close()
}
