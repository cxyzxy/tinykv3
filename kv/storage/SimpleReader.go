package storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

type SimpleReader struct {
	txn *badger.Txn
}

func NewSimpleReader(txn *badger.Txn) *SimpleReader {
	return &SimpleReader{
		txn: txn,
	}
}

func (r *SimpleReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(r.txn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return val, err
}

func (r *SimpleReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, r.txn)
}

func (r *SimpleReader) Close() {
	r.txn.Discard()
}
