package mvcc

import (
	"bytes"

	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	// Your Data Here (4C).
	nextKey []byte
	txn     *MvccTxn
	iter    engine_util.DBIterator
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	scan := &Scanner{
		nextKey: startKey,
		txn:     txn,
		iter:    txn.Reader.IterCF(engine_util.CfWrite),
	}
	// start from it
	scan.iter.Seek(EncodeKey(scan.nextKey, txn.StartTS))
	return scan
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
	scan.iter.Close()
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).
	if !scan.iter.Valid() {
		// none
		return nil, nil, nil
	}

	for scan.iter.Valid() {
		item := scan.iter.Item()
		curKey := DecodeUserKey(item.Key())
		commitTS := decodeTimestamp(item.Key())
		if bytes.Equal(curKey, scan.nextKey) {
			if commitTS > scan.txn.StartTS {
				// larger commit ts, ignore
				scan.iter.Next()
			} else {
				// find valid
				break
			}
		} else {
			scan.nextKey = curKey
		}
	}

	if !scan.iter.Valid() {
		// exhausted
		return nil, nil, nil
	}

	item := scan.iter.Item()
	key := DecodeUserKey(item.Key())
	value, err := item.Value()
	if err != nil {
		return key, nil, err
	}
	write, err := ParseWrite(value)
	if err != nil {
		return key, nil, err
	}
	if write.Kind != WriteKindPut {
		// deleted or rollback, get for next
		// 应该忽略掉后面所有这个key
		for scan.iter.Valid() {
			scan.iter.Next()
			tmpItem := scan.iter.Item()
			tmpKey := DecodeUserKey(tmpItem.Key())
			if !bytes.Equal(tmpKey, key) {
				break
			}
		}
		return scan.Next()
	}
	realValue, err := scan.txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(key, write.StartTS))
	if err != nil {
		return key, nil, err
	}
	scan.iter.Next()
	return key, realValue, nil
}
