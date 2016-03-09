package cockroach

import (
	cockroach "github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/juju/errors"
	"github.com/pingcap/tidb/kv"
)

var (
	_ kv.Snapshot = (*cockroachSnapshot)(nil)
	_ kv.Iterator = (*cockroachIter)(nil)
)

var (
	// MaxFetchRows define max rows in one fetch
	MaxFetchRows = 100
)

type cockroachSnapshot struct {
	txn *cockroach.Txn
}

func newCockroachSnapshot(txn *cockroach.Txn) *cockroachSnapshot {
	return &cockroachSnapshot{txn: txn}
}

func (s *cockroachSnapshot) Get(k kv.Key) ([]byte, error) {
	v, err := s.txn.Get(roachpb.MakeKey(roachpb.Key(tidbStartKeyPrefix), roachpb.Key(k)))
	if err != nil {
		return nil, errors.Trace(err.GoError())
	}
	return v.ValueBytes(), nil
}

// BatchGet implements kv.Snapshot.BatchGet interface.
func (s *cockroachSnapshot) BatchGet(keys []kv.Key) (map[string][]byte, error) {
	batch := s.txn.NewBatch()
	for _, key := range keys {
		batch.Get(roachpb.MakeKey(roachpb.Key(tidbStartKeyPrefix), roachpb.Key(key)))
	}
	err := s.txn.Run(batch)
	if err != nil {
		return nil, errors.Trace(err.GoError())
	}

	m := make(map[string][]byte)
	for _, r := range batch.Results {
		for _, row := range r.Rows {
			rawKey := string(row.Key)
			if len(rawKey) <= len(tidbStartKeyPrefix) {
				return nil, errors.Trace(ErrInvalidKey)
			}
			k := string(rawKey[len(tidbStartKeyPrefix):])
			v := row.ValueBytes()
			if v == nil {
				continue
			}
			m[k] = v
		}
	}
	return m, nil
}

func (s *cockroachSnapshot) Seek(k kv.Key) (kv.Iterator, error) {
	// TODO: check key invalidity
	if k == nil {
		return newInnerScanner(s.txn, roachpb.Key(tidbStartKeyPrefix), roachpb.Key(tidbEndKeyPrefix)), nil
	}

	return newInnerScanner(s.txn, roachpb.MakeKey(roachpb.Key(tidbStartKeyPrefix),
		roachpb.Key(k)), roachpb.Key(tidbEndKeyPrefix)), nil
}

func newInnerScanner(txn *cockroach.Txn, begin, end roachpb.Key) kv.Iterator {
	it := &cockroachIter{
		index: 0,
		txn:   txn,
	}
	it.rows, _ = it.txn.Scan(begin, end, int64(MaxFetchRows))
	return it
}

func (s *cockroachSnapshot) Release() {
	s.txn = nil
}

type cockroachIter struct {
	index int
	rows  []cockroach.KeyValue
	txn   *cockroach.Txn
}

func (it *cockroachIter) Next() error {
	it.index++
	if it.index == len(it.rows) {
		if len(it.rows) == MaxFetchRows {
			prev := it.rows[MaxFetchRows-1].Key
			begin := roachpb.Key(prev).Next()
			it.rows, _ = it.txn.Scan(begin, roachpb.Key(tidbEndKeyPrefix), int64(MaxFetchRows))
		} else {
			it.rows = nil
		}
		it.index = 0
	}
	return nil
}

func (it *cockroachIter) Valid() bool {
	if len(it.rows) == 0 {
		return false
	}
	return true
}

func (it *cockroachIter) Key() kv.Key {
	// TODO: check key invalidity
	return kv.Key(it.rows[it.index].Key[len(tidbStartKeyPrefix):])
}

func (it *cockroachIter) Value() []byte {
	return it.rows[it.index].ValueBytes()
}

func (it *cockroachIter) Close() {
	it.rows = nil
}
