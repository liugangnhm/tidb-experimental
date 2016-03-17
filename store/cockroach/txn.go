package cockroach

import (
	"fmt"

	cockroach "github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/kv"
)

var (
	_ kv.Transaction = (*cockroachTxn)(nil)
)

type cockroachTxn struct {
	us       kv.UnionStore
	txn      *cockroach.Txn
	batch    *cockroach.Batch
	startts  roachpb.Timestamp // start timestamp
	committs roachpb.Timestamp // commit timestam
	valid    bool
	dirty    bool
}

func newCockroachTxn(t *cockroach.Txn) *cockroachTxn {
	return &cockroachTxn{
		us:    kv.NewUnionStore(newCockroachSnapshot(t)),
		txn:   t,
		batch: t.NewBatch(),
		valid: true,
	}
}

// Implement transaction interface
func (txn *cockroachTxn) Get(k kv.Key) ([]byte, error) {
	d, err := txn.us.Get(k)
	if txn.startts.Equal(roachpb.ZeroTimestamp) {
		setTxnStartts(txn, txn.txn.Proto)
	}
	log.Debugf("[kv] get key: %q, txn:%d", k, txn.startts)
	return d, err
}

func (txn *cockroachTxn) Set(k kv.Key, v []byte) error {
	log.Debugf("[kv] set %q txn:%d", k, txn.startts)
	txn.dirty = true
	return txn.us.Set(k, v)
}

// TODO:get start ts and commit ts
func (txn *cockroachTxn) String() string {
	return fmt.Sprintf("%d", txn.startts)
}

func (txn *cockroachTxn) Seek(k kv.Key) (kv.Iterator, error) {
	d, err := txn.us.Seek(k)
	if txn.startts.Equal(roachpb.ZeroTimestamp) {
		setTxnStartts(txn, txn.txn.Proto)
	}
	log.Debugf("[kv] seek %q txn:%d", k, txn.startts)
	return d, err
}

func (txn *cockroachTxn) Delete(k kv.Key) error {
	log.Debugf("[kv] delete %q txn:%d", k, txn.startts)
	txn.dirty = true
	return txn.us.Delete(k)
}

func (txn *cockroachTxn) SetOption(opt kv.Option, val interface{}) {
	txn.us.SetOption(opt, val)
}

func (txn *cockroachTxn) DelOption(opt kv.Option) {
	txn.us.DelOption(opt)
}

func (txn *cockroachTxn) IsReadOnly() bool {
	return !txn.dirty
}

func (txn *cockroachTxn) doCommit() error {
	if err := txn.us.CheckLazyConditionPairs(); err != nil {
		return errors.Trace(err)
	}

	err := txn.us.WalkBuffer(func(k kv.Key, v []byte) error {
		if len(v) == 0 { // Deleted marker
			txn.batch.Del(roachpb.MakeKey(roachpb.Key(tidbStartKeyPrefix), roachpb.Key(k)))
		} else {
			txn.batch.Put(roachpb.MakeKey(roachpb.Key(tidbStartKeyPrefix), roachpb.Key(k)), v)
		}
		return nil
	})

	if err != nil {
		return errors.Trace(err)
	}

	if err := txn.txn.CommitInBatch(txn.batch); err != nil {
		log.Error(err)
		return errors.Trace(err.GoError())
	}

	setTxnCommitts(txn, txn.txn.Proto)
	log.Debugf("[kv] commit successfully! start ts:%v, commit ts %v", txn.startts, txn.committs)
	return nil
}

func (txn *cockroachTxn) Commit() error {
	if !txn.valid {
		return kv.ErrInvalidTxn
	}
	log.Debugf("[kv] start to commit txn %d", txn.startts)
	defer func() {
		txn.close()
	}()
	return txn.doCommit()
}

func (txn *cockroachTxn) close() error {
	txn.us.Release()
	txn.valid = false
	return nil
}

func (txn *cockroachTxn) Rollback() error {
	if !txn.valid {
		return kv.ErrInvalidTxn
	}

	err := txn.txn.Rollback()
	if err != nil {
		log.Error(err)
		return errors.Trace(err.GoError())
	}

	log.Warnf("[kv] Rollback txn %d", txn.startts)
	return txn.close()
}

func (txn *cockroachTxn) LockKeys(keys ...kv.Key) error {
	// TODO: to be supported
	return nil
}

func setTxnStartts(txn *cockroachTxn, tx roachpb.Transaction) {
	txn.startts = tx.OrigTimestamp
}

func setTxnCommitts(txn *cockroachTxn, tx roachpb.Transaction) {
	txn.committs = tx.Timestamp
}
