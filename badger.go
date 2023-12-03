package badger_store

import (
	"github.com/dgraph-io/badger/v4"
)

type BadgerStore struct {
	DB *badger.DB
}

func NewBadgerStore(DB *badger.DB) *BadgerStore {
	return &BadgerStore{DB: DB}
}

type txnImpl struct {
	raw  *badger.Txn
	refs []interface{}
}

func (tx *txnImpl) AppendRef(ref interface{}) {
	tx.refs = append(tx.refs, ref)
}

func (tx *txnImpl) Raw() *badger.Txn {
	return tx.raw
}

func (b *BadgerStore) View(f func(tx Transaction) error) error {
	return b.DB.View(func(tn *badger.Txn) error {
		return f(&txnImpl{raw: tn})
	})
}

func (b *BadgerStore) Update(f func(tx Transaction) error) error {
	err := b.DB.Update(func(tn *badger.Txn) error {
		return f(&txnImpl{raw: tn})
	})
	return err
}

func (b *BadgerStore) Reset() error {
	return b.DB.DropAll()
}

func (b *BadgerStore) Close() error {
	if b.DB != nil {
		return b.DB.Close()
	}

	return nil
}
