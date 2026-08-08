package badger_store

import (
	"github.com/dgraph-io/badger/v4"
)

// BadgerStore wraps a Badger database and hands out transactions.
type BadgerStore struct {
	DB *badger.DB
}

// NewBadgerStore wraps an already-open Badger database.
func NewBadgerStore(db *badger.DB) *BadgerStore {
	return &BadgerStore{DB: db}
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

// View runs f in a read-only transaction.
func (b *BadgerStore) View(f func(tx Transaction) error) error {
	return b.DB.View(func(tn *badger.Txn) error {
		return f(&txnImpl{raw: tn})
	})
}

// Update runs f in a read-write transaction, committing only if f returns nil.
func (b *BadgerStore) Update(f func(tx Transaction) error) error {
	err := b.DB.Update(func(tn *badger.Txn) error {
		return f(&txnImpl{raw: tn})
	})
	return err
}

// Reset drops every key in the database, not only those of one table.
func (b *BadgerStore) Reset() error {
	return b.DB.DropAll()
}

// Close closes the underlying database.
func (b *BadgerStore) Close() error {
	if b.DB != nil {
		return b.DB.Close()
	}

	return nil
}
