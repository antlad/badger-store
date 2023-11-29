package badger_store

import (
	"github.com/dgraph-io/badger/v4"
)

type BadgerDB struct {
	DB *badger.DB
}

type txnImpl struct {
	raw  *badger.Txn
	refs []interface{}
}

func (tx *txnImpl) appendRefs(ref interface{}) {
	tx.refs = append(tx.refs, ref)
}

func (b *BadgerDB) View(f func(tx interface{}) error) error {
	return b.DB.View(func(tn *badger.Txn) error {
		return f(&txnImpl{raw: tn})
	})
}

func (b *BadgerDB) Update(f func(tx interface{}) error) error {

	err := b.DB.Update(func(tn *badger.Txn) error {
		return f(&txnImpl{raw: tn})
	})
	return err
}

func (b *BadgerDB) Reset() error {
	return b.DB.DropAll()
}

func (b *BadgerDB) Close() error {
	if b.DB != nil {
		return b.DB.Close()
	}

	return nil
}
