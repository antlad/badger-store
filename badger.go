package badger_store

import (
	"github.com/dgraph-io/badger/v4"
)

type BadgerDB struct {
	DB *badger.DB
}

type Txn struct {
	raw  *badger.Txn
	refs []interface{}
}

func (tx *Txn) appendRefs(ref interface{}) {
	tx.refs = append(tx.refs, ref)
}

func (b *BadgerDB) View(f func(tx *Txn) error) error {
	return b.DB.View(func(txn *badger.Txn) error {
		return f(&Txn{raw: txn})
	})
}

func (b *BadgerDB) Update(f func(tx *Txn) error) error {

	err := b.DB.Update(func(txn *badger.Txn) error {
		return f(&Txn{raw: txn})
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
