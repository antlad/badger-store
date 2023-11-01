package badger_store

import (
	"github.com/dgraph-io/badger/v4"
)

type BadgerDB struct {
	b *badger.DB
}

type dbTxn struct {
	raw  *badger.Txn
	refs []interface{}
}

func (tx *dbTxn) appendRefs(ref interface{}) {
	tx.refs = append(tx.refs, ref)
}

func (b *BadgerDB) View(f func(tx Transaction) error) error {
	return b.b.View(func(txn *badger.Txn) error {
		return f(Transaction(&dbTxn{raw: txn}))
	})
}

func (b *BadgerDB) Update(f func(tx Transaction) error) error {
	err := b.b.Update(func(txn *badger.Txn) error {
		return f(Transaction(&dbTxn{raw: txn}))
	})
	return err
}

func (b *BadgerDB) Close() error {
	if b.b != nil {
		return b.b.Close()
	}
	return nil
}

func NewDB(folder string) (*BadgerDB, error) {
	opts := badger.DefaultOptions(folder)
	// https://github.com/dgraph-io/badger/issues/1297
	opts.NumVersionsToKeep = 0
	opts.CompactL0OnClose = true
	opts.NumLevelZeroTables = 1
	opts.NumLevelZeroTablesStall = 2
	opts.ValueLogFileSize = 1024 * 1024 * 10

	b, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	d := &BadgerDB{
		b: b,
	}
	return d, nil
}
