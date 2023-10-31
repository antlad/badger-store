package badger_store

import (
	"errors"
	"github.com/dgraph-io/badger/v4"
)

type ItemID []byte

type Item interface {
	ID() ItemID
	IndexValue(name string) []byte
}

type ItemMeta[View any, ST Item] interface {
	TableName() string
	Parse(val []byte) (View, error)
	Serialize(ST) ([]byte, error)
}

type Handler[T any, ST Item] struct {
	db *badger.DB
	m  ItemMeta[T, ST]
}

var ErrStopIteration = errors.New("stop iteration")

func IsStopIteration(err error) bool {
	return errors.Is(err, ErrStopIteration)
}

func NewHandler[T any, ST Item](db *badger.DB, m ItemMeta[T, ST]) *Handler[T, ST] {
	return &Handler[T, ST]{
		db: db,
		m:  m,
	}
}

func (b *Handler[T, ST]) PutItem(t Transaction, item ST) error {
	r := t.(*dbTxn)
	txn := r.raw
	data, err := b.m.Serialize(item)
	if err != nil {
		return err
	}
	if err = txn.Set(b.key(item.ID()), data); err != nil {
		return err
	}
	r.refs = append(r.refs, data)
	return nil
}

func (b *Handler[T, ST]) key(id ItemID) []byte {
	return append(b.tableName(), id...)
}

func (b *Handler[T, ST]) tableName() []byte {
	return []byte(b.m.TableName())
}

func (b *Handler[T, ST]) indexPrefix(name string) []byte {
	return append(b.tableName(), []byte(name)...)
}

func (b *Handler[T, ST]) indexKey(name string, value []byte) []byte {
	return append(b.indexPrefix(name), value...)
}

func (b *Handler[T, ST]) GetByUniqIndex(t Transaction, indexName string, indexValue []byte, cb func(view T)) error {
	txn := t.(*dbTxn).raw
	indexItem, err := txn.Get(b.indexKey(indexName, indexValue))
	if err != nil {
		return err
	}
	err = indexItem.Value(func(val []byte) error {
		item, err := txn.Get(b.key(val))
		if err != nil {
			return err
		}
		return item.Value(func(data []byte) error {
			v, err := b.m.Parse(data)
			if err != nil {
				return err
			}
			cb(v)
			return nil
		})
	})
	return err
}

func iteratorOpts(prefix []byte) badger.IteratorOptions {
	opts := badger.DefaultIteratorOptions
	opts.Prefix = prefix
	return opts
}

func (b *Handler[T, ST]) IterateByIndex(t Transaction, indexName string, indexKey []byte, cb func(view T) error) error {
	txn := t.(*dbTxn).raw

	it := txn.NewIterator(iteratorOpts(b.indexKey(indexName, indexKey)))
	defer it.Close()

	for it.Rewind(); it.Valid(); it.Next() {
		err := it.Item().Value(func(val []byte) error {
			item, err := txn.Get(b.key(val))
			if err != nil {
				return err
			}
			return item.Value(func(data []byte) error {
				v, err := b.m.Parse(data)
				if err != nil {
					return err
				}
				return cb(v)
			})
		})
		if err != nil {
			if IsStopIteration(err) {
				break
			}
			return err
		}
	}

	return nil
}

func (b *Handler[T, ST]) ViewItemByID(t Transaction, id ItemID, cb func(view T)) error {
	txn := t.(*dbTxn).raw
	item, err := txn.Get(b.key(id))
	if err != nil {
		return err
	}
	return item.Value(func(val []byte) error {
		v, err := b.m.Parse(val)
		if err != nil {
			return err
		}
		cb(v)
		return nil
	})
}

func (b *Handler[T, ST]) DeleteTable() error {
	return b.db.DropPrefix(b.tableName())
}

func (b *Handler[T, ST]) DeleteItems(t Transaction, items []ItemID) error {
	txn := t.(*dbTxn).raw
	for _, e := range items {
		if err := txn.Delete(e); err != nil {
			return nil
		}
	}
	return nil
}

func (b *Handler[T, ST]) DeleteItem(t Transaction, item ItemID) error {
	txn := t.(*dbTxn).raw
	return txn.Delete(item)
}
