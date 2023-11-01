package badger_store

import (
	"errors"
	"github.com/dgraph-io/badger/v4"
)

type ItemID []byte

type IndexType int

const (
	Unique IndexType = iota
	Match
)

type Index struct {
	IdxType IndexType
	IdxName string
}

type IndexesDesc map[string]IndexType

type ItemMeta[T any] interface {
	ID(*T) ItemID
	IndexValue(t *T, indexName string) []byte
}

type Meta[View any, Store any] interface {
	TableName() string
	TakeView([]byte) (*View, error)
	Serialize(*Store) ([]byte, error)
	Indexes() IndexesDesc
	StoreMeta() ItemMeta[Store]
	ViewMeta() ItemMeta[View]
}

type Handler[View any, Store any] struct {
	db        *badger.DB
	meta      Meta[View, Store]
	viewMeta  ItemMeta[View]
	storeMeta ItemMeta[Store]
	indexes   IndexesDesc
}

func IsStopIteration(err error) bool {
	return errors.Is(err, ErrStopIteration)
}

func NewHandler[View any, Store any](db *badger.DB, meta Meta[View, Store]) *Handler[View, Store] {
	return &Handler[View, Store]{
		db:        db,
		meta:      meta,
		viewMeta:  meta.ViewMeta(),
		storeMeta: meta.StoreMeta(),
		indexes:   meta.Indexes(),
	}
}

func (b *Handler[View, Store]) PutItem(t Transaction, item *Store) error {
	r := t.(*dbTxn)
	txn := r.raw
	data, err := b.meta.Serialize(item)
	if err != nil {
		return err
	}

	if err = txn.Set(b.itemKey(b.storeMeta.ID(item)), data); err != nil {
		return err
	}
	r.refs = append(r.refs, data)
	return nil
}

func (b *Handler[View, Store]) itemKey(id ItemID) []byte {
	return append(b.tableName(), id...)
}

func (b *Handler[View, Store]) tableName() []byte {
	return []byte(b.meta.TableName())
}

func (b *Handler[View, Store]) indexPrefix(name string) []byte {
	return append(b.tableName(), []byte(name)...)
}

func (b *Handler[View, Store]) uniqueIndexKey(name string, indexValue []byte) []byte {
	return append(b.indexPrefix(name), indexValue...)
}

func (b *Handler[View, Store]) matchIndexPrefix(name string, indexValue []byte) []byte {
	return append(b.indexPrefix(name), indexValue...)
}

func (b *Handler[View, Store]) matchIndexKey(name string, indexValue []byte, id ItemID) []byte {
	p := append(b.indexPrefix(name), indexValue...)
	return append(p, id...)
}

func (b *Handler[View, Store]) checkConstraints(tx *badger.Txn, item *Store) error {
	for k, v := range b.indexes {
		if v != Unique {
			continue
		}
		iv := b.storeMeta.IndexValue(item, k)
		if len(iv) == 0 {
			return ErrEmptyIndexValue
		}

		_, err := tx.Get(b.uniqueIndexKey(k, iv))
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				continue
			}
			return err
		}
	}
	return nil
}

func (b *Handler[View, Store]) onPut(tx *badger.Txn, item *Store) error {
	for k, v := range b.indexes {

		idxValue := b.storeMeta.IndexValue(item, k)

		if len(idxValue) == 0 {
			return ErrEmptyIndexValue
		}

		id := b.storeMeta.ID(item)
		var err error
		switch v {
		case Unique:
			err = tx.Set(b.uniqueIndexKey(k, idxValue), id)
		case Match:
			err = tx.Set(b.matchIndexKey(k, idxValue, id), id)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *Handler[View, Store]) onDelete(tx *badger.Txn, item *View) error {
	for k, v := range b.indexes {
		id := b.viewMeta.ID(item)
		idxValue := b.viewMeta.IndexValue(item, k)
		if len(idxValue) == 0 {
			return ErrEmptyIndexValue
		}
		var err error
		switch v {
		case Unique:
			err = tx.Delete(b.uniqueIndexKey(k, idxValue))
		case Match:
			err = tx.Delete(b.matchIndexKey(k, idxValue, id))
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *Handler[View, Store]) GetByUniqueIndex(t Transaction, indexName string, indexValue []byte, cb func(view *View)) error {
	if len(indexValue) == 0 {
		return ErrEmptyIndexValue
	}

	txn := t.(*dbTxn).raw
	indexItem, err := txn.Get(b.uniqueIndexKey(indexName, indexValue))
	if err != nil {
		return err
	}
	err = indexItem.Value(func(val []byte) error {
		item, err := txn.Get(b.itemKey(val))
		if err != nil {
			return err
		}
		return item.Value(func(data []byte) error {
			v, err := b.meta.TakeView(data)
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

func (b *Handler[View, Store]) IterateByMatchIndex(t Transaction, indexName string, indexKey []byte, cb func(view *View) error) error {
	txn := t.(*dbTxn).raw

	it := txn.NewIterator(iteratorOpts(b.matchIndexPrefix(indexName, indexKey)))
	defer it.Close()

	for it.Rewind(); it.Valid(); it.Next() {
		err := it.Item().Value(func(val []byte) error {
			item, err := txn.Get(b.itemKey(val))
			if err != nil {
				return err
			}
			return item.Value(func(data []byte) error {
				v, err := b.meta.TakeView(data)
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

func (b *Handler[View, Store]) ViewItemByID(t Transaction, id ItemID, cb func(view *View)) error {
	txn := t.(*dbTxn).raw
	item, err := txn.Get(b.itemKey(id))
	if err != nil {
		return err
	}
	return item.Value(func(val []byte) error {
		v, err := b.meta.TakeView(val)
		if err != nil {
			return err
		}
		cb(v)
		return nil
	})
}

func (b *Handler[View, Store]) DeleteTable() error {
	return b.db.DropPrefix(b.tableName())
}

func (b *Handler[View, Store]) DeleteItems(t Transaction, items []ItemID) error {
	for _, e := range items {
		if err := b.DeleteItem(t, e); err != nil {
			return nil
		}
	}
	return nil
}

func (b *Handler[View, Store]) DeleteItem(t Transaction, id ItemID) error {
	txn := t.(*dbTxn).raw

	if len(b.indexes) > 0 {
		item, err := txn.Get(b.itemKey(id))
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			v, err := b.meta.TakeView(val)
			if err != nil {
				return err
			}
			return b.onDelete(txn, v)
		})
	}

	if err := txn.Delete(id); err != nil {
		return err
	}

	return nil
}
