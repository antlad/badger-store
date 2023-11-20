package badger_store

import (
	"bytes"
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

func (b *Handler[View, Store]) PutItem(t Txn, item *Store) error {
	txn := t.(*txnImpl)
	if err := b.checkConstraints(txn.raw, item); err != nil {
		return err
	}
	if err := b.beforePut(txn.raw, item); err != nil {
		return err
	}

	data, err := b.meta.Serialize(item)
	if err != nil {
		return err
	}

	if err = txn.raw.Set(b.itemKey(b.storeMeta.ID(item)), data); err != nil {
		return err
	}

	txn.refs = append(txn.refs, data)
	return b.afterPut(txn.raw, item)
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
			continue
		}
		uID := b.uniqueIndexKey(k, iv)
		existing, err := tx.Get(uID)
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				continue
			}
			return err
		}
		putID := b.storeMeta.ID(item)
		err = existing.Value(func(existingID []byte) error {
			if !bytes.Equal(existingID, putID) {
				return ErrUniqueConstraintViolation
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *Handler[View, Store]) beforePut(tx *badger.Txn, item *Store) error {
	if len(b.indexes) < 1 {
		return nil
	}
	itemID := b.storeMeta.ID(item)
	existing, err := tx.Get(b.itemKey(itemID))
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
	}
	err = existing.Value(func(val []byte) error {
		view, err := b.meta.TakeView(val)
		if err != nil {
			return err
		}

		for k, v := range b.indexes {
			idxValue := b.viewMeta.IndexValue(view, k)
			if len(idxValue) == 0 {
				continue
			}

			switch v {
			case Unique:
				err = tx.Delete(b.uniqueIndexKey(k, idxValue))
			case Match:
				err = tx.Delete(b.matchIndexKey(k, idxValue, itemID))
			}
			if err != nil {
				return err
			}
		}
		return nil
	})

	return err
}

func (b *Handler[View, Store]) afterPut(tx *badger.Txn, item *Store) error {
	for k, v := range b.indexes {

		idxValue := b.storeMeta.IndexValue(item, k)
		if len(idxValue) == 0 {
			continue
		}

		id := b.storeMeta.ID(item)
		var err error
		switch v {
		case Unique:
			uID := b.uniqueIndexKey(k, idxValue)
			err = tx.Set(uID, id)
		case Match:
			mID := b.matchIndexKey(k, idxValue, id)
			err = tx.Set(mID, id)
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
			continue
		}
		var err error
		switch v {
		case Unique:
			uID := b.uniqueIndexKey(k, idxValue)
			err = tx.Delete(uID)
		case Match:
			mID := b.matchIndexKey(k, idxValue, id)
			err = tx.Delete(mID)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *Handler[View, Store]) GetByUniqueIndex(t Txn, indexName string, indexValue []byte, cb func(view *View)) error {
	if len(indexValue) == 0 {
		return ErrEmptyIndexValue
	}

	txn := t.(*txnImpl)
	indexItem, err := txn.raw.Get(b.uniqueIndexKey(indexName, indexValue))
	if err != nil {
		return err
	}
	err = indexItem.Value(func(val []byte) error {
		item, err := txn.raw.Get(b.itemKey(val))
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

func (b *Handler[View, Store]) IterateByMatchIndex(t Txn, indexName string, indexKey []byte, cb func(view *View) error) error {
	txn := t.(*txnImpl)

	matchPrefix := b.matchIndexPrefix(indexName, indexKey)
	it := txn.raw.NewIterator(iteratorOpts(matchPrefix))
	defer it.Close()

	for it.Rewind(); it.Valid(); it.Next() {
		err := it.Item().Value(func(val []byte) error {
			item, err := txn.raw.Get(b.itemKey(val))
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

func (b *Handler[View, Store]) GetByID(t Txn, id ItemID, cb func(view *View)) error {
	txn := t.(*txnImpl)
	item, err := txn.raw.Get(b.itemKey(id))
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

func (b *Handler[View, Store]) DeleteItems(t Txn, items []ItemID) error {
	for _, e := range items {
		if err := b.DeleteItem(t, e); err != nil {
			return nil
		}
	}
	return nil
}

func (b *Handler[View, Store]) DeleteByMatchingIndex(t Txn, indexName string, indexKey []byte) error {
	var IDs []ItemID
	if err := b.IterateByMatchIndex(t, indexName, indexKey, func(view *View) error {
		IDs = append(IDs, b.viewMeta.ID(view))
		return nil
	}); err != nil {
		return err
	}

	return b.DeleteItems(t, IDs)
}

func (b *Handler[View, Store]) DeleteItem(t Txn, id ItemID) error {
	txn := t.(*txnImpl)

	if len(b.indexes) > 0 {
		item, err := txn.raw.Get(b.itemKey(id))
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			v, err := b.meta.TakeView(val)
			if err != nil {
				return err
			}
			return b.onDelete(txn.raw, v)
		})
	}

	if err := txn.raw.Delete(id); err != nil {
		return err
	}

	return nil
}
