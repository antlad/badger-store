package badger_store

import (
	"bytes"
	"errors"
	"fmt"

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

type Transaction interface {
	AppendRef(interface{})
	Raw() *badger.Txn
}

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

func IsNotFound(err error) bool {
	return errors.Is(err, badger.ErrKeyNotFound)
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

	if err := b.checkConstraints(t.Raw(), item); err != nil {
		return err
	}

	if err := b.beforePut(t.Raw(), item); err != nil {
		return err
	}

	data, err := b.meta.Serialize(item)
	if err != nil {
		return err
	}

	if err = t.Raw().Set(b.itemKey(b.storeMeta.ID(item)), data); err != nil {
		return err
	}

	t.AppendRef(data)
	return b.afterPut(t.Raw(), item)
}

func (b *Handler[View, Store]) itemKey(id ItemID) []byte {
	r := append(b.tableName(), []byte{'v'}...)
	return append(r, id...)
}

func (b *Handler[View, Store]) tableName() []byte {
	return []byte(b.meta.TableName())
}

func (b *Handler[View, Store]) valuesPrefix() []byte {
	return append(b.tableName(), []byte{'v'}...)
}

func (b *Handler[View, Store]) indexPrefix(name string) []byte {
	r := append(b.tableName(), []byte{'i'}...)
	return append(r, []byte(name)...)
}

func (b *Handler[View, Store]) uniqueIndexKey(name string, indexValue []byte) []byte {
	r := append(b.indexPrefix(name), 'u')
	return append(r, indexValue...)
}

func (b *Handler[View, Store]) matchIndexPrefix(name string, indexValue []byte) []byte {
	r := append(b.indexPrefix(name), 'm')
	return append(r, indexValue...)
}

func (b *Handler[View, Store]) matchIndexKey(name string, indexValue []byte, id ItemID) []byte {
	r := append(b.indexPrefix(name), 'm')
	p := append(r, indexValue...)
	return append(p, id...)
}

func (b *Handler[View, Store]) checkConstraints(tx *badger.Txn, item *Store) error {
	if b.meta.StoreMeta().ID(item) == nil {
		return ErrEmptyID
	}

	for k, v := range b.indexes {
		if v != Unique {
			continue
		}
		iv := b.storeMeta.IndexValue(item, k)
		if len(iv) == 0 {
			continue
		}
		existing, err := tx.Get(b.uniqueIndexKey(k, iv))
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				continue
			}
			return err
		}
		putID := b.storeMeta.ID(item)
		err = existing.Value(func(existingID []byte) error {
			if !bytes.Equal(existingID, putID) {
				return fmt.Errorf("index name %s: %w", k, ErrUniqueConstraintViolation)
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
			if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
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
			err = tx.Delete(b.uniqueIndexKey(k, idxValue))
		case Match:
			err = tx.Delete(b.matchIndexKey(k, idxValue, id))
		}
		if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}
	}
	return nil
}

func (b *Handler[View, Store]) GetByUniqueIndex(t Transaction, indexName string, indexValue []byte, cb func(view *View)) error {
	if len(indexValue) == 0 {
		return ErrEmptyIndexValue
	}

	indexItem, err := t.Raw().Get(b.uniqueIndexKey(indexName, indexValue))
	if err != nil {
		return err
	}
	err = indexItem.Value(func(val []byte) error {
		item, err := t.Raw().Get(b.itemKey(val))
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

func (b *Handler[View, Store]) Iterate(t Transaction, cb func(view *View) error) error {
	it := t.Raw().NewIterator(iteratorOpts(b.valuesPrefix()))
	defer it.Close()

	for it.Rewind(); it.Valid(); it.Next() {
		err := it.Item().Value(func(val []byte) error {
			v, err := b.meta.TakeView(val)
			if err != nil {
				return err
			}
			return cb(v)
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

func (b *Handler[View, Store]) IterateByMatchIndex(t Transaction, indexName string, indexKey []byte, cb func(view *View) error) error {
	matchPrefix := b.matchIndexPrefix(indexName, indexKey)
	it := t.Raw().NewIterator(iteratorOpts(matchPrefix))
	defer it.Close()

	for it.Rewind(); it.Valid(); it.Next() {
		err := it.Item().Value(func(val []byte) error {
			item, err := t.Raw().Get(b.itemKey(val))
			if err != nil {
				if IsNotFound(err) {
					return nil
				}
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

func (b *Handler[View, Store]) GetByID(t Transaction, id ItemID, cb func(view *View) error) error {
	item, err := t.Raw().Get(b.itemKey(id))
	if err != nil {
		return err
	}
	return item.Value(func(val []byte) error {
		v, err := b.meta.TakeView(val)
		if err != nil {
			return err
		}
		return cb(v)
	})
}

func (b *Handler[View, Store]) DeleteTable() error {
	return b.db.DropPrefix(b.tableName())
}

func (b *Handler[View, Store]) DeleteItems(t Transaction, items []ItemID) error {
	for _, e := range items {
		if err := b.DeleteItem(t, e); err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
	}
	return nil
}

func (b *Handler[View, Store]) DeleteByMatchingIndex(t Transaction, indexName string, indexKey []byte) error {
	var IDs []ItemID
	if err := b.IterateByMatchIndex(t, indexName, indexKey, func(view *View) error {
		IDs = append(IDs, b.viewMeta.ID(view))
		return nil
	}); err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
		return err
	}

	return b.DeleteItems(t, IDs)
}

func (b *Handler[View, Store]) DeleteItem(t Transaction, id ItemID) error {
	itemKey := b.itemKey(id)
	if len(b.indexes) > 0 {
		item, err := t.Raw().Get(itemKey)
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return nil
			}
			return err
		}
		err = item.Value(func(val []byte) error {
			v, err := b.meta.TakeView(val)
			if err != nil {
				return err
			}
			return b.onDelete(t.Raw(), v)
		})
		if err != nil {
			return err
		}
	}

	if err := t.Raw().Delete(itemKey); err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
		return err
	}

	return nil
}
