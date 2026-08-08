package badger_store

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/dgraph-io/badger/v4"
)

// ItemID is a record's primary key. All ids in a table should be the same length.
type ItemID []byte

// IndexType selects how a secondary index behaves.
type IndexType int

const (
	// Unique allows at most one record per index value.
	Unique IndexType = iota
	// Match allows many records per index value.
	Match
)

var (
	metaByte   = byte('t')
	indexByte  = byte('i')
	uniqueByte = byte('u')
	matchByte  = byte('m')
	valueByte  = byte('v')
)

// rebuildMarker keys record an index that was dropped and not yet rebuilt.
const rebuildMarker = "rebuild:"

// TableMeta is the schema stored on disk for a table.
type TableMeta struct {
	Name    string      `json:"name"`
	Version int         `json:"version"`
	Indexes IndexesDesc `json:"indexes"`
}

// IndexDesc describes one secondary index. Changing either field makes the next
// Handler.Init rebuild it.
type IndexDesc struct {
	IdxType IndexType `json:"type"`
	Version int       `json:"version"`
}

// IndexesDesc maps index name to its definition. No name may be a prefix of
// another in the same table.
type IndexesDesc map[string]IndexDesc

// Transaction is a Badger transaction as handed to Handler methods. Get one
// from BadgerStore.View or BadgerStore.Update.
type Transaction interface {
	// AppendRef keeps a reference alive for the lifetime of the transaction.
	//
	// Deprecated: Badger already retains the slices it is given until commit.
	AppendRef(interface{})
	// Raw exposes the underlying Badger transaction.
	Raw() *badger.Txn
}

// ItemMeta reads the id and index values of a record. It is implemented twice
// per record type, once over Store and once over View, so the store can rebuild
// index entries from stored bytes without decoding them.
//
// The two must return identical bytes for every index. Entries are written from
// the Store side and deleted from the View side, so if they drift the delete
// targets a key that was never written: the stale entry survives and lookups
// return wrong rows. Nothing detects this - no error is reported anywhere.
type ItemMeta[T any] interface {
	// ID returns the record's primary key.
	ID(*T) ItemID
	// IndexValue returns the value to index under indexName. Empty means "leave
	// this record out of that index".
	IndexValue(t *T, indexName string) []byte
}

// Meta describes one record type: how it is named, versioned, serialized,
// viewed and indexed.
type Meta[View any, Store any] interface {
	// TableName is the key namespace for this record type. No table name may be
	// a prefix of another.
	TableName() string
	// Version is the table's schema version.
	Version() int
	// TakeView returns a read accessor over stored bytes, valid only for the
	// duration of the callback they were handed to.
	TakeView([]byte) (*View, error)
	// Serialize encodes a record. It must return a slice the store may keep,
	// and must be safe for concurrent use.
	Serialize(*Store) ([]byte, error)
	// Indexes declares the secondary indexes for this table.
	Indexes() IndexesDesc
	// StoreMeta reads a record being written.
	StoreMeta() ItemMeta[Store]
	// ViewMeta reads stored bytes.
	ViewMeta() ItemMeta[View]
}

// Handler is the entry point for one record type. Call Init once at startup.
type Handler[View any, Store any] struct {
	db        *badger.DB
	meta      Meta[View, Store]
	viewMeta  ItemMeta[View]
	storeMeta ItemMeta[Store]
	indexes   IndexesDesc
}

// IsStopIteration reports whether err is or wraps ErrStopIteration.
func IsStopIteration(err error) bool {
	return errors.Is(err, ErrStopIteration)
}

// IsNotFound reports whether err means "no such record or key".
func IsNotFound(err error) bool {
	return errors.Is(err, badger.ErrKeyNotFound)
}

// NewHandler builds a handler for the record type described by meta. StoreMeta,
// ViewMeta and Indexes are read once, so they must not change afterwards.
func NewHandler[View any, Store any](db *badger.DB, meta Meta[View, Store]) *Handler[View, Store] {
	return &Handler[View, Store]{
		db:        db,
		meta:      meta,
		viewMeta:  meta.ViewMeta(),
		storeMeta: meta.StoreMeta(),
		indexes:   meta.Indexes(),
	}
}

// LoadTableMeta reads the schema currently stored on disk. It returns an error
// for which IsNotFound reports true if the table has never been initialized.
func (b *Handler[View, Store]) LoadTableMeta() (meta TableMeta, err error) {
	err = b.db.View(func(tx *badger.Txn) error {
		item, err2 := tx.Get(b.tableMetaKey())
		if err2 != nil {
			return err2
		}
		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &meta)
		})
	})
	return
}

// DropTableMeta removes the stored schema, leaving records and index entries in
// place. The next Init then treats the table as new and does not migrate it.
func (b *Handler[View, Store]) DropTableMeta() error {
	return b.db.DropPrefix(append(b.tableName(), metaByte))
}

func (b *Handler[View, Store]) writeTableMeta(tx *badger.Txn) error {
	meta := TableMeta{
		Name:    b.meta.TableName(),
		Version: b.meta.Version(),
		Indexes: b.meta.Indexes(),
	}
	data, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	return tx.Set(b.tableMetaKey(), data)
}

// SaveTableMeta writes the declared schema in its own transaction, without
// migrating anything. Prefer Init.
func (b *Handler[View, Store]) SaveTableMeta() error {
	return b.db.Update(b.writeTableMeta)
}

// Init brings the indexes on disk in line with the ones the code declares, and
// records the new schema. Call it once per handler at startup:
//
//	err := store.Update(handler.Init)
//
// An index dropped from Indexes has its keys removed; one whose type or version
// changed is rebuilt from the table's records; an unchanged one is left alone.
//
// Run it before serving traffic: dropping an index prefix blocks writes to the
// whole database, so concurrent commits can fail with badger.ErrBlockedWrites.
// On a read-only database Init returns an error.
//
// A migration that fails, or whose transaction rolls back, records nothing and
// is redone by the next Init.
func (b *Handler[View, Store]) Init(tx Transaction) error {
	metaFound := true
	oldMeta, err := b.LoadTableMeta()
	if err != nil {
		if !IsNotFound(err) {
			return err
		}
		metaFound = false
	}

	newIndexes := b.meta.Indexes()

	if err = b.dropRemovedIndexes(oldMeta.Indexes, newIndexes); err != nil {
		return err
	}

	pending, err := b.loadRebuildMarkers()
	if err != nil {
		return err
	}

	rebuild := planRebuild(oldMeta.Indexes, newIndexes, pending, metaFound)
	if len(rebuild) == 0 {
		return b.writeTableMeta(tx.Raw())
	}

	// Own transaction, so the markers outlive a rollback of the caller's.
	if err = b.setRebuildMarkers(rebuild); err != nil {
		return err
	}

	if err = b.dropStaleIndexes(rebuild, oldMeta.Indexes, pending); err != nil {
		return err
	}

	if err = b.rebuildIndexes(tx, rebuild, newIndexes); err != nil {
		return err
	}

	// On the caller's transaction, last: migrated only if the caller commits.
	if err = b.clearRebuildMarkers(tx.Raw(), rebuild); err != nil {
		return err
	}
	return b.writeTableMeta(tx.Raw())
}

// dropRemovedIndexes removes the keys of indexes that are no longer declared.
func (b *Handler[View, Store]) dropRemovedIndexes(old, current IndexesDesc) error {
	for k := range old {
		if _, ok := current[k]; ok {
			continue
		}
		if err := b.db.DropPrefix(b.indexPrefix(k)); err != nil {
			return err
		}
	}
	return nil
}

// planRebuild lists the indexes to rebuild: those whose definition changed, and
// those an earlier Init dropped without finishing.
func planRebuild(old, current IndexesDesc, pending map[string]struct{}, metaFound bool) []string {
	var rebuild []string
	for k, v := range current {
		if _, stale := pending[k]; stale {
			rebuild = append(rebuild, k)
			continue
		}
		if !metaFound {
			continue // no stored schema to migrate from: the table is new
		}
		if m, ok := old[k]; ok && m.IdxType == v.IdxType && m.Version == v.Version {
			continue
		}
		rebuild = append(rebuild, k)
	}
	sort.Strings(rebuild)
	return rebuild
}

// dropStaleIndexes clears what the indexes about to be rebuilt hold today.
func (b *Handler[View, Store]) dropStaleIndexes(rebuild []string, old IndexesDesc, pending map[string]struct{}) error {
	for _, k := range rebuild {
		_, existed := old[k]
		if _, stale := pending[k]; !existed && !stale {
			continue // never existed, nothing to drop
		}
		if err := b.db.DropPrefix(b.indexPrefix(k)); err != nil {
			return err
		}
	}
	return nil
}

// rebuildIndexes writes the entries for names in one pass over the table.
//
// It uses a WriteBatch, not the caller's transaction, which caps at badger's
// MaxBatchCount entries: a real table would fail with ErrTxnTooBig.
func (b *Handler[View, Store]) rebuildIndexes(t Transaction, names []string, indexes IndexesDesc) error {
	wb := b.db.NewWriteBatch()
	defer wb.Cancel()

	err := b.Iterate(t, func(view *View) error {
		// copy: view bytes die with the callback, the batch keeps them to commit
		id := append(ItemID(nil), b.viewMeta.ID(view)...)
		for _, name := range names {
			indexVal := b.viewMeta.IndexValue(view, name)
			if len(indexVal) == 0 {
				continue
			}
			var err error
			switch indexes[name].IdxType {
			case Unique:
				err = wb.Set(b.uniqueIndexKey(name, indexVal), id)
			case Match:
				err = wb.Set(b.matchIndexKey(name, indexVal, id), id)
			}
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	return wb.Flush()
}

func (b *Handler[View, Store]) rebuildMarkerPrefix() []byte {
	r := append(b.tableName(), metaByte)
	return append(r, []byte(rebuildMarker)...)
}

func (b *Handler[View, Store]) rebuildMarkerKey(indexName string) []byte {
	return append(b.rebuildMarkerPrefix(), []byte(indexName)...)
}

// loadRebuildMarkers reports the indexes an earlier Init dropped but did not
// finish rebuilding.
func (b *Handler[View, Store]) loadRebuildMarkers() (map[string]struct{}, error) {
	prefix := b.rebuildMarkerPrefix()
	markers := make(map[string]struct{})
	err := b.db.View(func(tx *badger.Txn) error {
		opts := iteratorOpts(prefix)
		opts.PrefetchValues = false
		it := tx.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			markers[string(it.Item().Key()[len(prefix):])] = struct{}{}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return markers, nil
}

func (b *Handler[View, Store]) setRebuildMarkers(names []string) error {
	return b.db.Update(func(tx *badger.Txn) error {
		for _, name := range names {
			if err := tx.Set(b.rebuildMarkerKey(name), nil); err != nil {
				return err
			}
		}
		return nil
	})
}

func (b *Handler[View, Store]) clearRebuildMarkers(tx *badger.Txn, names []string) error {
	for _, name := range names {
		if err := tx.Delete(b.rebuildMarkerKey(name)); err != nil {
			return err
		}
	}
	return nil
}

// PutItem inserts or replaces a record and updates its index entries, all
// within t. Returns ErrEmptyID if the record has no primary key, and
// ErrUniqueConstraintViolation if another record holds one of its unique values.
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

func (b *Handler[View, Store]) tableMetaKey() []byte {
	r := append(b.tableName(), metaByte)
	r = append(r, []byte{'m', 'e', 't', 'a'}...)
	return r
}

func (b *Handler[View, Store]) itemKey(id ItemID) []byte {
	r := append(b.tableName(), valueByte)
	return append(r, id...)
}

func (b *Handler[View, Store]) tableName() []byte {
	return []byte(b.meta.TableName())
}

func (b *Handler[View, Store]) valuesPrefix() []byte {
	return append(b.tableName(), valueByte)
}

func (b *Handler[View, Store]) indexPrefix(name string) []byte {
	r := append(b.tableName(), indexByte)
	return append(r, []byte(name)...)
}

func (b *Handler[View, Store]) uniqueIndexKey(name string, indexValue []byte) []byte {
	r := append(b.indexPrefix(name), uniqueByte)
	return append(r, indexValue...)
}

func (b *Handler[View, Store]) matchIndexPrefix(name string, indexValue []byte) []byte {
	r := append(b.indexPrefix(name), matchByte)
	return append(r, indexValue...)
}

func (b *Handler[View, Store]) matchIndexKey(name string, indexValue []byte, id ItemID) []byte {
	r := append(b.indexPrefix(name), matchByte)
	r = append(r, indexValue...)
	return append(r, id...)
}

func (b *Handler[View, Store]) checkConstraints(tx *badger.Txn, item *Store) error {
	// len, not nil: an empty id builds a key equal to the table's value prefix
	if len(b.storeMeta.ID(item)) == 0 {
		return ErrEmptyID
	}

	for k, v := range b.indexes {
		if v.IdxType != Unique {
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
		return err
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

			switch v.IdxType {
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
	var id ItemID
	for k, v := range b.indexes {
		idxValue := b.storeMeta.IndexValue(item, k)
		if len(idxValue) == 0 {
			continue
		}

		if id == nil {
			// copy: badger keeps the slice until commit, the caller may reuse it
			id = append(ItemID(nil), b.storeMeta.ID(item)...)
		}
		var err error
		switch v.IdxType {
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
		switch v.IdxType {
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

// GetByUniqueIndex passes the record holding indexValue to cb. Returns
// ErrEmptyIndexValue for an empty value, a not-found error if nothing holds it,
// and cb's error unchanged.
//
// The view is only valid inside cb.
func (b *Handler[View, Store]) GetByUniqueIndex(t Transaction, indexName string, indexValue []byte, cb func(view *View) error) error {
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
			return cb(v)
		})
	})
	return err
}

func iteratorOpts(prefix []byte) badger.IteratorOptions {
	opts := badger.DefaultIteratorOptions
	opts.Prefix = prefix
	return opts
}

// Iterate calls cb for every record in the table, in primary key order.
// Returning ErrStopIteration from cb ends the scan without an error; any other
// error is returned as is.
//
// The view is only valid inside cb.
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

// IterateByMatchIndex calls cb for every record whose Match index value equals
// indexKey. Returning ErrStopIteration from cb ends the scan without an error.
// Index entries whose record has gone are skipped.
//
// The view is only valid inside cb.
func (b *Handler[View, Store]) IterateByMatchIndex(t Transaction, indexName string, indexKey []byte, cb func(view *View) error) error {
	matchPrefix := b.matchIndexPrefix(indexName, indexKey)
	it := t.Raw().NewIterator(iteratorOpts(matchPrefix))
	defer it.Close()

	for it.Rewind(); it.Valid(); it.Next() {
		entry := it.Item()
		err := entry.Value(func(val []byte) error {
			// The key is matchPrefix+id and val is that id, so an exact hit is
			// exactly this long. Longer means the row's index value only starts
			// with the one asked for, which is a different value.
			if len(entry.Key()) != len(matchPrefix)+len(val) {
				return nil
			}

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

// GetByID passes the record with the given primary key to cb, or returns a
// not-found error.
//
// The view is only valid inside cb.
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

// DeleteTable drops every key of this table - records, indexes and metadata -
// in its own transaction. Blocks writes to the whole database while it runs.
func (b *Handler[View, Store]) DeleteTable() error {
	return b.db.DropPrefix(b.tableName())
}

// DeleteItems deletes each record and its index entries, skipping ids that are
// not present. All the deletes go into t, so a long list can fail with
// badger.ErrTxnTooBig - delete in chunks.
func (b *Handler[View, Store]) DeleteItems(t Transaction, items []ItemID) error {
	for _, e := range items {
		if err := b.DeleteItem(t, e); err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}
	}
	return nil
}

// DeleteByMatchingIndex deletes every record whose Match index value equals
// indexKey. Ids are collected first and deleted in t, so memory and transaction
// size both grow with the number of matches.
func (b *Handler[View, Store]) DeleteByMatchingIndex(t Transaction, indexName string, indexKey []byte) error {
	var IDs []ItemID
	if err := b.IterateByMatchIndex(t, indexName, indexKey, func(view *View) error {
		// copy: view bytes are only valid inside this callback
		IDs = append(IDs, append(ItemID(nil), b.viewMeta.ID(view)...))
		return nil
	}); err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
		return err
	}

	return b.DeleteItems(t, IDs)
}

// DeleteItem deletes a record and its index entries. Deleting an id that is not
// present is not an error.
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
