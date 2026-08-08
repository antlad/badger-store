package badger_store

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/antlad/badger-store/test"
	"github.com/antlad/badger-store/test/ds"
	"github.com/dgraph-io/badger/v4"
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// openTempDB opens an isolated Badger DB in a per-test temp dir.
func openTempDB(t *testing.T) *BadgerStore {
	t.Helper()
	d, err := NewBadgerDB(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = d.Close() })
	return d
}

func humanHandler(t *testing.T, d *BadgerStore, version int) *Handler[ds.Human, test.Human] {
	t.Helper()
	h := NewHandler[ds.Human, test.Human](d.DB, &HumanTable{
		builder: flatbuffers.NewBuilder(0xFFF),
		version: version,
	})
	require.NoError(t, d.Update(h.Init))
	return h
}

// Match-only Meta: PutItem reaches beforePut without a prior Get.
type matchOnlyTable struct {
	builder *flatbuffers.Builder
}

func (m *matchOnlyTable) TableName() string { return "match_only" }
func (m *matchOnlyTable) Version() int      { return 0 }
func (m *matchOnlyTable) TakeView(data []byte) (*ds.Human, error) {
	return ds.GetRootAsHuman(data, 0), nil
}
func (m *matchOnlyTable) Serialize(h *test.Human) ([]byte, error) {
	data := test.Pack(m.builder, h)
	out := make([]byte, len(data))
	copy(out, data)
	return out, nil
}
func (m *matchOnlyTable) Indexes() IndexesDesc {
	return IndexesDesc{statusIDx: {IdxType: Match, Version: 0}}
}
func (m *matchOnlyTable) StoreMeta() ItemMeta[test.Human] { return &HumanStoreMeta{} }
func (m *matchOnlyTable) ViewMeta() ItemMeta[ds.Human]    { return &HumanViewMeta{} }

func putRow(t *testing.T, d *BadgerStore, h *Handler[ds.Human, test.Human], row test.Human) {
	t.Helper()
	require.NoError(t, d.Update(func(tx Transaction) error { return h.PutItem(tx, &row) }))
}

func matchCount(t *testing.T, d *BadgerStore, h *Handler[ds.Human, test.Human], status string) int {
	t.Helper()
	count := 0
	require.NoError(t, d.View(func(tx Transaction) error {
		return h.IterateByMatchIndex(tx, statusIDx, []byte(status), func(view *ds.Human) error {
			count++
			return nil
		})
	}))
	return count
}

// DeleteByMatchingIndex must delete only the matching rows.
func TestDeleteByMatchingIndex_DeletesOnlyMatching(t *testing.T) {
	d := openTempDB(t)
	h := humanHandler(t, d, 0)

	for i := 0; i < 50; i++ {
		putRow(t, d, h, test.Human{Id: uuid.New(), Name: fmt.Sprintf("a%d", i), Email: fmt.Sprintf("a%d@x.com", i), Status: "active"})
		putRow(t, d, h, test.Human{Id: uuid.New(), Name: fmt.Sprintf("i%d", i), Email: fmt.Sprintf("i%d@x.com", i), Status: "inactive"})
	}

	require.NoError(t, d.Update(func(tx Transaction) error {
		return h.DeleteByMatchingIndex(tx, statusIDx, []byte("active"))
	}))

	require.Equal(t, 0, matchCount(t, d, h, "active"), "active rows should be gone")
	require.Equal(t, 50, matchCount(t, d, h, "inactive"), "inactive rows must survive")
}

// P0-2: DeleteItems must return real errors, not swallow them.
func TestDeleteItems_PropagatesError(t *testing.T) {
	d := openTempDB(t)
	h := humanHandler(t, d, 0)

	row := test.Human{Id: uuid.New(), Name: "x", Email: "x@x.com", Status: "ok"}
	putRow(t, d, h, row)

	// read-only tx makes the delete fail; DeleteItems must surface it
	err := d.View(func(tx Transaction) error {
		return h.DeleteItems(tx, []ItemID{row.Id[:]})
	})
	require.Error(t, err, "DeleteItems swallowed a real delete error")
}

// P0-4: a non-"not found" Get error in beforePut must not panic.
func TestPutItem_GetErrorDoesNotPanic(t *testing.T) {
	d := openTempDB(t)
	h := NewHandler[ds.Human, test.Human](d.DB, &matchOnlyTable{builder: flatbuffers.NewBuilder(256)})
	require.NoError(t, d.Update(h.Init))

	// A discarded transaction makes every Get fail with a non-"not found" error.
	raw := d.DB.NewTransaction(true)
	raw.Discard()
	tx := &txnImpl{raw: raw}

	row := test.Human{Id: uuid.New(), Name: "p", Email: "p@x.com", Status: "ok"}
	require.NotPanics(t, func() {
		err := h.PutItem(tx, &row)
		require.Error(t, err, "PutItem should surface the Get error")
	})
}

// P1-1: meta must not persist if the Init transaction rolls back.
func TestInit_MetaNotSavedIfTxnRollsBack(t *testing.T) {
	d := openTempDB(t)
	h0 := humanHandler(t, d, 0) // creates table meta at version 0
	putRow(t, d, h0, test.Human{Id: uuid.New(), Name: "x", Email: "x@x.com", Status: "ok"})

	// Run a version-1 migration but fail the surrounding transaction.
	h1 := NewHandler[ds.Human, test.Human](d.DB, &HumanTable{
		builder: flatbuffers.NewBuilder(0xFFF),
		version: 1,
	})
	boom := errors.New("boom")
	err := d.Update(func(tx Transaction) error {
		if e := h1.Init(tx); e != nil {
			return e
		}
		return boom
	})
	require.ErrorIs(t, err, boom)

	meta, err := h1.LoadTableMeta()
	require.NoError(t, err)
	require.Equal(t, 0, meta.Version, "meta persisted despite the transaction rolling back")
}

// openSmallBatchDB shrinks the memtable, and with it the per-transaction entry
// ceiling, so a migration outgrows one transaction without a lot of rows.
func openSmallBatchDB(t *testing.T, folder string) *BadgerStore {
	t.Helper()
	opts := badger.DefaultOptions(folder).WithLogger(nil)
	opts.NumVersionsToKeep = 1
	opts.MemTableSize = 1 << 20
	opts.ValueThreshold = 1 << 10 // must stay under the (now small) batch size
	db, err := badger.Open(opts)
	require.NoError(t, err)
	return &BadgerStore{DB: db}
}

// seedRows writes n rows in chunks - a row costs four entries, one record plus
// three index entries.
func seedRows(t *testing.T, d *BadgerStore, h *Handler[ds.Human, test.Human], n int) {
	t.Helper()
	chunk := int(d.DB.MaxBatchCount() / 8)
	for start := 0; start < n; start += chunk {
		end := min(start+chunk, n)
		require.NoError(t, d.Update(func(tx Transaction) error {
			for i := start; i < end; i++ {
				hm := createHuman(i)
				if err := h.PutItem(tx, &hm); err != nil {
					return err
				}
			}
			return nil
		}))
	}
}

// A migration bigger than one badger transaction must still succeed.
func TestInitMigrate_ExceedsOneTransaction(t *testing.T) {
	dir := t.TempDir()
	d := openSmallBatchDB(t, dir)
	h := humanHandler(t, d, 0)

	// 3 indexes per row, so this comfortably outgrows one transaction.
	rows := int(d.DB.MaxBatchCount()/3) + 500
	seedRows(t, d, h, rows)
	require.NoError(t, d.Close())

	d2 := openSmallBatchDB(t, dir)
	t.Cleanup(func() { _ = d2.Close() })
	h2 := NewHandler[ds.Human, test.Human](d2.DB, &HumanTable{
		builder: flatbuffers.NewBuilder(0xFFF),
		version: 1,
	})
	require.NoError(t, d2.Update(h2.Init), "migration must not hit the transaction size limit")

	// Every index must be complete afterwards.
	require.Equal(t, rows/2, matchCount(t, d2, h2, "ok"), "match index incomplete after migration")
	require.NoError(t, d2.View(func(tx Transaction) error {
		return h2.GetByUniqueIndex(tx, emailIDx, []byte("user7@mail.com"), func(view *ds.Human) error { return nil })
	}), "unique index incomplete after migration")
}

// A migration whose transaction never commits must be retried by the next Init,
// even one running the old version.
func TestInitMigrate_RetriedAfterRollback(t *testing.T) {
	dir := t.TempDir()
	d := openSmallBatchDB(t, dir)
	h := humanHandler(t, d, 0)
	seedRows(t, d, h, 200)
	require.Equal(t, 100, matchCount(t, d, h, "ok"))
	require.NoError(t, d.Close())

	// v1 migration that rolls back after Init succeeded.
	d2 := openSmallBatchDB(t, dir)
	h2 := NewHandler[ds.Human, test.Human](d2.DB, &HumanTable{
		builder: flatbuffers.NewBuilder(0xFFF),
		version: 1,
	})
	boom := errors.New("boom")
	require.ErrorIs(t, d2.Update(func(tx Transaction) error {
		if err := h2.Init(tx); err != nil {
			return err
		}
		return boom
	}), boom)

	meta, err := h2.LoadTableMeta()
	require.NoError(t, err)
	require.Equal(t, 0, meta.Version, "meta must not persist across a rollback")

	markers, err := h2.loadRebuildMarkers()
	require.NoError(t, err)
	require.NotEmpty(t, markers, "a rolled-back migration must leave rebuild markers behind")
	require.NoError(t, d2.Close())

	// Operator rolls back to the v0 binary: meta agrees with the schema, so the
	// only thing that can trigger a rebuild is the marker.
	d3 := openSmallBatchDB(t, dir)
	t.Cleanup(func() { _ = d3.Close() })
	h3 := humanHandler(t, d3, 0)

	require.Equal(t, 100, matchCount(t, d3, h3, "ok"), "match index was not rebuilt after the failed migration")
	require.NoError(t, d3.View(func(tx Transaction) error {
		return h3.GetByUniqueIndex(tx, emailIDx, []byte("user7@mail.com"), func(view *ds.Human) error { return nil })
	}), "unique index was not rebuilt after the failed migration")

	markers, err = h3.loadRebuildMarkers()
	require.NoError(t, err)
	require.Empty(t, markers, "markers must be cleared once the rebuild commits")
}

// A match lookup must not return rows whose value only starts with the one asked
// for: the id is appended, so "okX" could otherwise match "ok" + an id from X.
func TestIterateByMatchIndex_PrefixDoesNotLeak(t *testing.T) {
	d := openTempDB(t)
	h := humanHandler(t, d, 0)

	idA := uuid.New()
	idA[0] = 'X'
	rowA := test.Human{Id: idA, Name: "a", Email: "a@x.com", Status: "ok"}
	rowB := test.Human{Id: uuid.New(), Name: "b", Email: "b@x.com", Status: "okX"}
	putRow(t, d, h, rowA)
	putRow(t, d, h, rowB)

	var got []string
	require.NoError(t, d.View(func(tx Transaction) error {
		return h.IterateByMatchIndex(tx, statusIDx, []byte("okX"), func(view *ds.Human) error {
			got = append(got, string(view.Name()))
			return nil
		})
	}))
	require.Equal(t, []string{"b"}, got, "prefix of a longer index value leaked into the result")

	// ...and the shorter value still finds its own row.
	got = nil
	require.NoError(t, d.View(func(tx Transaction) error {
		return h.IterateByMatchIndex(tx, statusIDx, []byte("ok"), func(view *ds.Human) error {
			got = append(got, string(view.Name()))
			return nil
		})
	}))
	require.Equal(t, []string{"a"}, got)
}

// GetByUniqueIndex must return a callback error, like every other read.
func TestGetByUniqueIndex_PropagatesCallbackError(t *testing.T) {
	d := openTempDB(t)
	h := humanHandler(t, d, 0)
	putRow(t, d, h, test.Human{Id: uuid.New(), Name: "c", Email: "c@x.com", Status: "ok"})

	boom := errors.New("boom")
	err := d.View(func(tx Transaction) error {
		return h.GetByUniqueIndex(tx, emailIDx, []byte("c@x.com"), func(view *ds.Human) error {
			return boom
		})
	})
	require.ErrorIs(t, err, boom)

	// ...and a callback that succeeds still reports success.
	require.NoError(t, d.View(func(tx Transaction) error {
		return h.GetByUniqueIndex(tx, emailIDx, []byte("c@x.com"), func(view *ds.Human) error {
			return nil
		})
	}))
}

// Both scans must stop on ErrStopIteration and report success, not the sentinel.
func TestErrStopIteration_EndsScansWithoutError(t *testing.T) {
	d := openTempDB(t)
	h := humanHandler(t, d, 0)
	seedRows(t, d, h, 40) // 20 of them have status "ok"

	t.Run("Iterate", func(t *testing.T) {
		seen := 0
		require.NoError(t, d.View(func(tx Transaction) error {
			return h.Iterate(tx, func(view *ds.Human) error {
				seen++
				if seen == 3 {
					return ErrStopIteration
				}
				return nil
			})
		}))
		require.Equal(t, 3, seen, "Iterate kept going after ErrStopIteration")
	})

	t.Run("IterateByMatchIndex", func(t *testing.T) {
		seen := 0
		require.NoError(t, d.View(func(tx Transaction) error {
			return h.IterateByMatchIndex(tx, statusIDx, []byte("ok"), func(view *ds.Human) error {
				seen++
				if seen == 2 {
					return ErrStopIteration
				}
				return nil
			})
		}))
		require.Equal(t, 2, seen, "IterateByMatchIndex kept going after ErrStopIteration")
	})

	t.Run("wrapped sentinel is recognized", func(t *testing.T) {
		seen := 0
		require.NoError(t, d.View(func(tx Transaction) error {
			return h.Iterate(tx, func(view *ds.Human) error {
				seen++
				return fmt.Errorf("caller ran out of budget: %w", ErrStopIteration)
			})
		}))
		require.Equal(t, 1, seen)
	})

	t.Run("any other error is returned as is", func(t *testing.T) {
		boom := errors.New("boom")
		err := d.View(func(tx Transaction) error {
			return h.Iterate(tx, func(view *ds.Human) error { return boom })
		})
		require.ErrorIs(t, err, boom)
	})
}

// The documented cancellation pattern: watch ctx, return ErrStopIteration.
func TestErrStopIteration_HonoursContextCancellation(t *testing.T) {
	d := openTempDB(t)
	h := humanHandler(t, d, 0)
	seedRows(t, d, h, 100)

	ctx, cancel := context.WithCancel(context.Background())
	seen := 0
	require.NoError(t, d.View(func(tx Transaction) error {
		return h.Iterate(tx, func(view *ds.Human) error {
			select {
			case <-ctx.Done():
				return ErrStopIteration
			default:
			}
			seen++
			if seen == 10 {
				cancel() // something elsewhere gives up
			}
			return nil
		})
	}))
	require.Equal(t, 10, seen, "scan did not stop at cancellation")
	require.ErrorIs(t, ctx.Err(), context.Canceled)
}

// An id of length zero would collide with the table's value prefix.
func TestPutItem_RejectsZeroLengthID(t *testing.T) {
	d := openTempDB(t)
	h := NewHandler[ds.Human, test.Human](d.DB, &emptyIDTable{
		HumanTable: &HumanTable{builder: flatbuffers.NewBuilder(256), version: 0},
	})
	require.NoError(t, d.Update(h.Init))

	row := test.Human{Id: uuid.New(), Name: "z", Email: "z@x.com", Status: "ok"}
	err := d.Update(func(tx Transaction) error { return h.PutItem(tx, &row) })
	require.ErrorIs(t, err, ErrEmptyID)
}

type emptyIDStoreMeta struct{}

func (emptyIDStoreMeta) ID(*test.Human) ItemID                 { return ItemID{} } // non-nil, empty
func (emptyIDStoreMeta) IndexValue(*test.Human, string) []byte { return nil }

type emptyIDTable struct{ *HumanTable }

func (t *emptyIDTable) TableName() string               { return "empty_id" }
func (t *emptyIDTable) StoreMeta() ItemMeta[test.Human] { return emptyIDStoreMeta{} }

// Badger holds the value until commit, so an indexed id must not alias a struct
// the caller can still change.
func TestPutItem_IndexKeepsIDAcrossLaterMutation(t *testing.T) {
	d := openTempDB(t)
	h := humanHandler(t, d, 0)

	row := test.Human{Id: uuid.New(), Name: "m", Email: "m@x.com", Status: "ok"}
	original := row.Id
	require.NoError(t, d.Update(func(tx Transaction) error {
		if err := h.PutItem(tx, &row); err != nil {
			return err
		}
		// caller reuses the struct before the transaction commits
		row.Id = uuid.New()
		return nil
	}))

	found := false
	require.NoError(t, d.View(func(tx Transaction) error {
		return h.GetByUniqueIndex(tx, emailIDx, []byte("m@x.com"), func(view *ds.Human) error {
			found = true
			require.Equal(t, original[:], view.IdBytes())
			return nil
		})
	}))
	require.True(t, found, "unique index entry did not survive the caller mutating the id")
}

// P1-4: migration rebuild must skip empty index values, like normal writes do.
func TestInitMigrate_SkipsEmptyIndexValues(t *testing.T) {
	d := openTempDB(t)
	h := humanHandler(t, d, 0)

	// Empty email -> unique "email" index must have no entry for it.
	putRow(t, d, h, test.Human{Id: uuid.New(), Name: "solo", Email: "", Status: "ok"})

	// Bump the version to force an index rebuild.
	h2 := humanHandler(t, d, 1)

	emptyKey := h2.uniqueIndexKey(emailIDx, nil)
	err := d.DB.View(func(txn *badger.Txn) error {
		_, getErr := txn.Get(emptyKey)
		return getErr
	})
	require.ErrorIs(t, err, badger.ErrKeyNotFound, "rebuild created an index entry for an empty value")
}
