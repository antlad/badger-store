package badger_store

import (
	"fmt"
	"testing"

	"github.com/antlad/badger-store/test"
	"github.com/antlad/badger-store/test/ds"
	"github.com/dgraph-io/badger/v4"
	flatbuffers "github.com/google/flatbuffers/go"
	uuid "github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

const (
	emailIDx  = "xMl"
	nameIDx   = "xNm"
	statusIDx = "xSt"
)

func NewBadgerDB(folder string) (*BadgerDB, error) {
	opts := badger.DefaultOptions(folder)
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
		DB: b,
	}
	return d, nil
}

func prepareDB(t *testing.T) (*BadgerDB, *Handler[ds.Human, test.Human]) {
	d, err := NewBadgerDB("/tmp/store_test")
	require.NoError(t, err)
	err = d.Reset()
	require.NoError(t, err)
	h := NewHandler[ds.Human, test.Human](d.DB, &HumanTable{builder: flatbuffers.NewBuilder(0xFFF)})
	return d, h
}

type HumanStoreMeta struct {
}

func (m *HumanStoreMeta) ID(h *test.Human) ItemID {
	return h.Id[:]
}

func (m *HumanStoreMeta) IndexValue(h *test.Human, indexName string) []byte {
	switch indexName {
	case emailIDx:
		return []byte(h.Email)
	case statusIDx:
		return []byte(h.Status)
	case nameIDx:
		return []byte(h.Name)
	}
	panic("unexpected index name")
}

type HumanViewMeta struct {
}

func (m *HumanViewMeta) ID(h *ds.Human) ItemID {
	return h.IdBytes()
}

func (m *HumanViewMeta) IndexValue(h *ds.Human, indexName string) []byte {
	switch indexName {
	case emailIDx:
		return h.Email()
	case statusIDx:
		return h.Status()
	case nameIDx:
		return h.Name()
	}
	panic("unexpected index name")
}

type HumanTable struct {
	builder *flatbuffers.Builder
}

func (m *HumanTable) TableName() string {
	return "human"
}

func (m *HumanTable) TakeView(data []byte) (*ds.Human, error) {
	return ds.GetRootAsHuman(data, 0), nil
}

func (m *HumanTable) Serialize(h *test.Human) ([]byte, error) {
	data := test.Pack(m.builder, h)
	pack := make([]byte, len(data))
	copy(pack, data)
	return pack, nil
}

func (m *HumanTable) Indexes() IndexesDesc {
	return map[string]IndexType{
		emailIDx:  Unique,
		statusIDx: Match,
		nameIDx:   Unique,
	}
}

func (m *HumanTable) StoreMeta() ItemMeta[test.Human] {
	return &HumanStoreMeta{}
}

func (m *HumanTable) ViewMeta() ItemMeta[ds.Human] {
	return &HumanViewMeta{}
}

func createHuman(i int) test.Human {
	status := "bad"
	if i%2 == 0 {
		status = "ok"
	}

	return test.Human{
		Id:     uuid.New(),
		Age:    i,
		Name:   fmt.Sprintf("name_%d", i),
		Email:  fmt.Sprintf("user%d@mail.com", i),
		Status: status,
	}
}

func checkViewMatch(t *testing.T, h test.Human, view *ds.Human) {
	require.Equal(t, h.Email, string(view.Email()))
	require.Equal(t, h.Id[:], view.IdBytes())
	require.Equal(t, h.Status, string(view.Status()))
	require.Equal(t, h.Name, string(view.Name()))
	require.Equal(t, h.Age, int(view.Age()))
}

func TestBase(t *testing.T) {
	d, h := prepareDB(t)
	defer d.Close()

	h1 := createHuman(1)
	err := d.Update(func(tx Txn) error {
		return h.PutItem(tx, &h1)
	})
	require.NoError(t, err)

	found := false
	err = d.View(func(tx Txn) error {
		return h.GetByID(tx, h1.Id[:], func(view *ds.Human) {
			found = true
			checkViewMatch(t, h1, view)
		})
	})
	require.NoError(t, err)
	require.True(t, found)

}

func TestMatch(t *testing.T) {
	d, h := prepareDB(t)
	defer d.Close()

	items := make([]test.Human, 0, 100)

	err := d.Update(func(tx Txn) error {
		for i := 0; i < 100; i++ {
			hm := createHuman(i)
			items = append(items, hm)
			err := h.PutItem(tx, &hm)
			require.NoError(t, err)
		}
		return nil
	})

	h1 := items[42]
	expectedCount := 50

	err = d.View(func(tx Txn) error {
		found := false
		err = h.GetByID(tx, h1.Id[:], func(view *ds.Human) {
			checkViewMatch(t, h1, view)
			found = true
		})
		require.NoError(t, err)
		require.True(t, found)

		count := 0
		err = h.IterateByMatchIndex(tx, statusIDx, []byte("ok"), func(view *ds.Human) error {
			count++
			require.Equal(t, "ok", string(view.Status()))
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, expectedCount, count)
		return nil
	})
	require.NoError(t, err)

	deleteCount := 40
	IDs := make([]ItemID, 0, deleteCount)
	for i := 0; i < deleteCount; i++ {
		v := items[i]
		IDs = append(IDs, v.Id[:])
		t.Log("delete id=", v.Id.String())
	}

	err = d.Update(func(tx Txn) error {
		err = h.DeleteItems(tx, IDs)

		require.NoError(t, err)
		return nil
	})
	require.NoError(t, err)

	err = d.View(func(tx Txn) error {
		found := false
		err = h.GetByID(tx, h1.Id[:], func(view *ds.Human) {
			checkViewMatch(t, h1, view)
			found = true
		})
		require.NoError(t, err)
		require.True(t, found)

		count := 0

		t.Log("iterate after delete:")
		err = h.IterateByMatchIndex(tx, statusIDx, []byte("ok"), func(view *ds.Human) error {
			id, err := uuid.FromBytes(view.IdBytes())
			require.NoError(t, err)
			t.Log("id=", id.String())
			require.Equal(t, "ok", string(view.Status()))
			count++
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, expectedCount-deleteCount/2, count)
		return nil
	})
	require.NoError(t, err)
}

func TestUniqueConstrain(t *testing.T) {
	d, h := prepareDB(t)
	defer d.Close()
	h1 := createHuman(0)
	h2 := createHuman(1)
	h2.Email = h1.Email
	err := d.Update(func(tx Txn) error {
		return h.PutItem(tx, &h1)
	})
	require.NoError(t, err)

	err = d.Update(func(tx Txn) error {
		return h.PutItem(tx, &h2)
	})
	require.Error(t, err)
}

func TestIndexUpdate(t *testing.T) {
	d, h := prepareDB(t)
	defer d.Close()

	items := make([]test.Human, 0, 10)
	err := d.Update(func(tx Txn) error {
		for i := 0; i < 10; i++ {
			hm := createHuman(i)
			items = append(items, hm)
			err := h.PutItem(tx, &hm)
			require.NoError(t, err)
		}
		return nil
	})

	err = d.View(func(tx Txn) error {
		err = h.GetByUniqueIndex(tx, nameIDx, []byte("missing"), func(view *ds.Human) {
		})
		require.ErrorIs(t, err, badger.ErrKeyNotFound)
		err = h.GetByID(tx, []byte{1, 2, 3}, func(view *ds.Human) {
		})
		require.ErrorIs(t, err, badger.ErrKeyNotFound)
		count := 0
		err = h.IterateByMatchIndex(tx, statusIDx, []byte("ok"), func(view *ds.Human) error {
			count++
			return nil
		})
		require.Equal(t, 5, count)
		return nil
	})
	require.NoError(t, err)

	err = d.Update(func(tx Txn) error {
		h1 := items[3]
		original := h1
		err = h.GetByUniqueIndex(tx, emailIDx, []byte(h1.Email), func(view *ds.Human) {
			checkViewMatch(t, h1, view)
		})
		h1.Email = "new_email@mail.com"
		h1.Status = "some"

		err = h.PutItem(tx, &h1)
		require.NoError(t, err)

		count := 0
		err = h.IterateByMatchIndex(tx, statusIDx, []byte("bad"), func(view *ds.Human) error {
			count++
			t.Log("email=", string(view.Email()))
			return nil
		})
		require.Equal(t, 4, count)

		err = h.GetByUniqueIndex(tx, emailIDx, []byte(original.Email), func(view *ds.Human) {
		})
		require.ErrorIs(t, err, badger.ErrKeyNotFound)

		return nil
	})
	require.NoError(t, err)

}
