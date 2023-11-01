package badger_store

import (
	"fmt"
	"github.com/antlad/badger-store.git/test"
	"github.com/antlad/badger-store.git/test/ds"
	flatbuffers "github.com/google/flatbuffers/go"
	"testing"

	"github.com/stretchr/testify/require"

	uuid "github.com/google/uuid"
)

const (
	emailIDx  = "email"
	nameIDx   = "name"
	statusIDx = "status"
)

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
	view := ds.GetRootAsHuman(data, 0)
	return view, nil
}

func (m *HumanTable) Serialize(h *test.Human) ([]byte, error) {
	return test.Pack(m.builder, h), nil
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
	d, err := NewDB("/tmp/store_test")
	require.NoError(t, err)
	err = d.Reset()
	require.NoError(t, err)

	defer d.Close()
	h := NewHandler[ds.Human, test.Human](d.b, &HumanTable{builder: flatbuffers.NewBuilder(0xFFF)})
	id := uuid.New()
	h1 := createHuman(1)
	err = d.Update(func(tx Transaction) error {
		return h.PutItem(tx, &h1)
	})
	require.NoError(t, err)

	found := false
	err = d.View(func(tx Transaction) error {
		return h.ViewItemByID(tx, id[:], func(view *ds.Human) {
			found = true
			checkViewMatch(t, h1, view)
		})
	})
	require.NoError(t, err)
	require.True(t, found)

}

func TestMatch(t *testing.T) {
	d, err := NewDB("/tmp/store_test")
	require.NoError(t, err)
	err = d.Reset()
	require.NoError(t, err)
	h := NewHandler[ds.Human, test.Human](d.b, &HumanTable{builder: flatbuffers.NewBuilder(0xFFF)})

	items := make([]test.Human, 0, 100)

	err = d.Update(func(tx Transaction) error {
		for i := 0; i < 100; i++ {
			hm := createHuman(i)
			items = append(items, hm)
			err = h.PutItem(tx, &hm)
			require.NoError(t, err)
		}
		return nil
	})

	h1 := items[42]

	err = d.View(func(tx Transaction) error {
		found := false
		err = h.ViewItemByID(tx, h1.Id[:], func(view *ds.Human) {
			checkViewMatch(t, h1, view)
			found = true
		})
		require.NoError(t, err)
		require.True(t, found)
		return nil
	})
	require.NoError(t, err)
}
