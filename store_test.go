package badger_store

import (
	"github.com/antlad/badger-store.git/test"
	"github.com/antlad/badger-store.git/test/ds"
	flatbuffers "github.com/google/flatbuffers/go"
	"testing"

	"github.com/stretchr/testify/require"

	uuid "github.com/google/uuid"
)

type HumanStoreMeta struct {
}

func (m *HumanStoreMeta) ID(h *test.Human) ItemID {
	return h.Id[:]
}

func (m *HumanStoreMeta) IndexValue(h *test.Human, indexName string) []byte {
	switch indexName {
	case "email":
		return []byte(h.Email)
	case "status":
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
	case "email":
		return h.Email()
	case "status":
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
		"email":  Unique,
		"status": Match,
	}
}

func (m *HumanTable) StoreMeta() ItemMeta[test.Human] {
	return &HumanStoreMeta{}
}

func (m *HumanTable) ViewMeta() ItemMeta[ds.Human] {
	return &HumanViewMeta{}
}

func TestBase(t *testing.T) {
	d, err := NewDB("/tmp/store_test")
	require.NoError(t, err)
	defer d.Close()
	h := NewHandler[ds.Human, test.Human](d.b, &HumanTable{builder: flatbuffers.NewBuilder(0xFFF)})
	id := uuid.New()
	h1 := test.Human{
		Id:     id,
		Age:    99,
		Name:   "hello",
		Email:  "user@mail.com",
		Status: "ok",
	}
	err = d.Update(func(tx Transaction) error {
		return h.PutItem(tx, &h1)
	})
	require.NoError(t, err)

	found := false
	err = d.View(func(tx Transaction) error {
		return h.ViewItemByID(tx, id[:], func(view *ds.Human) {
			found = true
			require.Equal(t, h1.Email, string(view.Email()))
			require.Equal(t, h1.Id[:], view.IdBytes())
			require.Equal(t, h1.Status, string(view.Status()))
			require.Equal(t, h1.Name, string(view.Name()))
			require.Equal(t, h1.Age, int(view.Age()))
		})
	})
	require.NoError(t, err)
	require.True(t, found)

}
