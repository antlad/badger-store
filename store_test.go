package badger_store

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	uuid "github.com/google/uuid"
)

type Human struct {
	Id   uuid.UUID
	Age  int
	Name string
}

type HumanMeta struct {
}

func (m HumanMeta) TableName() string {
	return "human"
}

func (m HumanMeta) Parse(val []byte) (Human, error) {
	var h Human
	err := json.Unmarshal(val, &h)
	return h, err
}

func (m HumanMeta) Serialize(human Human) ([]byte, error) {
	return json.Marshal(human)
}

func (h Human) ID() ItemID {
	return h.Id[:]
}

func (h Human) IndexValue(name string) []byte {
	return nil
}

func TestBase(t *testing.T) {
	d, err := NewDB("/tmp/store_test")
	require.NoError(t, err)
	defer d.Close()
	h := NewHandler[Human, Human](d.b, &HumanMeta{})
	id := uuid.New()
	h1 := Human{
		Id:   id,
		Age:  99,
		Name: "hello",
	}
	err = d.Update(func(tx Transaction) error {
		return h.PutItem(tx, h1)
	})
	require.NoError(t, err)
	var h2 Human
	err = d.View(func(tx Transaction) error {
		return h.ViewItemByID(tx, id[:], func(view Human) {
			h2 = view
		})
	})
	require.NoError(t, err)

	require.Equal(t, h1, h2)
}
