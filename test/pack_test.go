package test

import (
	"github.com/antlad/badger-store/test/ds"
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestPack(t *testing.T) {
	h1 := Human{
		Id:     uuid.New(),
		Age:    99,
		Name:   "hello",
		Email:  "user@mail.com",
		Status: "ok",
	}
	builder := flatbuffers.NewBuilder(0xFFF)
	data := Pack(builder, &h1)

	view := ds.GetRootAsHuman(data, 0)
	require.Equal(t, h1.Status, string(view.Status()))
	require.Equal(t, h1.Email, string(view.Email()))
	require.Equal(t, h1.Name, string(view.Name()))
	require.Equal(t, h1.Age, int(view.Age()))
	require.Equal(t, h1.Id[:], view.IdBytes())

}
