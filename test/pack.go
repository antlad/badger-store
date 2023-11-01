package test

import (
	"github.com/antlad/badger-store.git/test/ds"
	flatbuffers "github.com/google/flatbuffers/go"
)

func Pack(b *flatbuffers.Builder, h *Human) []byte {
	b.Reset()

	hName := b.CreateString(h.Name)
	hStatus := b.CreateString(h.Status)
	hEmail := b.CreateString(h.Email)

	id := b.CreateByteVector(h.Id[:])

	ds.HumanStart(b)
	ds.HumanAddEmail(b, hEmail)
	ds.HumanAddName(b, hName)
	ds.HumanAddStatus(b, hStatus)
	ds.HumanAddAge(b, int32(h.Age))
	ds.HumanAddId(b, id)
	end := ds.HumanEnd(b)
	b.Finish(end)
	return b.FinishedBytes()
}
