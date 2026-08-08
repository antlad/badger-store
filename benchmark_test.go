package badger_store

import (
	"encoding/json"
	"testing"

	"github.com/antlad/badger-store/test"
	"github.com/antlad/badger-store/test/ds"
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/google/uuid"
)

// Zero-copy FlatBuffers view vs json.Unmarshal on the read path.
var sink int

func benchHuman() test.Human {
	return test.Human{
		Id:     uuid.New(),
		Age:    42,
		Name:   "Ada Lovelace",
		Email:  "ada@analytical.engine",
		Status: "active",
	}
}

func BenchmarkReadFlatbuffers(b *testing.B) {
	h := benchHuman()
	data := test.Pack(flatbuffers.NewBuilder(256), &h)
	buf := make([]byte, len(data))
	copy(buf, data)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v := ds.GetRootAsHuman(buf, 0)
		sink += len(v.Name()) + len(v.Email()) + len(v.Status()) + int(v.Age()) + len(v.IdBytes())
	}
}

func BenchmarkReadJSON(b *testing.B) {
	h := benchHuman()
	buf, err := json.Marshal(h)
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var out test.Human
		if err := json.Unmarshal(buf, &out); err != nil {
			b.Fatal(err)
		}
		sink += len(out.Name) + len(out.Email) + len(out.Status) + out.Age + len(out.Id)
	}
}
