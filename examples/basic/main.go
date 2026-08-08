// Command basic is a runnable tour of badger-store: go run ./examples/basic
package main

import (
	"errors"
	"fmt"
	"log"
	"os"

	bstore "github.com/antlad/badger-store"
	"github.com/antlad/badger-store/test"
	"github.com/antlad/badger-store/test/ds"
	"github.com/dgraph-io/badger/v4"
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/google/uuid"
)

const (
	emailIdx  = "em" // unique
	statusIdx = "st" // match
)

// Store side: read the ID and index values from a record.
type humanStoreMeta struct{}

func (humanStoreMeta) ID(h *test.Human) bstore.ItemID { return h.Id[:] }

func (humanStoreMeta) IndexValue(h *test.Human, name string) []byte {
	switch name {
	case emailIdx:
		return []byte(h.Email)
	case statusIdx:
		return []byte(h.Status)
	}
	return nil
}

// View side: same, but from stored bytes (used on update/delete/migrate).
type humanViewMeta struct{}

func (humanViewMeta) ID(h *ds.Human) bstore.ItemID { return h.IdBytes() }

func (humanViewMeta) IndexValue(h *ds.Human, name string) []byte {
	switch name {
	case emailIdx:
		return h.Email()
	case statusIdx:
		return h.Status()
	}
	return nil
}

type humanTable struct {
	version int
	builder *flatbuffers.Builder // reused across calls; not safe for concurrent writes
}

func (t *humanTable) TableName() string { return "human" }
func (t *humanTable) Version() int      { return t.version }

func (t *humanTable) TakeView(data []byte) (*ds.Human, error) {
	return ds.GetRootAsHuman(data, 0), nil
}

// Serialize must return a slice the store owns (the builder is reused).
func (t *humanTable) Serialize(h *test.Human) ([]byte, error) {
	data := test.Pack(t.builder, h)
	out := make([]byte, len(data))
	copy(out, data)
	return out, nil
}

func (t *humanTable) Indexes() bstore.IndexesDesc {
	return bstore.IndexesDesc{
		emailIdx:  {IdxType: bstore.Unique, Version: t.version},
		statusIdx: {IdxType: bstore.Match, Version: t.version},
	}
}

func (t *humanTable) StoreMeta() bstore.ItemMeta[test.Human] { return humanStoreMeta{} }
func (t *humanTable) ViewMeta() bstore.ItemMeta[ds.Human]    { return humanViewMeta{} }

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	dir, err := os.MkdirTemp("", "badger-store-example")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)

	db, err := badger.Open(badger.DefaultOptions(dir).WithLogger(nil))
	if err != nil {
		return err
	}
	store := bstore.NewBadgerStore(db)
	defer store.Close()

	handler := bstore.NewHandler[ds.Human, test.Human](db, &humanTable{
		version: 1,
		builder: flatbuffers.NewBuilder(1024),
	})

	// create/migrate metadata + indexes at startup
	if err := store.Update(handler.Init); err != nil {
		return err
	}

	// fixed IDs keep the output deterministic
	alice := test.Human{Id: uuid.MustParse("11111111-1111-1111-1111-111111111111"), Age: 36, Name: "Alice", Email: "alice@example.com", Status: "active"}
	bob := test.Human{Id: uuid.MustParse("22222222-2222-2222-2222-222222222222"), Age: 41, Name: "Bob", Email: "bob@example.com", Status: "inactive"}
	carol := test.Human{Id: uuid.MustParse("33333333-3333-3333-3333-333333333333"), Age: 29, Name: "Carol", Email: "carol@example.com", Status: "active"}

	fmt.Println("== insert ==")
	if err := store.Update(func(tx bstore.Transaction) error {
		for _, h := range []test.Human{alice, bob, carol} {
			if err := handler.PutItem(tx, &h); err != nil {
				return err
			}
			fmt.Printf("put %s (%s)\n", h.Name, h.Email)
		}
		return nil
	}); err != nil {
		return err
	}

	fmt.Println("\n== get by id ==")
	if err := store.View(func(tx bstore.Transaction) error {
		return handler.GetByID(tx, bob.Id[:], func(v *ds.Human) error {
			fmt.Printf("id=%s name=%s email=%s age=%d\n",
				bob.Id, v.Name(), v.Email(), v.Age())
			return nil
		})
	}); err != nil {
		return err
	}

	fmt.Println("\n== get by unique index (email) ==")
	if err := store.View(func(tx bstore.Transaction) error {
		return handler.GetByUniqueIndex(tx, emailIdx, []byte("carol@example.com"), func(v *ds.Human) error {
			fmt.Printf("found %s\n", v.Name())
			return nil
		})
	}); err != nil {
		return err
	}

	fmt.Println("\n== iterate by match index (status=active) ==")
	if err := store.View(func(tx bstore.Transaction) error {
		return handler.IterateByMatchIndex(tx, statusIdx, []byte("active"), func(v *ds.Human) error {
			fmt.Printf("active: %s\n", v.Name())
			return nil
		})
	}); err != nil {
		return err
	}

	fmt.Println("\n== unique constraint ==")
	dup := test.Human{Id: uuid.MustParse("44444444-4444-4444-4444-444444444444"), Name: "Mallory", Email: "alice@example.com", Status: "active"}
	err = store.Update(func(tx bstore.Transaction) error { return handler.PutItem(tx, &dup) })
	fmt.Printf("insert with taken email -> unique violation: %t\n",
		errors.Is(err, bstore.ErrUniqueConstraintViolation))

	fmt.Println("\n== update (bob becomes active) ==")
	bob.Status = "active"
	if err := store.Update(func(tx bstore.Transaction) error { return handler.PutItem(tx, &bob) }); err != nil {
		return err
	}
	if err := store.View(func(tx bstore.Transaction) error {
		count := 0
		if err := handler.IterateByMatchIndex(tx, statusIdx, []byte("active"), func(v *ds.Human) error {
			count++
			return nil
		}); err != nil {
			return err
		}
		fmt.Printf("active count is now %d\n", count)
		return nil
	}); err != nil {
		return err
	}

	fmt.Println("\n== delete alice ==")
	if err := store.Update(func(tx bstore.Transaction) error {
		return handler.DeleteItem(tx, alice.Id[:])
	}); err != nil {
		return err
	}
	return store.View(func(tx bstore.Transaction) error {
		err := handler.GetByID(tx, alice.Id[:], func(v *ds.Human) error { return nil })
		fmt.Printf("alice found after delete: %t\n", !bstore.IsNotFound(err))
		return nil
	})
}
