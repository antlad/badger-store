// Command migration is a tour of index versioning: go run ./examples/migration
//
// It opens one database four times, each time declaring a slightly different
// schema, the way successive deploys of a service would. Records are written
// once, in the first generation, and never touched again - everything that
// happens to the indexes afterwards is done by Init.
package main

import (
	"fmt"
	"log"
	"os"
	"sort"
	"strings"

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

// schema is one generation of the table definition - what a given deploy of the
// service declares. Init's whole job is to reconcile the previous generation
// stored on disk with this one.
type schema struct {
	version       int  // table version
	withEmail     bool // is the unique e-mail index declared at all?
	withStatus    bool // is the match status index declared at all?
	statusVersion int  // bump to force the status index to be rebuilt
	upperStatus   bool // index status upper-cased instead of as written
}

type humanTable struct {
	schema
	builder *flatbuffers.Builder
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
	d := bstore.IndexesDesc{}
	if t.withEmail {
		d[emailIdx] = bstore.IndexDesc{IdxType: bstore.Unique, Version: 1}
	}
	if t.withStatus {
		d[statusIdx] = bstore.IndexDesc{IdxType: bstore.Match, Version: t.statusVersion}
	}
	return d
}

func (t *humanTable) StoreMeta() bstore.ItemMeta[test.Human] { return storeMeta{t.schema} }
func (t *humanTable) ViewMeta() bstore.ItemMeta[ds.Human]    { return viewMeta{t.schema} }

// The two ItemMeta implementations must agree: one reads index values from a
// record being written, the other from bytes already on disk. A rebuild goes
// through the view side, so if they ever drift the index quietly goes wrong.
type storeMeta struct{ schema }

func (storeMeta) ID(h *test.Human) bstore.ItemID { return h.Id[:] }

func (m storeMeta) IndexValue(h *test.Human, name string) []byte {
	switch name {
	case emailIdx:
		return []byte(h.Email)
	case statusIdx:
		return []byte(m.indexedStatus(h.Status))
	}
	return nil
}

type viewMeta struct{ schema }

func (viewMeta) ID(h *ds.Human) bstore.ItemID { return h.IdBytes() }

func (m viewMeta) IndexValue(h *ds.Human, name string) []byte {
	switch name {
	case emailIdx:
		return h.Email()
	case statusIdx:
		return []byte(m.indexedStatus(string(h.Status())))
	}
	return nil
}

func (s schema) indexedStatus(status string) string {
	if s.upperStatus {
		return strings.ToUpper(status)
	}
	return status
}

// rows are written once, by the first generation. Statuses are equal length on
// purpose: match-index values must not be prefixes of one another.
var rows = []test.Human{
	{Id: uuid.MustParse("11111111-1111-1111-1111-111111111111"), Age: 36, Name: "Alice", Email: "alice@example.com", Status: "active"},
	{Id: uuid.MustParse("22222222-2222-2222-2222-222222222222"), Age: 41, Name: "Bob", Email: "bob@example.com", Status: "paused"},
	{Id: uuid.MustParse("33333333-3333-3333-3333-333333333333"), Age: 29, Name: "Carol", Email: "carol@example.com", Status: "active"},
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	dir, err := os.MkdirTemp("", "badger-store-migration")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)

	// Generation 1: only a match index on status. There is no way to look a
	// record up by e-mail yet.
	err = deploy(dir, schema{version: 1, withStatus: true, statusVersion: 1},
		func(store *bstore.BadgerStore, h *handler) error {
			fmt.Println("writing the records (this is the only generation that writes)")
			if err := store.Update(func(tx bstore.Transaction) error {
				for i := range rows {
					if err := h.PutItem(tx, &rows[i]); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				return err
			}
			return report(store, h, "active", "")
		})
	if err != nil {
		return err
	}

	// Generation 2: add a unique index on e-mail. Init scans the table and
	// builds it over records that were written before it existed.
	err = deploy(dir, schema{version: 2, withStatus: true, statusVersion: 1, withEmail: true},
		func(store *bstore.BadgerStore, h *handler) error {
			return report(store, h, "active", "carol@example.com")
		})
	if err != nil {
		return err
	}

	// Generation 3: the status index now stores upper case. The values on disk
	// are wrong for the new rule, so the index version is bumped and Init drops
	// and rebuilds it. Note the lookup key changes with it.
	err = deploy(dir, schema{version: 3, withStatus: true, statusVersion: 2, upperStatus: true, withEmail: true},
		func(store *bstore.BadgerStore, h *handler) error {
			if err := report(store, h, "active", "carol@example.com"); err != nil {
				return err
			}
			return report(store, h, "ACTIVE", "")
		})
	if err != nil {
		return err
	}

	// Generation 4: drop the status index. Its keys go away; the records do not.
	return deploy(dir, schema{version: 4, withEmail: true},
		func(store *bstore.BadgerStore, h *handler) error {
			return report(store, h, "ACTIVE", "carol@example.com")
		})
}

type handler = bstore.Handler[ds.Human, test.Human]

// deploy opens the database, runs Init for the given schema, then hands the
// store to fn - the shape of a service starting up.
func deploy(dir string, s schema, fn func(*bstore.BadgerStore, *handler) error) error {
	fmt.Printf("\n=== deploy: table v%d, indexes %v ===\n", s.version,
		indexNames(&humanTable{schema: s}))

	db, err := badger.Open(badger.DefaultOptions(dir).WithLogger(nil))
	if err != nil {
		return err
	}
	store := bstore.NewBadgerStore(db)
	defer store.Close()

	h := bstore.NewHandler[ds.Human, test.Human](db, &humanTable{
		schema:  s,
		builder: flatbuffers.NewBuilder(1024),
	})

	// One transaction: Init records the new schema only if it commits, and a
	// migration that fails is redone by the next start.
	if err := store.Update(h.Init); err != nil {
		return fmt.Errorf("init: %w", err)
	}

	meta, err := h.LoadTableMeta()
	if err != nil {
		return err
	}
	fmt.Printf("stored meta: version=%d indexes=%v\n", meta.Version, indexVersions(meta.Indexes))

	return fn(store, h)
}

// report shows what each index can answer after the migration.
func report(store *bstore.BadgerStore, h *handler, status, email string) error {
	return store.View(func(tx bstore.Transaction) error {
		var names []string
		err := h.IterateByMatchIndex(tx, statusIdx, []byte(status), func(v *ds.Human) error {
			names = append(names, string(v.Name()))
			return nil
		})
		if err != nil {
			return err
		}
		fmt.Printf("  match status=%-6q -> %v\n", status, names)

		if email == "" {
			return nil
		}
		err = h.GetByUniqueIndex(tx, emailIdx, []byte(email), func(v *ds.Human) {
			fmt.Printf("  unique email=%q -> %s (written before this index existed)\n",
				email, v.Name())
		})
		if bstore.IsNotFound(err) {
			fmt.Printf("  unique email=%q -> not found\n", email)
			return nil
		}
		return err
	})
}

func indexNames(t *humanTable) []string {
	var names []string
	for name := range t.Indexes() {
		names = append(names, name)
	}
	if len(names) == 0 {
		return []string{"(none)"}
	}
	sort.Strings(names)
	return names
}

func indexVersions(d bstore.IndexesDesc) map[string]int {
	out := map[string]int{}
	for name, desc := range d {
		out[name] = desc.Version
	}
	return out
}
