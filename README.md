# badger-store

A tiny, generic **document-store layer on top of [BadgerDB](https://github.com/dgraph-io/badger)** for Go.

It gives an embedded key-value database three things a plain KV store lacks:

1. **Zero-copy reads.** Records are stored as [FlatBuffers](https://flatbuffers.dev/) and read back as a *view* directly over the bytes Badger already holds — no `Unmarshal`, no allocations on the read path.
2. **Secondary indexes.** `Unique` and `Match` (non-unique) indexes are maintained automatically, inside the same transaction as the write, so you can look records up by fields other than the primary key.
3. **Versioned indexes with online migration.** Change an index's type or bump its version and the store rebuilds it on the next `Init`.

It is a thin, dependency-light building block — not a full ORM. You bring the schema and the (de)serialization; the library owns the key layout, the indexes and the transactions.

---

## Why it exists

Badger is a fast embedded KV store, but it only knows keys and byte blobs. Anything above that — "give me the user with this e-mail", "give me every active session", "don't allow two rows with the same slug" — you build yourself, and you usually pay a full decode on every read.

badger-store removes both costs. The read model is a FlatBuffers accessor, so touching a couple of fields never decodes the whole record:

```
BenchmarkReadFlatbuffers-10     32 ns/op      0 B/op    0 allocs/op
BenchmarkReadJSON-10          1354 ns/op    344 B/op    8 allocs/op
```

_(`go test -run '^$' -bench BenchmarkRead -benchmem ./` — Apple M-series. ~40× faster, zero allocations.)_

That matters most for hot read paths and iteration over many rows — scanning a match index and reading one field per row costs essentially nothing beyond Badger's own iteration.

---

## Installation

```sh
go get github.com/antlad/badger-store
```

Requires Go 1.25+.

---

## Core concepts

### The Store / View split

Every record type is described by **two** Go types:

| Role      | Example        | Used for                                              |
|-----------|----------------|-------------------------------------------------------|
| **Store** | `test.Human`   | writing — a plain struct you serialize into bytes     |
| **View**  | `ds.Human`     | reading — a zero-copy accessor over the stored bytes  |

With FlatBuffers the View is the generated accessor and the Store is a convenience struct you fill in and pack. The two types are threaded through the generic `Handler[View, Store]`.

### Meta — you describe the record, the library owns the storage

You implement one `Meta[View, Store]` interface:

```go
type Meta[View any, Store any] interface {
	TableName() string                       // key namespace for this record type
	Version() int                            // table/index version (drives migration)
	Indexes() IndexesDesc                    // named indexes and their types/versions
	Serialize(*Store) ([]byte, error)        // record -> owned byte slice
	TakeView([]byte) (*View, error)          // bytes -> zero-copy view
	StoreMeta() ItemMeta[Store]              // read ID + index values from a record
	ViewMeta() ItemMeta[View]                // read ID + index values from a view
}

type ItemMeta[T any] interface {
	ID(*T) ItemID                            // primary key bytes
	IndexValue(t *T, indexName string) []byte // value to index for a given index
}
```

`ItemMeta` exists for both sides because the store must be able to recompute a record's index entries from *stored bytes* (during updates, deletes and migrations) without decoding into a Go struct.

### Index types

```go
const (
	Unique IndexType = iota // at most one record per value; enforced on write
	Match                   // many records per value; iterate all matches
)
```

- `Unique` — `PutItem` fails with `ErrUniqueConstraintViolation` if another record already holds the value. Look up with `GetByUniqueIndex`.
- `Match` — iterate every record with a given value via `IterateByMatchIndex`, or bulk-delete with `DeleteByMatchingIndex`.

An empty index value (`len == 0`) is skipped — the record simply is not present in that index.

### Key layout

All keys live under the table name and are discriminated by a single byte:

| Kind         | Key shape                                          | Value        |
|--------------|----------------------------------------------------|--------------|
| Table meta   | `<table>` `t` `meta`                               | JSON meta    |
| Record       | `<table>` `v` `<id>`                               | record bytes |
| Unique index | `<table>` `i` `<index>` `u` `<value>`             | record id    |
| Match index  | `<table>` `i` `<index>` `m` `<value>` `<id>`     | record id    |

> Components are concatenated **without length delimiters** — a deliberate space/speed optimization. It assumes fixed-width (or at least prefix-free) keys and values; see [Design constraints](#design-constraints).

---

## Quick start

Two complete, runnable programs:

```sh
go run ./examples/basic      # write, read by id / unique index / match index, update, delete
go run ./examples/migration  # add, re-version and drop indexes across four deploys
```

The condensed flow:

```go
db, _ := badger.Open(badger.DefaultOptions(dir))
store := badger_store.NewBadgerStore(db)
defer store.Close()

handler := badger_store.NewHandler[ds.Human, test.Human](db, myMeta)

// Create/migrate the table and its indexes once at startup.
_ = store.Update(handler.Init)

// Write — record + indexes updated atomically.
_ = store.Update(func(tx badger_store.Transaction) error {
	return handler.PutItem(tx, &test.Human{ /* ... */ })
})

// Read by primary key — zero-copy view.
_ = store.View(func(tx badger_store.Transaction) error {
	return handler.GetByID(tx, id, func(v *ds.Human) error {
		fmt.Println(string(v.Email()))
		return nil
	})
})

// Read by unique index.
_ = store.View(func(tx badger_store.Transaction) error {
	return handler.GetByUniqueIndex(tx, "email", []byte("a@b.com"), func(v *ds.Human) {
		fmt.Println(string(v.Name()))
	})
})

// Iterate a match index.
_ = store.View(func(tx badger_store.Transaction) error {
	return handler.IterateByMatchIndex(tx, "status", []byte("active"), func(v *ds.Human) error {
		fmt.Println(string(v.Name()))
		return nil
	})
})
```

### Defining the record types (FlatBuffers)

The example reuses the module's `test` types. In your own project you write a schema and generate the accessor:

```fbs
// human.fbs
namespace ds;
table Human {
  id:[ubyte];
  age:int;
  name:string;
  email:string;
  status:string;
}
```

```sh
flatc --go --scoped-enums human.fbs
```

You are free to use any serialization (Protobuf, JSON, gob…) — just implement `Serialize`/`TakeView` accordingly. FlatBuffers is what makes reads zero-copy; other formats will decode on read.

---

## API reference

Construction:

- `NewBadgerStore(*badger.DB) *BadgerStore` — wraps a Badger DB; provides `View`, `Update`, `Reset`, `Close`.
- `NewHandler[View, Store](*badger.DB, Meta[View, Store]) *Handler` — one handler per record type.

Transactions (via `BadgerStore`):

- `View(func(tx Transaction) error) error` — read-only transaction.
- `Update(func(tx Transaction) error) error` — read-write transaction.

Schema / lifecycle (`Handler`):

- `Init(tx Transaction) error` — create metadata and indexes, run migrations. Call at startup.
- `LoadTableMeta() (TableMeta, error)`, `SaveTableMeta() error`, `DropTableMeta() error`
- `DeleteTable() error` — drop every key under the table.

Writes (`Handler`):

- `PutItem(tx Transaction, item *Store) error` — upsert; enforces unique constraints and maintains indexes.
- `DeleteItem(tx Transaction, id ItemID) error`
- `DeleteItems(tx Transaction, ids []ItemID) error`
- `DeleteByMatchingIndex(tx Transaction, indexName string, indexValue []byte) error`

Reads (`Handler`):

- `GetByID(tx, id ItemID, cb func(*View) error) error`
- `GetByUniqueIndex(tx, indexName string, indexValue []byte, cb func(*View)) error`
- `Iterate(tx, cb func(*View) error) error` — every record in the table.
- `IterateByMatchIndex(tx, indexName string, indexValue []byte, cb func(*View) error) error`

Errors and helpers:

- `ErrUniqueConstraintViolation`, `ErrEmptyID`, `ErrEmptyIndexValue`, `ErrStopIteration`
- `IsNotFound(err) bool` — record/key not found.
- `IsStopIteration(err) bool` — return `ErrStopIteration` from an iterate callback to stop early.

---

## Index versioning and migration

`Init` compares the indexes you declare against the `TableMeta` stored on disk:

- **Index removed** from `Indexes()` → its keys are dropped.
- **Index type or version changed** → the index is dropped and rebuilt by scanning every record.
- **New index** → built by scanning every record.
- **Unchanged** → left as-is.

To force a rebuild — say you changed how a value is derived before indexing it — bump that index's `Version` and redeploy. [`examples/migration`](examples/migration/main.go) walks through adding, re-versioning and dropping an index across four deploys.

**Call `Init` at startup, before serving traffic.** Dropping an index prefix blocks every write to the database for its duration, so concurrent commits elsewhere can fail with `badger.ErrBlockedWrites`. On a read-only database `Init` returns an error.

### What happens if a migration fails

Rebuilding a table of any real size writes far more index entries than one Badger transaction can hold, so the rebuild does not run inside your transaction — it streams through a `WriteBatch` that commits in chunks. Table size is therefore not a limit on migrations.

That means the rebuild is durable independently of the transaction you passed in, and so are the prefix drops that precede it. To keep that recoverable, `Init` writes a **rebuild marker** for each index before dropping it, and clears the marker and the new `TableMeta` together on *your* transaction, last. The outcomes are:

| Init | Your transaction | Result |
|------|------------------|--------|
| ok | commits | new schema recorded, markers cleared |
| ok | rolls back | schema stays at the old version, markers survive → the next `Init` rebuilds those indexes |
| fails | rolls back | same as above; nothing is recorded as migrated |

A marker forces a rebuild regardless of what the versions say, so a half-finished migration is redone even by a **rolled-back deploy of the previous binary** — which is exactly the case that would otherwise leave an index silently dropped and empty.

---

## Concurrency and correct usage

- **Every operation runs inside a transaction.** Call the handler through `store.View` (read-only) or `store.Update` (read-write) and pass the given `Transaction` straight to it. A read-write transaction commits atomically, so a `PutItem` and the index updates it triggers either all land or none do.
- **A `Handler`/`Meta` holds serialization state, so use one transaction at a time per handler.** The `Meta`'s FlatBuffers builder is reused across calls, which is exactly right inside a single transaction. Badger does allow *concurrent* transactions, though — if you run write transactions in parallel, give each goroutine its own `Handler` (and builder), or serialize them.
- **`Serialize` must return a slice the store owns.** Reusing a shared builder buffer without copying will corrupt data. The example copies; do the same.
- **View bytes are only valid inside the callback.** A `*View` (and any `[]byte` it returns, including the ID) points into Badger's transaction buffer. Copy anything you need to keep past the callback.
- **Badger transaction size limits apply to your own writes.** One transaction holds at most `db.MaxBatchCount()` entries, and a `PutItem` costs one entry per index plus one for the record. A large import, `DeleteItems` over a long list, or `DeleteByMatchingIndex` over many matches can hit `badger.ErrTxnTooBig` — commit in chunks. (`Init` is not subject to this; see [migration](#what-happens-if-a-migration-fails).)
- **`Init` belongs at startup**, before other goroutines are writing — it blocks writes database-wide while it drops index prefixes.

---

## Design constraints

Keys are built by **raw concatenation with no length delimiters** — an intentional optimization that
avoids storing and parsing length prefixes on the hot path. It relies on a convention:

- **Match-index values should be equal length.** A match key is `…m<value><id>` and a lookup scans
  the prefix `…m<value>`. Because the id is appended, it is not enough for the *values* to be
  prefix-free: with statuses `"ok"` and `"okX"`, a query for `"okX"` also matches the row whose value
  is `"ok"` and whose id happens to begin with `X`. Equal-length values rule that out entirely.
  *(Unique-index values are looked up with an exact `Get`, not a prefix scan, so they may be any
  length — e-mails, names, etc.)*

  `IterateByMatchIndex` and `DeleteByMatchingIndex` do guard against this: an entry's value is the
  full id, so `len(key) == len(prefix)+len(id)` holds exactly for an exact match, and rows that fail
  it are skipped. That is one integer comparison per row and no allocation. **Equal-length values are
  still the right design** — the guard keeps a mistake from returning wrong rows, it does not make
  variable-length match values a supported feature.
- **Table names and index names must not be a prefix of another of the same kind** (`"user"` vs
  `"users"`, index `"a"` vs `"au"`), because index drop/rebuild and `DeleteTable` use `DropPrefix`.
  These are chosen at config time, so just keep them distinct.
- **Ids should be equal length too** (a UUID, say). They are the trailing component of every match
  key and the payload of every index entry.

If you ever need genuinely variable-length match values, switch to a length-delimited key format
behind a table-version bump and migration.

---

## Development

```sh
make test       # go test with coverage
make coverage   # coverage + HTML report
make lint       # golangci-lint v2 (config in .golangci.yml)
make lint-fix   # golangci-lint with autofix
```

CI (GitHub Actions, `.github/workflows/ci.yml`) runs `go build`, `go vet`, `go test -race`, and
golangci-lint on every push and pull request.

Regenerate the test FlatBuffers types (requires `flatc`):

```sh
cd test && go generate ./...
```

---

## Known limitations

The concatenated key layout is a deliberate optimization, not a defect — see
[Design constraints](#design-constraints).

Everything else that is still open needs a **breaking API change**, so none of it has been touched:
the library is in production. Each one is written up in **[ROADMAP.md](ROADMAP.md)** — what is wrong,
why the fix breaks callers, what to do instead in the meantime, and how they would be batched into a
single `v2`. In brief:

| | Limitation | Interim |
|---|-----------|---------|
| 1 | `GetByUniqueIndex`'s callback has no `error` return, unlike every other read | capture the error in the closure |
| 2 | No `context.Context`, so no cancellation or deadlines | `ErrStopIteration` for cooperative early exit |
| 3 | `GetByID`/`GetByUniqueIndex` return `badger.ErrKeyNotFound` verbatim | always use `IsNotFound(err)` |
| 4 | `Transaction.AppendRef` protects nothing (already `Deprecated:`) | ignore it |
| 5 | Index extraction is written twice (`StoreMeta` + `ViewMeta`) and can drift | add a round-trip consistency test |
| 6 | `DeleteItems`/`DeleteByMatchingIndex` are bounded by transaction size | delete in chunks |
| 7 | Package name `badger_store` is not idiomatic | import it under an alias |

None of these need an on-disk change, so a future `v2` reads and writes the same data.

Contributions and fixes welcome.

## License

See [LICENSE](LICENSE).
