# Concepts

How records are described and how they are laid out on disk.

## The Store / View split

Every record type is described by **two** Go types:

| Role      | Example        | Used for                                              |
|-----------|----------------|-------------------------------------------------------|
| **Store** | `test.Human`   | writing — a plain struct you serialize into bytes     |
| **View**  | `ds.Human`     | reading — a zero-copy accessor over the stored bytes  |

With FlatBuffers the View is the generated accessor and the Store is a convenience struct you fill in and pack. The two types are threaded through the generic `Handler[View, Store]`.

## Meta — you describe the record, the library owns the storage

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

`ItemMeta` exists for both sides because the store must be able to recompute a record's index entries from *stored bytes* — during updates, deletes and migrations — without decoding into a Go struct.

The two implementations must agree. Writes go through the store side and rebuilds through the view side, so if they drift, the index goes quietly wrong.

## Index types

```go
const (
	Unique IndexType = iota // at most one record per value; enforced on write
	Match                   // many records per value; iterate all matches
)
```

- `Unique` — `PutItem` fails with `ErrUniqueConstraintViolation` if another record already holds the value. Look up with `GetByUniqueIndex`.
- `Match` — iterate every record with a given value via `IterateByMatchIndex`, or bulk-delete with `DeleteByMatchingIndex`.

An empty index value (`len == 0`) is skipped — the record simply is not present in that index.

## Key layout

All keys live under the table name and are discriminated by a single byte:

| Kind         | Key shape                                      | Value        |
|--------------|------------------------------------------------|--------------|
| Table meta   | `<table>` `t` `meta`                           | JSON meta    |
| Record       | `<table>` `v` `<id>`                           | record bytes |
| Unique index | `<table>` `i` `<index>` `u` `<value>`          | record id    |
| Match index  | `<table>` `i` `<index>` `m` `<value>` `<id>`   | record id    |

Components are concatenated without length delimiters, which is what the constraints below are about.

## Design constraints

- **Match-index values should be equal length.** A match key is `…m<value><id>` and a lookup scans
  the prefix `…m<value>`. Because the id is appended, it is not enough for the *values* to be
  prefix-free: with statuses `"ok"` and `"okX"`, a query for `"okX"` also matches the row whose value
  is `"ok"` and whose id happens to begin with `X`. Equal-length values rule that out entirely.
  *(Unique-index values are looked up with an exact `Get`, not a prefix scan, so they may be any
  length — e-mails, names, etc.)*

  `IterateByMatchIndex` and `DeleteByMatchingIndex` do skip such rows rather than return them, at a
  cost of one integer comparison each. Treat that as a backstop, not a licence: variable-length match
  values are not supported.
- **Table names and index names must not be a prefix of another of the same kind** (`"user"` vs
  `"users"`, index `"a"` vs `"au"`), because index drop/rebuild and `DeleteTable` use `DropPrefix`.
  These are chosen at config time, so just keep them distinct.
- **Ids should be equal length too** (a UUID, say). They are the trailing component of every match
  key and the payload of every index entry.

---

Next: [Usage](usage.md) · [API reference](api.md) · [Migrations](migrations.md)
