# API reference

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
- `GetByUniqueIndex(tx, indexName string, indexValue []byte, cb func(*View) error) error`
- `Iterate(tx, cb func(*View) error) error` — every record in the table.
- `IterateByMatchIndex(tx, indexName string, indexValue []byte, cb func(*View) error) error`

Errors and helpers:

- `ErrUniqueConstraintViolation`, `ErrEmptyID`, `ErrEmptyIndexValue`, `ErrStopIteration`
- `IsNotFound(err) bool` — record/key not found. Use this rather than comparing against
  `badger.ErrKeyNotFound` yourself.
- `IsStopIteration(err) bool` — matches `ErrStopIteration`, including wrapped.

## Stopping a scan early

Return `ErrStopIteration` from an `Iterate` or `IterateByMatchIndex` callback and the scan stops;
the call itself returns `nil`, not the sentinel. This is also how you cancel a scan on a context,
since no operation takes a `context.Context`:

```go
err := handler.Iterate(tx, func(v *ds.Human) error {
	select {
	case <-ctx.Done():
		return badger_store.ErrStopIteration
	default:
	}
	...
	return nil
})
```

Wrapping works — `fmt.Errorf("budget exhausted: %w", badger_store.ErrStopIteration)` still stops the
scan, so you can carry your own reason. Any other error is returned to the caller unchanged. Because
a stopped scan and a finished one both return `nil`, record in the closure whether you stopped.

The one long-running operation this does not cover is the index rebuild inside `Init`, which scans
the table through a callback the library owns.

---

Next: [Concepts](concepts.md) · [Usage](usage.md) · [Migrations](migrations.md)
