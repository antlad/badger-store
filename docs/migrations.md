# Index versioning and migration

`Init` compares the indexes you declare against the `TableMeta` stored on disk:

- **Index removed** from `Indexes()` → its keys are dropped.
- **Index type or version changed** → the index is dropped and rebuilt by scanning every record.
- **New index** → built by scanning every record.
- **Unchanged** → left as-is.

To force a rebuild — say you changed how a value is derived before indexing it — bump that index's `Version` and redeploy.

[`examples/migration`](../examples/migration/main.go) walks through adding, re-versioning and dropping an index across four deploys, writing records only in the first:

```sh
go run ./examples/migration
```

## Running it

Call `Init` once per handler at startup, inside a transaction:

```go
if err := store.Update(handler.Init); err != nil {
	return err
}
```

**Before serving traffic.** Dropping an index prefix blocks every write to the database for its duration, so concurrent commits elsewhere can fail with `badger.ErrBlockedWrites`. On a read-only database `Init` returns an error.

## If a migration fails

Table size is not a limit — a rebuild is not bounded by Badger's transaction size, however many records and indexes it has to write.

A migration that fails, or whose surrounding transaction rolls back, records nothing: the stored schema stays at the old version and the indexes it was rebuilding are marked unfinished. **Restart and `Init` redoes them.** That holds even if you roll back to the previous binary, so a failed deploy cannot leave an index half-built and silently empty. Nothing extra to run by hand.

---

Next: [Concepts](concepts.md) · [Usage](usage.md) · [API reference](api.md)
