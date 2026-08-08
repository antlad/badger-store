# badger-store

A tiny, generic **document-store layer on top of [BadgerDB](https://github.com/dgraph-io/badger)** for Go.

Badger is a fast embedded KV store, but it only knows keys and byte blobs. Anything above that — "give me the user with this e-mail", "give me every active session", "don't allow two rows with the same slug" — you build yourself, and you usually pay a full decode on every read. badger-store removes both costs:

- **Zero-copy reads.** Records are stored as [FlatBuffers](https://flatbuffers.dev/) and read back as a *view* directly over the bytes Badger already holds — no `Unmarshal`, no allocations on the read path.
- **Secondary indexes.** `Unique` and `Match` indexes are maintained automatically, inside the same transaction as the write.
- **Versioned indexes with online migration.** Bump an index's version and the store rebuilds it on the next `Init`.

It is a thin, dependency-light building block — not an ORM. You bring the schema and the (de)serialization; the library owns the key layout, the indexes and the transactions.

```
BenchmarkReadFlatbuffers-10     32 ns/op      0 B/op    0 allocs/op
BenchmarkReadJSON-10          1354 ns/op    344 B/op    8 allocs/op
```

## Install

```sh
go get github.com/antlad/badger-store
```

Requires Go 1.25+.

## Quick start

```go
store := badger_store.NewBadgerStore(db)
defer store.Close()

handler := badger_store.NewHandler[ds.Human, test.Human](db, myMeta)

// Create the table and its indexes, and run any migration. Once, at startup.
if err := store.Update(handler.Init); err != nil {
	return err
}

// Write. Record and indexes commit together.
err := store.Update(func(tx badger_store.Transaction) error {
	return handler.PutItem(tx, &record)
})

// Read by a unique index, with no decode.
err = store.View(func(tx badger_store.Transaction) error {
	return handler.GetByUniqueIndex(tx, "email", []byte("a@b.com"), func(v *ds.Human) error {
		fmt.Println(string(v.Name()))
		return nil
	})
})
```

Two runnable programs:

```sh
go run ./examples/basic      # write, read by id / unique index / match index, update, delete
go run ./examples/migration  # add, re-version and drop indexes across four deploys
```

## Documentation

| | |
|---|---|
| [Concepts](docs/concepts.md) | the Store/View split, `Meta`, index types, key layout, design constraints |
| [Usage](docs/usage.md) | the full flow, defining record types, and the rules to write against |
| [API reference](docs/api.md) | every exported call, errors, stopping a scan early |
| [Migrations](docs/migrations.md) | index versioning, running `Init`, what happens when one fails |
| [Development](docs/development.md) | build, test, lint, benchmarks, regenerating FlatBuffers |

Package docs are on [pkg.go.dev](https://pkg.go.dev/github.com/antlad/badger-store).

## License

See [LICENSE](LICENSE).
