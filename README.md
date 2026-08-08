# badger-store

[![CI](https://github.com/antlad/badger-store/actions/workflows/ci.yml/badge.svg)](https://github.com/antlad/badger-store/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/antlad/badger-store.svg)](https://pkg.go.dev/github.com/antlad/badger-store)

A tiny, generic **document store on top of [BadgerDB](https://github.com/dgraph-io/badger)** for Go.

Badger is a fast embedded KV store, but it only knows keys and byte blobs. Anything above that — "give me the user with this e-mail", "give me every active session", "don't allow two rows with the same slug" — you build yourself. badger-store owns the key layout, the indexes and the transactions:

- **Secondary indexes.** `Unique` and `Match`, updated in the same transaction as the record.
- **Versioned indexes with online migration.** Bump an index's version and the store rebuilds it on the next `Init`.
- **Reads hand you a view over the stored bytes**, never a decoded struct.

It is a thin building block, not an ORM. You bring the record types and the (de)serialization.

## Reads return a view

Every record type is two Go types:

```go
handler := badger_store.NewHandler[ds.Human, test.Human](db, myMeta)
//                                 └ View    └ Store
```

- the **Store** type is an ordinary struct — you fill it in and hand it to `PutItem`
- the **View** type is what read callbacks receive, built from the bytes Badger holds

You supply both through one `Meta`: `Serialize` turns a Store into bytes, `TakeView` turns bytes back into a View.

**The View, and every `[]byte` reachable from it, is only valid inside the callback that received it.** Copy anything you keep.

## Serialization is yours — and FlatBuffers makes it free

The library never looks inside a record. `Serialize` and `TakeView` are your code, so **any format works** — JSON, protobuf, gob, hand-rolled bytes. Pick whatever you already use.

But the pairing worth knowing about is Badger + [FlatBuffers](https://flatbuffers.dev/). Badger hands the value bytes straight to the callback, and FlatBuffers reads fields in place out of those bytes, so `TakeView` is a pointer cast and a read allocates nothing at all:

```
BenchmarkReadFlatbuffers-10     32 ns/op      0 B/op    0 allocs/op
BenchmarkReadJSON-10          1354 ns/op    344 B/op    8 allocs/op
```

Reading five fields from one record, no database involved — the decode step on its own (`go test -bench Read`). A format that parses in place keeps that cost; one that unmarshals into a struct pays it on every read, and the ratio is what you see above.

## Install

```sh
go get github.com/antlad/badger-store
```

Requires Go 1.25+.

## Quick start

```go
store := badger_store.NewBadgerStore(db)
defer store.Close()

// ds.Human is the generated FlatBuffers view, test.Human the plain struct.
handler := badger_store.NewHandler[ds.Human, test.Human](db, myMeta)

// Create the table and its indexes, and run any migration. Once, at startup.
if err := store.Update(handler.Init); err != nil {
	return err
}

// Write. Record and indexes commit together.
err := store.Update(func(tx badger_store.Transaction) error {
	return handler.PutItem(tx, &record)
})

// Read by a unique index. v points at the stored bytes; don't keep it.
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
