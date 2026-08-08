# Usage

## The flow

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
	return handler.GetByUniqueIndex(tx, "email", []byte("a@b.com"), func(v *ds.Human) error {
		fmt.Println(string(v.Name()))
		return nil
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

Both examples are runnable end to end:

```sh
go run ./examples/basic      # write, read by id / unique index / match index, update, delete
go run ./examples/migration  # add, re-version and drop indexes across four deploys
```

## Defining the record types (FlatBuffers)

The examples reuse the module's `test` types. In your own project you write a schema and generate the accessor:

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

## Correct usage

- **Every operation runs inside a transaction.** Call the handler through `store.View` (read-only) or `store.Update` (read-write) and pass the given `Transaction` straight to it. A read-write transaction commits atomically, so a `PutItem` and the index updates it triggers either all land or none do.
- **A `Handler`/`Meta` holds serialization state, so use one transaction at a time per handler.** The `Meta`'s FlatBuffers builder is reused across calls, which is exactly right inside a single transaction. Badger does allow *concurrent* transactions, though — if you run write transactions in parallel, give each goroutine its own `Handler` (and builder), or serialize them.
- **`Serialize` must return a slice the store owns.** Reusing a shared builder buffer without copying will corrupt data. The examples copy; do the same.
- **View bytes are only valid inside the callback.** A `*View` (and any `[]byte` it returns, including the ID) points into Badger's transaction buffer. Copy anything you need to keep past the callback.
- **Badger transaction size limits apply to your own writes.** One transaction holds at most `db.MaxBatchCount()` entries, and a `PutItem` costs one entry per index plus one for the record. A large import, `DeleteItems` over a long list, or `DeleteByMatchingIndex` over many matches can hit `badger.ErrTxnTooBig` — commit in chunks. (`Init` is not subject to this.)
- **`Init` belongs at startup**, before other goroutines are writing — it blocks writes database-wide while it drops index prefixes.

## Chunking a large delete

```go
const chunk = 5000
for i := 0; i < len(ids); i += chunk {
	end := min(i+chunk, len(ids))
	if err := store.Update(func(tx badger_store.Transaction) error {
		return handler.DeleteItems(tx, ids[i:end])
	}); err != nil {
		return err
	}
}
```

---

Next: [Concepts](concepts.md) · [API reference](api.md) · [Migrations](migrations.md)
