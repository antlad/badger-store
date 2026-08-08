// Package badger_store is a small, generic document-store layer over BadgerDB.
//
// It adds three things a plain key-value store lacks: zero-copy reads, secondary
// indexes maintained inside the same transaction as the write, and versioned
// indexes that are rebuilt online when their definition changes.
//
// # The Store / View split
//
// Every record type is described by two Go types. The Store type is what you
// write - an ordinary struct you fill in and serialize. The View type is what
// you read - an accessor over the bytes Badger already holds, so reading a field
// costs no decode and no allocation. With FlatBuffers the View is the generated
// accessor; any other format works too, it just decodes on read.
//
// You supply both through a single [Meta] implementation, which tells the store
// how to name the table, how to serialize a record, how to take a view over
// stored bytes, and how to read the primary key and index values from either
// side. The library owns the key layout, the indexes and the transactions.
//
//	handler := badger_store.NewHandler[ds.Human, test.Human](db, myMeta)
//
//	store := badger_store.NewBadgerStore(db)
//	if err := store.Update(handler.Init); err != nil { ... }   // at startup
//
//	err := store.Update(func(tx badger_store.Transaction) error {
//		return handler.PutItem(tx, &record)
//	})
//
// # Transactions
//
// Every operation runs inside a transaction obtained from [BadgerStore.View]
// (read-only) or [BadgerStore.Update] (read-write). A write and the index
// updates it triggers commit together or not at all.
//
// # Lifetimes
//
// A *View, and every []byte reachable from it including the primary key, points
// into Badger's transaction buffer and is only valid inside the callback that
// received it. Copy anything you need to keep. Symmetrically, [Meta.Serialize]
// must return a slice the store may keep: returning a reused builder buffer
// without copying corrupts data, because Badger holds that slice until commit.
//
// # Key layout
//
// All keys are namespaced by table name and discriminated by a single byte:
//
//	<table> t meta                  -> JSON table metadata
//	<table> v <id>                  -> record bytes
//	<table> i <index> u <value>     -> record id   (unique index)
//	<table> i <index> m <value><id> -> record id   (match index)
//
// Components are concatenated without length delimiters, which is a deliberate
// space and speed optimization. It constrains what may be used as a table name,
// an index name and a match-index value; see "Design constraints" in the README.
package badger_store
