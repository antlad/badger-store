// Package badger_store is a small, generic document-store layer over BadgerDB.
//
// It adds zero-copy reads, secondary indexes maintained in the same transaction
// as the write, and versioned indexes that are rebuilt when their definition
// changes.
//
// # The Store / View split
//
// Every record type has two Go types: the Store type you write, an ordinary
// struct you fill in and serialize, and the View type you read, an accessor
// over the bytes Badger already holds. With FlatBuffers the View is the
// generated accessor; other formats work too, they just decode on read.
//
// Both are supplied through one [Meta] implementation.
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
// Every operation runs inside a transaction from [BadgerStore.View] (read-only)
// or [BadgerStore.Update] (read-write). A write and its index updates commit
// together or not at all.
//
// # Stopping a scan
//
// Returning [ErrStopIteration] from an [Handler.Iterate] or
// [Handler.IterateByMatchIndex] callback ends the scan and the call returns nil.
// Any other error is returned unchanged. No operation takes a context.Context,
// so this is also how a scan is canceled:
//
//	err := handler.Iterate(tx, func(v *ds.Human) error {
//		select {
//		case <-ctx.Done():
//			return badger_store.ErrStopIteration
//		default:
//		}
//		...
//		return nil
//	})
//
// # Lifetimes
//
// A *View, and every []byte reachable from it including the id, points into
// Badger's transaction buffer and is only valid inside the callback that
// received it. Copy what you need to keep. Likewise [Meta.Serialize] must
// return a slice the store may keep: Badger holds it until commit, so a reused
// builder buffer must be copied.
//
// # Key layout
//
//	<table> t meta                  -> JSON table metadata
//	<table> v <id>                  -> record bytes
//	<table> i <index> u <value>     -> record id   (unique index)
//	<table> i <index> m <value><id> -> record id   (match index)
//
// Components are concatenated without length delimiters. Table names and index
// names must not be prefixes of one another, and ids and match-index values
// should be fixed length; see docs/concepts.md.
package badger_store
