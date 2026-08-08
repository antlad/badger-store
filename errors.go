package badger_store

import "errors"

var (
	// ErrStopIteration ends an Iterate or IterateByMatchIndex scan when returned
	// from its callback; the scan then returns nil. It may be wrapped.
	ErrStopIteration = errors.New("stop iteration")
	// ErrUniqueConstraintViolation is returned by PutItem when another record
	// already holds one of the record's unique index values. It is wrapped with
	// the index name.
	ErrUniqueConstraintViolation = errors.New("unique constraint violation")
	// ErrEmptyIndexValue is returned by GetByUniqueIndex for an empty value.
	ErrEmptyIndexValue = errors.New("index value can't be empty")
	// ErrEmptyID is returned by PutItem when a record's ItemMeta.ID is empty.
	ErrEmptyID = errors.New("id can't be empty")
)
