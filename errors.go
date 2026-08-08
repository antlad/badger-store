package badger_store

import "errors"

var (
	// ErrStopIteration ends an Iterate or IterateByMatchIndex scan early when
	// returned from its callback. The iteration itself then returns nil.
	ErrStopIteration = errors.New("stop iteration")
	// ErrUniqueConstraintViolation is returned by PutItem when a different
	// record already holds one of the record's unique index values. It is
	// wrapped with the offending index name.
	ErrUniqueConstraintViolation = errors.New("unique constraint violation")
	// ErrEmptyIndexValue is returned by GetByUniqueIndex when asked to look up
	// an empty value. Records with an empty index value are not indexed.
	ErrEmptyIndexValue = errors.New("index value can't be empty")
	// ErrEmptyID is returned by PutItem when a record's ItemMeta.ID is empty.
	ErrEmptyID = errors.New("id can't be empty")
)
