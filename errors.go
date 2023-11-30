package badger_store

import "errors"

var (
	ErrStopIteration             = errors.New("stop iteration")
	ErrUniqueConstraintViolation = errors.New("unique constraint violation")
	ErrEmptyIndexValue           = errors.New("index value can't be empty")
	ErrEmptyID                   = errors.New("id can't be empty")
)
