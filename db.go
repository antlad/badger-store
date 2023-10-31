package badger_store

type Transaction interface{}

type DB interface {
	View(func(txn Transaction) error) error
	Update(func(txn Transaction) error) error
	Close() error
}
