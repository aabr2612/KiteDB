package graphdb

// WALManager is a minimal stub for write-ahead logging
type WALManager struct {
	operations []TransactionOperation // In-memory operation log
}

// NewWALManager initializes a new WALManager
func NewWALManager() *WALManager {
	return &WALManager{
		operations: []TransactionOperation{},
	}
}

// LogOperation logs a transaction operation
func (wm *WALManager) LogOperation(op TransactionOperation) error {
	wm.operations = append(wm.operations, op)
	return nil
}

// Commit clears logged operations for a transaction
func (wm *WALManager) Commit(txnID int64) error {
	wm.operations = []TransactionOperation{}
	return nil
}

// Close performs cleanup
func (wm *WALManager) Close() error {
	wm.operations = []TransactionOperation{}
	return nil
}
