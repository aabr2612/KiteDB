package graphdb

import (
	"github.com/sirupsen/logrus"
)

// WALManager is a minimal stub for write-ahead logging
type WALManager struct {
	operations []TransactionOperation // In-memory operation log
}

// NewWALManager initializes a new WALManager
func NewWALManager() *WALManager {
	log := logrus.WithField("component", "WALManager")
	log.Info("Initializing WALManager (in-memory stub)")
	return &WALManager{
		operations: []TransactionOperation{},
	}
}

// LogOperation logs a transaction operation
func (wm *WALManager) LogOperation(op TransactionOperation) error {
	log := logrus.WithFields(logrus.Fields{
		"component": "WALManager",
		"op_type":   op.Type,
		"node_id":   op.NodeID,
		"edge_id":   op.EdgeID,
	})
	wm.operations = append(wm.operations, op)
	log.Debug("Operation logged in memory")
	return nil
}

// Commit clears logged operations for a transaction
func (wm *WALManager) Commit(txnID int64) error {
	log := logrus.WithFields(logrus.Fields{
		"component": "WALManager",
		"txn_id":    txnID,
	})
	wm.operations = []TransactionOperation{}
	log.Debug("Transaction operations cleared")
	return nil
}

// Close performs cleanup
func (wm *WALManager) Close() error {
	log := logrus.WithField("component", "WALManager")
	wm.operations = []TransactionOperation{}
	log.Info("WALManager closed")
	return nil
}
