package graphdb

import (
	"fmt"

	"github.com/sirupsen/logrus"
)

// OperationType defines types of operations
type OperationType int

const (
	OpAddNode OperationType = iota
	OpAddEdge
	OpUpdateNode
	OpUpdateEdge
	OpDeleteNode
	OpDeleteEdge
)

// TransactionOperation represents a transaction operation
type TransactionOperation struct {
	Type       OperationType
	NodeID     int64
	EdgeID     int64
	Properties []Property
}

// TransactionManager manages transactions
type TransactionManager struct {
	nextTxnID  int64
	operations map[int64][]TransactionOperation // txnID -> operations
	wal        *WALManager
}

// NewTransactionManager initializes a new TransactionManager
func NewTransactionManager(wal *WALManager) *TransactionManager {
	log := logrus.WithField("component", "TransactionManager")
	log.Info("Initializing TransactionManager")
	return &TransactionManager{
		nextTxnID:  1,
		operations: make(map[int64][]TransactionOperation),
		wal:        wal,
	}
}

// BeginTransaction starts a new transaction
func (tm *TransactionManager) BeginTransaction() int64 {
	txnID := tm.nextTxnID
	tm.nextTxnID++
	tm.operations[txnID] = []TransactionOperation{}
	log := logrus.WithField("txn_id", txnID)
	log.Info("Transaction started")
	return txnID
}

// RecordOperation logs an operation for a transaction
func (tm *TransactionManager) RecordOperation(txnID int64, op TransactionOperation) error {
	log := logrus.WithFields(logrus.Fields{
		"txn_id":  txnID,
		"op_type": op.Type,
	})
	if _, exists := tm.operations[txnID]; !exists {
		log.Error("Transaction not found")
		return fmt.Errorf("transaction %d not found", txnID)
	}
	tm.operations[txnID] = append(tm.operations[txnID], op)
	if err := tm.wal.LogOperation(op); err != nil {
		log.WithError(err).Error("Failed to log operation to WAL")
		return fmt.Errorf("failed to log operation to WAL: %v", err)
	}
	log.Debug("Operation recorded")
	return nil
}

// CommitTransaction commits a transaction
func (tm *TransactionManager) CommitTransaction(txnID int64) error {
	log := logrus.WithField("txn_id", txnID)
	if _, exists := tm.operations[txnID]; !exists {
		log.Error("Transaction not found for commit")
		return fmt.Errorf("transaction %d not found", txnID)
	}
	if err := tm.wal.Commit(txnID); err != nil {
		log.WithError(err).Error("Failed to commit WAL")
		return fmt.Errorf("failed to commit WAL: %v", err)
	}
	delete(tm.operations, txnID)
	log.Info("Transaction committed")
	return nil
}
