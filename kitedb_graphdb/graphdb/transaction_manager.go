package graphdb

import (
	"fmt"
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
	return txnID
}

// RecordOperation logs an operation for a transaction
func (tm *TransactionManager) RecordOperation(txnID int64, op TransactionOperation) error {
	if _, exists := tm.operations[txnID]; !exists {
		return fmt.Errorf("transaction %d not found", txnID)
	}
	tm.operations[txnID] = append(tm.operations[txnID], op)
	if err := tm.wal.LogOperation(op); err != nil {
		return fmt.Errorf("failed to log operation to WAL: %v", err)
	}
	fmt.Println("Operation recorded")
	return nil
}

// CommitTransaction commits a transaction
func (tm *TransactionManager) CommitTransaction(txnID int64) error {
	if _, exists := tm.operations[txnID]; !exists {
		return fmt.Errorf("transaction %d not found", txnID)
	}
	if err := tm.wal.Commit(txnID); err != nil {
		return fmt.Errorf("failed to commit WAL: %v", err)
	}
	delete(tm.operations, txnID)
	return nil
}
