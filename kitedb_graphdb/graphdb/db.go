package graphdb

// GraphDB is the main database interface
type GraphDB struct {
	storage    *StorageManager
	bufferPool *BufferPool
	indexMgr   *IndexManager
	recordMgr  *RecordManager
	graph      *GraphManager
	txnMgr     *TransactionManager
	wal        *WALManager
	executor   *Executor
}

// NewGraphDB initializes a new GraphDB instance
func NewGraphDB(filename string, pageSize, bufferCapacity int) (*GraphDB, error) {
	storage, err := NewStorageManager(filename, pageSize)
	if err != nil {
		return nil, err
	}

	bufferPool := NewBufferPool(storage, bufferCapacity)
	indexMgr := NewIndexManager()
	recordMgr := NewRecordManager(bufferPool, pageSize)
	wal := NewWALManager()
	graph := NewGraphManager(bufferPool, indexMgr, recordMgr)
	txnMgr := NewTransactionManager(wal)
	executor := NewExecutor(graph, txnMgr)

	return &GraphDB{
		storage:    storage,
		bufferPool: bufferPool,
		indexMgr:   indexMgr,
		recordMgr:  recordMgr,
		wal:        wal,
		graph:      graph,
		txnMgr:     txnMgr,
		executor:   executor,
	}, nil
}

// ExecuteQuery processes a Cypher query
func (db *GraphDB) ExecuteQuery(query string) ([]map[string]interface{}, error) {
	tokenizer := NewTokenizer(query)
	tokens := tokenizer.Tokenize()
	parser := NewParser(tokens)
	ast, err := parser.Parse()
	if err != nil {
		return nil, err
	}

	txnID := db.txnMgr.BeginTransaction()
	results, err := db.executor.Execute(txnID, ast)
	if err != nil {
		return nil, err
	}

	if err := db.txnMgr.CommitTransaction(txnID); err != nil {
		return nil, err
	}

	return results, nil
}

// Close shuts down the database
func (db *GraphDB) Close() error {
	if err := db.wal.Close(); err != nil {
		return err
	}
	if err := db.bufferPool.Close(); err != nil {
		return err
	}
	if err := db.storage.Close(); err != nil {
		return err
	}
	return nil
}

// GetNodeLabels returns all node labels (for debugging)
func (db *GraphDB) GetNodeLabels() []string {
	labels := make([]string, 0, len(db.graph.nodeLabelMap))
	for label := range db.graph.nodeLabelMap {
		labels = append(labels, label)
	}
	return labels
}
