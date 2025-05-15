package graphdb

import (
	"fmt"
)

// RecordManager handles page-level record operations
type RecordManager struct {
	bufferPool *BufferPool
	pageSize   int
}

// NewRecordManager initializes a new RecordManager
func NewRecordManager(bufferPool *BufferPool, pageSize int) *RecordManager {
	return &RecordManager{
		bufferPool: bufferPool,
		pageSize:   pageSize,
	}
}

// WriteRecord serializes and writes a record (Node or Edge) to a new page
func (rm *RecordManager) WriteRecord(record interface{}) (int, error) {
	data, err := Serialize(record)
	if err != nil {
		return -1, fmt.Errorf("failed to serialize record: %v", err)
	}

	if len(data) > rm.pageSize {
		return -1, fmt.Errorf("record size %d exceeds page size %d", len(data), rm.pageSize)
	}

	pageID, err := rm.bufferPool.storage.AllocatePage()
	if err != nil {
		return -1, fmt.Errorf("failed to allocate page: %v", err)
	}

	// Pad data to page size
	paddedData := make([]byte, rm.pageSize)
	copy(paddedData, data)

	if err := rm.bufferPool.WritePage(pageID, paddedData); err != nil {
		return -1, fmt.Errorf("failed to write record to page %d: %v", pageID, err)
	}

	return pageID, nil
}

// ReadRecord reads and deserializes a record from a page
func (rm *RecordManager) ReadRecord(pageID int, record interface{}) error {
	data, err := rm.bufferPool.GetPage(pageID)
	if err != nil {
		return fmt.Errorf("failed to read page %d: %v", pageID, err)
	}

	if err := Deserialize(data, record); err != nil {
		return fmt.Errorf("failed to deserialize record from page %d: %v", pageID, err)
	}

	return nil
}
