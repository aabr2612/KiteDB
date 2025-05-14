package graphdb

import (
	"fmt"

	"github.com/sirupsen/logrus"
)

// RecordManager handles page-level record operations
type RecordManager struct {
	bufferPool *BufferPool
	pageSize   int
}

// NewRecordManager initializes a new RecordManager
func NewRecordManager(bufferPool *BufferPool, pageSize int) *RecordManager {
	log := logrus.WithFields(logrus.Fields{
		"component": "RecordManager",
		"page_size": pageSize,
	})
	log.Info("Initializing RecordManager")
	return &RecordManager{
		bufferPool: bufferPool,
		pageSize:   pageSize,
	}
}

// WriteRecord serializes and writes a record (Node or Edge) to a new page
func (rm *RecordManager) WriteRecord(record interface{}) (int, error) {
	log := logrus.WithField("component", "RecordManager")
	data, err := Serialize(record)
	if err != nil {
		log.WithError(err).Error("Failed to serialize record")
		return -1, fmt.Errorf("failed to serialize record: %v", err)
	}

	if len(data) > rm.pageSize {
		log.WithField("data_size", len(data)).Error("Record too large for page")
		return -1, fmt.Errorf("record size %d exceeds page size %d", len(data), rm.pageSize)
	}

	pageID, err := rm.bufferPool.storage.AllocatePage()
	if err != nil {
		log.WithError(err).Error("Failed to allocate page")
		return -1, fmt.Errorf("failed to allocate page: %v", err)
	}

	// Pad data to page size
	paddedData := make([]byte, rm.pageSize)
	copy(paddedData, data)

	if err := rm.bufferPool.WritePage(pageID, paddedData); err != nil {
		log.WithError(err).WithField("page_id", pageID).Error("Failed to write record")
		return -1, fmt.Errorf("failed to write record to page %d: %v", pageID, err)
	}

	log.WithField("page_id", pageID).Debug("Record written to page")
	return pageID, nil
}

// ReadRecord reads and deserializes a record from a page
func (rm *RecordManager) ReadRecord(pageID int, record interface{}) error {
	log := logrus.WithFields(logrus.Fields{
		"component": "RecordManager",
		"page_id":   pageID,
	})
	data, err := rm.bufferPool.GetPage(pageID)
	if err != nil {
		log.WithError(err).Error("Failed to read page")
		return fmt.Errorf("failed to read page %d: %v", pageID, err)
	}

	if err := Deserialize(data, record); err != nil {
		log.WithError(err).Error("Failed to deserialize record")
		return fmt.Errorf("failed to deserialize record from page %d: %v", pageID, err)
	}

	log.Debug("Record read from page")
	return nil
}
