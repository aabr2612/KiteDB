package graphdb

import (
	"encoding/binary"
	"os"

	"github.com/sirupsen/logrus"
)

// StorageManager handles disk I/O for the database
type StorageManager struct {
	file     *os.File
	pageSize int
	numPages int
}

// NewStorageManager initializes a new StorageManager
func NewStorageManager(filename string, pageSize int) (*StorageManager, error) {
	log := logrus.WithFields(logrus.Fields{
		"filename":  filename,
		"page_size": pageSize,
	})
	log.Info("Initializing StorageManager")

	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		log.WithError(err).Error("Failed to open storage file")
		return nil, err
	}

	fileInfo, err := file.Stat()
	if err != nil {
		log.WithError(err).Error("Failed to stat storage file")
		file.Close()
		return nil, err
	}
	fileSize := fileInfo.Size()

	numPages := int(fileSize / int64(pageSize))
	if fileSize%int64(pageSize) != 0 {
		log.Error("File size not aligned with page size")
		file.Close()
		return nil, os.ErrInvalid
	}

	if fileSize == 0 {
		log.Debug("Initializing new database file with header")
		header := make([]byte, pageSize)
		copy(header[0:4], []byte("GDB\000"))
		binary.LittleEndian.PutUint32(header[4:8], uint32(pageSize))
		binary.LittleEndian.PutUint32(header[8:12], uint32(numPages))
		_, err = file.WriteAt(header, 0)
		if err != nil {
			log.WithError(err).Error("Failed to write header")
			file.Close()
			return nil, err
		}
		numPages = 1
	}

	return &StorageManager{
		file:     file,
		pageSize: pageSize,
		numPages: numPages,
	}, nil
}

// ReadPage reads a page from disk
func (sm *StorageManager) ReadPage(pageID int) ([]byte, error) {
	log := logrus.WithField("page_id", pageID)
	if pageID < 0 || pageID >= sm.numPages {
		log.Error("Invalid page ID")
		return nil, os.ErrInvalid
	}

	data := make([]byte, sm.pageSize)
	_, err := sm.file.ReadAt(data, int64(pageID)*int64(sm.pageSize))
	if err != nil {
		log.WithError(err).Error("Failed to read page")
		return nil, err
	}
	return data, nil
}

// WritePage writes a page to disk
func (sm *StorageManager) WritePage(pageID int, data []byte) error {
	log := logrus.WithField("page_id", pageID)
	if pageID < 0 || pageID >= sm.numPages || len(data) != sm.pageSize {
		log.Error("Invalid page ID or data length")
		return os.ErrInvalid
	}

	_, err := sm.file.WriteAt(data, int64(pageID)*int64(sm.pageSize))
	if err != nil {
		log.WithError(err).Error("Failed to write page")
		return err
	}
	return nil
}

// AllocatePage allocates a new page
func (sm *StorageManager) AllocatePage() (int, error) {
	log := logrus.WithField("component", "StorageManager")
	pageID := sm.numPages
	newPage := make([]byte, sm.pageSize)
	_, err := sm.file.WriteAt(newPage, int64(pageID)*int64(sm.pageSize))
	if err != nil {
		log.WithError(err).Error("Failed to allocate new page")
		return -1, err
	}
	sm.numPages++
	log.WithField("new_page_id", pageID).Info("Allocated new page")

	// Update header with new numPages
	header := make([]byte, sm.pageSize)
	_, err = sm.file.ReadAt(header, 0)
	if err != nil {
		log.WithError(err).Error("Failed to read header")
		return -1, err
	}
	binary.LittleEndian.PutUint32(header[8:12], uint32(sm.numPages))
	_, err = sm.file.WriteAt(header, 0)
	if err != nil {
		log.WithError(err).Error("Failed to update header")
		return -1, err
	}
	return pageID, nil
}

// Close closes the storage file
func (sm *StorageManager) Close() error {
	log := logrus.WithField("component", "StorageManager")
	if err := sm.file.Sync(); err != nil {
		log.WithError(err).Error("Failed to sync file")
		return err
	}
	if err := sm.file.Close(); err != nil {
		log.WithError(err).Error("Failed to close file")
		return err
	}
	log.Info("Storage file closed")
	return nil
}
