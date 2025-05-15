package graphdb

import (
	"encoding/binary"
	"os"
)

// StorageManager handles disk I/O for the database
type StorageManager struct {
	file     *os.File
	pageSize int
	numPages int
}

// NewStorageManager initializes a new StorageManager
func NewStorageManager(filename string, pageSize int) (*StorageManager, error) {

	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}

	fileInfo, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}
	fileSize := fileInfo.Size()

	numPages := int(fileSize / int64(pageSize))
	if fileSize%int64(pageSize) != 0 {
		file.Close()
		return nil, os.ErrInvalid
	}

	if fileSize == 0 {
		header := make([]byte, pageSize)
		copy(header[0:4], []byte("GDB\000"))
		binary.LittleEndian.PutUint32(header[4:8], uint32(pageSize))
		binary.LittleEndian.PutUint32(header[8:12], uint32(numPages))
		_, err = file.WriteAt(header, 0)
		if err != nil {
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
	if pageID < 0 || pageID >= sm.numPages {
		return nil, os.ErrInvalid
	}

	data := make([]byte, sm.pageSize)
	_, err := sm.file.ReadAt(data, int64(pageID)*int64(sm.pageSize))
	if err != nil {
		return nil, err
	}
	return data, nil
}

// WritePage writes a page to disk
func (sm *StorageManager) WritePage(pageID int, data []byte) error {
	if pageID < 0 || pageID >= sm.numPages || len(data) != sm.pageSize {
		return os.ErrInvalid
	}

	_, err := sm.file.WriteAt(data, int64(pageID)*int64(sm.pageSize))
	if err != nil {
		return err
	}
	return nil
}

// AllocatePage allocates a new page
func (sm *StorageManager) AllocatePage() (int, error) {
	pageID := sm.numPages
	newPage := make([]byte, sm.pageSize)
	_, err := sm.file.WriteAt(newPage, int64(pageID)*int64(sm.pageSize))
	if err != nil {
		return -1, err
	}
	sm.numPages++

	// Update header with new numPages
	header := make([]byte, sm.pageSize)
	_, err = sm.file.ReadAt(header, 0)
	if err != nil {
		return -1, err
	}
	binary.LittleEndian.PutUint32(header[8:12], uint32(sm.numPages))
	_, err = sm.file.WriteAt(header, 0)
	if err != nil {
		return -1, err
	}
	return pageID, nil
}

// Close closes the storage file
func (sm *StorageManager) Close() error {
	if err := sm.file.Sync(); err != nil {
		return err
	}
	if err := sm.file.Close(); err != nil {
		return err
	}
	return nil
}
