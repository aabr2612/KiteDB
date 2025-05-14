package graphdb

import (
	"container/list"
	"fmt"

	"github.com/sirupsen/logrus"
)

// BufferPool manages a cache of pages in memory
type BufferPool struct {
	storage  *StorageManager
	capacity int
	pages    map[int][]byte
	lru      *list.List
	lruKeys  map[int]*list.Element
}

// NewBufferPool initializes a new BufferPool
func NewBufferPool(storage *StorageManager, capacity int) *BufferPool {
	log := logrus.WithField("capacity", capacity)
	log.Info("Initializing BufferPool (single-threaded, write-through)")
	return &BufferPool{
		storage:  storage,
		capacity: capacity,
		pages:    make(map[int][]byte),
		lru:      list.New(),
		lruKeys:  make(map[int]*list.Element),
	}
}

// GetPage retrieves a page, loading from disk if not in cache
func (bp *BufferPool) GetPage(pageID int) ([]byte, error) {
	log := logrus.WithField("page_id", pageID)

	// Check if page is in cache
	if data, ok := bp.pages[pageID]; ok {
		// Update LRU
		if elem, exists := bp.lruKeys[pageID]; exists {
			bp.lru.MoveToFront(elem)
		}
		log.Debug("Page found in buffer pool")
		return data, nil
	}

	// Load page from disk
	data, err := bp.storage.ReadPage(pageID)
	if err != nil {
		log.WithError(err).Error("Failed to read page from storage")
		return nil, err
	}

	// Evict if cache is full
	if len(bp.pages) >= bp.capacity {
		if err := bp.evictPage(); err != nil {
			log.WithError(err).Error("Failed to evict page")
			return nil, err
		}
	}

	// Add to cache
	bp.pages[pageID] = data
	elem := bp.lru.PushFront(pageID)
	bp.lruKeys[pageID] = elem
	log.Info("Page loaded into buffer pool")
	return data, nil
}

// WritePage writes a page to disk and updates the cache
func (bp *BufferPool) WritePage(pageID int, data []byte) error {
	log := logrus.WithField("page_id", pageID)

	// Write directly to disk (write-through)
	if err := bp.storage.WritePage(pageID, data); err != nil {
		log.WithError(err).Error("Failed to write page to storage")
		return err
	}

	// Update cache if page exists, or add it
	if _, ok := bp.pages[pageID]; ok {
		bp.pages[pageID] = data
		if elem, exists := bp.lruKeys[pageID]; exists {
			bp.lru.MoveToFront(elem)
		}
	} else {
		if len(bp.pages) >= bp.capacity {
			if err := bp.evictPage(); err != nil {
				log.WithError(err).Error("Failed to evict page")
				return err
			}
		}
		bp.pages[pageID] = data
		elem := bp.lru.PushFront(pageID)
		bp.lruKeys[pageID] = elem
	}
	log.Debug("Page written and cached")
	return nil
}

// evictPage removes the least recently used page from the cache
func (bp *BufferPool) evictPage() error {
	log := logrus.WithField("component", "buffer_pool")
	if bp.lru.Len() == 0 {
		log.Error("No pages to evict")
		return fmt.Errorf("buffer pool empty")
	}

	elem := bp.lru.Back()
	pageID := elem.Value.(int)
	bp.lru.Remove(elem)
	delete(bp.pages, pageID)
	delete(bp.lruKeys, pageID)
	log.WithField("page_id", pageID).Debug("Evicted page")
	return nil
}

// Close cleans up the buffer pool
func (bp *BufferPool) Close() error {
	log := logrus.WithField("component", "buffer_pool")
	log.Info("Closing BufferPool")
	bp.pages = make(map[int][]byte)
	bp.lru.Init()
	bp.lruKeys = make(map[int]*list.Element)
	log.Info("BufferPool closed")
	return nil
}
