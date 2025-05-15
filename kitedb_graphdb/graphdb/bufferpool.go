package graphdb

import (
	"container/list"
	"fmt"
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

	// Check if page is in cache
	if data, ok := bp.pages[pageID]; ok {
		// Update LRU
		if elem, exists := bp.lruKeys[pageID]; exists {
			bp.lru.MoveToFront(elem)
		}
		return data, nil
	}

	// Load page from disk
	data, err := bp.storage.ReadPage(pageID)
	if err != nil {
		return nil, err
	}

	// Evict if cache is full
	if len(bp.pages) >= bp.capacity {
		if err := bp.evictPage(); err != nil {
			return nil, err
		}
	}

	// Add to cache
	bp.pages[pageID] = data
	elem := bp.lru.PushFront(pageID)
	bp.lruKeys[pageID] = elem
	return data, nil
}

// WritePage writes a page to disk and updates the cache
func (bp *BufferPool) WritePage(pageID int, data []byte) error {

	// Write directly to disk (write-through)
	if err := bp.storage.WritePage(pageID, data); err != nil {
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
				return err
			}
		}
		bp.pages[pageID] = data
		elem := bp.lru.PushFront(pageID)
		bp.lruKeys[pageID] = elem
	}
	return nil
}

// evictPage removes the least recently used page from the cache
func (bp *BufferPool) evictPage() error {
	if bp.lru.Len() == 0 {
		return fmt.Errorf("buffer pool empty")
	}

	elem := bp.lru.Back()
	pageID := elem.Value.(int)
	bp.lru.Remove(elem)
	delete(bp.pages, pageID)
	delete(bp.lruKeys, pageID)
	return nil
}

// Close cleans up the buffer pool
func (bp *BufferPool) Close() error {
	bp.pages = make(map[int][]byte)
	bp.lru.Init()
	bp.lruKeys = make(map[int]*list.Element)
	return nil
}
