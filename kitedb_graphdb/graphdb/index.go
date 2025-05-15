package graphdb

import (
	"fmt"
)

// IndexManager handles indexing for nodes and edges
type IndexManager struct {
	nodeIndex map[int64]int // nodeID -> pageID
	edgeIndex map[int64]int // edgeID -> pageID
}

// NewIndexManager initializes a new IndexManager
func NewIndexManager() *IndexManager {
	return &IndexManager{
		nodeIndex: make(map[int64]int),
		edgeIndex: make(map[int64]int),
	}
}

// InsertNode adds a node to the index
func (im *IndexManager) InsertNode(nodeID int64, pageID int) error {
	if _, exists := im.nodeIndex[nodeID]; exists {
		return fmt.Errorf("node ID %d already exists", nodeID)
	}
	im.nodeIndex[nodeID] = pageID
	return nil
}

// InsertEdge adds an edge to the index
func (im *IndexManager) InsertEdge(edgeID int64, pageID int) error {
	if _, exists := im.edgeIndex[edgeID]; exists {
		return fmt.Errorf("edge ID %d already exists", edgeID)
	}
	im.edgeIndex[edgeID] = pageID
	return nil
}

// SearchNode retrieves the page ID for a node
func (im *IndexManager) SearchNode(nodeID int64) (int, error) {
	pageID, exists := im.nodeIndex[nodeID]
	if !exists {
		return -1, fmt.Errorf("node ID %d not found", nodeID)
	}
	return pageID, nil
}

// SearchEdge retrieves the page ID for an edge
func (im *IndexManager) SearchEdge(edgeID int64) (int, error) {
	pageID, exists := im.edgeIndex[edgeID]
	if !exists {
		return -1, fmt.Errorf("edge ID %d not found", edgeID)
	}
	return pageID, nil
}

// DeleteNode removes a node from the index
func (im *IndexManager) DeleteNode(nodeID int64) error {
	if _, exists := im.nodeIndex[nodeID]; !exists {
		return fmt.Errorf("node ID %d not found", nodeID)
	}
	delete(im.nodeIndex, nodeID)
	return nil
}

// DeleteEdge removes an edge from the index
func (im *IndexManager) DeleteEdge(edgeID int64) error {
	if _, exists := im.edgeIndex[edgeID]; !exists {
		return fmt.Errorf("edge ID %d not found", edgeID)
	}
	delete(im.edgeIndex, edgeID)
	return nil
}

// GetEdgeIDs returns all edge IDs in the index
func (im *IndexManager) GetEdgeIDs() []int64 {
	ids := make([]int64, 0, len(im.edgeIndex))
	for id := range im.edgeIndex {
		ids = append(ids, id)
	}
	return ids
}
