package graphdb

import (
	"fmt"

	"github.com/sirupsen/logrus"
)

// IndexManager handles indexing for nodes and edges
type IndexManager struct {
	nodeIndex map[int64]int // nodeID -> pageID
	edgeIndex map[int64]int // edgeID -> pageID
}

// NewIndexManager initializes a new IndexManager
func NewIndexManager() *IndexManager {
	log := logrus.WithField("component", "IndexManager")
	log.Info("Initializing IndexManager")
	return &IndexManager{
		nodeIndex: make(map[int64]int),
		edgeIndex: make(map[int64]int),
	}
}

// InsertNode adds a node to the index
func (im *IndexManager) InsertNode(nodeID int64, pageID int) error {
	log := logrus.WithFields(logrus.Fields{
		"node_id": nodeID,
		"page_id": pageID,
	})
	if _, exists := im.nodeIndex[nodeID]; exists {
		log.Error("Node ID already exists in index")
		return fmt.Errorf("node ID %d already exists", nodeID)
	}
	im.nodeIndex[nodeID] = pageID
	log.Debug("Node inserted into index")
	return nil
}

// InsertEdge adds an edge to the index
func (im *IndexManager) InsertEdge(edgeID int64, pageID int) error {
	log := logrus.WithFields(logrus.Fields{
		"edge_id": edgeID,
		"page_id": pageID,
	})
	if _, exists := im.edgeIndex[edgeID]; exists {
		log.Error("Edge ID already exists in index")
		return fmt.Errorf("edge ID %d already exists", edgeID)
	}
	im.edgeIndex[edgeID] = pageID
	log.Debug("Edge inserted into index")
	return nil
}

// SearchNode retrieves the page ID for a node
func (im *IndexManager) SearchNode(nodeID int64) (int, error) {
	log := logrus.WithField("node_id", nodeID)
	pageID, exists := im.nodeIndex[nodeID]
	if !exists {
		log.Error("Node not found in index")
		return -1, fmt.Errorf("node ID %d not found", nodeID)
	}
	log.Debug("Node found in index")
	return pageID, nil
}

// SearchEdge retrieves the page ID for an edge
func (im *IndexManager) SearchEdge(edgeID int64) (int, error) {
	log := logrus.WithField("edge_id", edgeID)
	pageID, exists := im.edgeIndex[edgeID]
	if !exists {
		log.Error("Edge not found in index")
		return -1, fmt.Errorf("edge ID %d not found", edgeID)
	}
	log.Debug("Edge found in index")
	return pageID, nil
}

// DeleteNode removes a node from the index
func (im *IndexManager) DeleteNode(nodeID int64) error {
	log := logrus.WithField("node_id", nodeID)
	if _, exists := im.nodeIndex[nodeID]; !exists {
		log.Error("Node not found in index for deletion")
		return fmt.Errorf("node ID %d not found", nodeID)
	}
	delete(im.nodeIndex, nodeID)
	log.Debug("Node deleted from index")
	return nil
}

// DeleteEdge removes an edge from the index
func (im *IndexManager) DeleteEdge(edgeID int64) error {
	log := logrus.WithField("edge_id", edgeID)
	if _, exists := im.edgeIndex[edgeID]; !exists {
		log.Error("Edge not found in index for deletion")
		return fmt.Errorf("edge ID %d not found", edgeID)
	}
	delete(im.edgeIndex, edgeID)
	log.Debug("Edge deleted from index")
	return nil
}

// GetEdgeIDs returns all edge IDs in the index
func (im *IndexManager) GetEdgeIDs() []int64 {
	log := logrus.WithField("component", "IndexManager")
	ids := make([]int64, 0, len(im.edgeIndex))
	for id := range im.edgeIndex {
		ids = append(ids, id)
	}
	log.WithField("edge_count", len(ids)).Debug("Retrieved edge IDs")
	return ids
}
