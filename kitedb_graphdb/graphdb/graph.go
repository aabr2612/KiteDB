package graphdb

import (
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
)

// GraphManager manages graph operations
type GraphManager struct {
	bufferPool   *BufferPool
	indexManager *IndexManager
	recordMgr    *RecordManager
	nextNodeID   int64
	nextEdgeID   int64
	nodeLabelMap map[string][]int64 // Label -> Node IDs
}

// NewGraphManager initializes a new GraphManager
func NewGraphManager(bufferPool *BufferPool, indexManager *IndexManager, recordMgr *RecordManager) *GraphManager {
	return &GraphManager{
		bufferPool:   bufferPool,
		indexManager: indexManager,
		recordMgr:    recordMgr,
		nextNodeID:   1,
		nextEdgeID:   1,
		nodeLabelMap: make(map[string][]int64),
	}
}

// AddNode adds a new node to the graph
func (gm *GraphManager) AddNode(node Node) (int64, error) {
	node.ID = gm.nextNodeID
	node.Active = true
	gm.nextNodeID++

	pageID, err := gm.recordMgr.WriteRecord(node)
	if err != nil {
		return 0, fmt.Errorf("failed to write node: %v", err)
	}

	if err := gm.indexManager.InsertNode(node.ID, pageID); err != nil {
		return 0, fmt.Errorf("failed to insert node into index: %v", err)
	}

	for _, label := range node.Labels {
		gm.nodeLabelMap[label] = append(gm.nodeLabelMap[label], node.ID)
	}

	return node.ID, nil
}

// AddEdge adds a new edge to the graph
func (gm *GraphManager) AddEdge(edge Edge) (int64, error) {
	edge.ID = gm.nextEdgeID
	edge.Active = true
	gm.nextEdgeID++

	pageID, err := gm.recordMgr.WriteRecord(edge)
	if err != nil {
		return 0, fmt.Errorf("failed to write edge: %v", err)
	}

	if err := gm.indexManager.InsertEdge(edge.ID, pageID); err != nil {
		return 0, fmt.Errorf("failed to insert edge into index: %v", err)
	}

	return edge.ID, nil
}

// GetNode retrieves a node by ID
func (gm *GraphManager) GetNode(nodeID int64) (Node, error) {
	log := logrus.WithField("node_id", nodeID)
	start := time.Now()
	defer func() {
		log.WithField("duration_ms", time.Since(start).Milliseconds()).Debug("GetNode completed")
	}()

	pageID, err := gm.indexManager.SearchNode(nodeID)
	if err != nil {
		return Node{}, fmt.Errorf("failed to find node %d: %v", nodeID, err)
	}

	var node Node
	if err := gm.recordMgr.ReadRecord(pageID, &node); err != nil {
		return Node{}, fmt.Errorf("failed to read node %d: %v", nodeID, err)
	}

	if !node.Active {
		return Node{}, fmt.Errorf("node %d is not active", nodeID)
	}

	return node, nil
}

// GetEdge retrieves an edge by ID
func (gm *GraphManager) GetEdge(edgeID int64) (Edge, error) {
	log := logrus.WithFields(logrus.Fields{
		"edge_id":      edgeID,
		"next_edge_id": gm.nextEdgeID,
	})
	start := time.Now()
	defer func() {
		log.WithField("duration_ms", time.Since(start).Milliseconds()).Debug("GetEdge completed")
	}()

	pageID, err := gm.indexManager.SearchEdge(edgeID)
	if err != nil {
		return Edge{}, fmt.Errorf("failed to find edge %d: %v", edgeID, err)
	}

	var edge Edge
	if err := gm.recordMgr.ReadRecord(pageID, &edge); err != nil {
		return Edge{}, fmt.Errorf("failed to read edge %d: %v", edgeID, err)
	}

	if !edge.Active {
		return Edge{}, fmt.Errorf("edge %d is not active", edgeID)
	}

	return edge, nil
}

// UpdateNode updates a node's properties
func (gm *GraphManager) UpdateNode(nodeID int64, newProperties []Property) error {
	node, err := gm.GetNode(nodeID)
	if err != nil {
		return fmt.Errorf("failed to get node %d: %v", nodeID, err)
	}

	// Merge properties
	propMap := make(map[string]Property)
	for _, p := range node.Properties {
		propMap[p.Key] = p
	}
	for _, p := range newProperties {
		propMap[p.Key] = p
	}
	node.Properties = make([]Property, 0, len(propMap))
	for _, p := range propMap {
		node.Properties = append(node.Properties, p)
	}

	pageID, err := gm.recordMgr.WriteRecord(node)
	if err != nil {
		return fmt.Errorf("failed to write updated node: %v", err)
	}

	// Remove old index entry
	if err := gm.indexManager.DeleteNode(nodeID); err != nil {
		return fmt.Errorf("failed to delete old node %d index entry: %v", nodeID, err)
	}

	// Insert new index entry
	if err := gm.indexManager.InsertNode(nodeID, pageID); err != nil {
		return fmt.Errorf("failed to update node %d in index: %v", nodeID, err)
	}

	return nil
}

// UpdateEdge updates an edge's properties
func (gm *GraphManager) UpdateEdge(edgeID int64, newProperties []Property) error {
	edge, err := gm.GetEdge(edgeID)
	if err != nil {
		return fmt.Errorf("failed to get edge %d: %v", edgeID, err)
	}

	// Merge properties
	propMap := make(map[string]Property)
	for _, p := range edge.Properties {
		propMap[p.Key] = p
	}
	for _, p := range newProperties {
		propMap[p.Key] = p
	}
	edge.Properties = make([]Property, 0, len(propMap))
	for _, p := range propMap {
		edge.Properties = append(edge.Properties, p)
	}

	pageID, err := gm.recordMgr.WriteRecord(edge)
	if err != nil {
		return fmt.Errorf("failed to write updated edge: %v", err)
	}

	// Remove old index entry
	if err := gm.indexManager.DeleteEdge(edgeID); err != nil {
		return fmt.Errorf("failed to delete old edge %d index entry: %v", edgeID, err)
	}

	// Insert new index entry
	if err := gm.indexManager.InsertEdge(edgeID, pageID); err != nil {
		return fmt.Errorf("failed to update edge %d in index: %v", edgeID, err)
	}

	return nil
}

// DeleteNode marks a node as inactive
func (gm *GraphManager) DeleteNode(nodeID int64) error {
	node, err := gm.GetNode(nodeID)
	if err != nil {
		return fmt.Errorf("failed to get node %d: %v", nodeID, err)
	}

	node.Active = false
	_, err = gm.indexManager.SearchNode(nodeID)
	if err != nil {
		return fmt.Errorf("failed to find node %d in index: %v", nodeID, err)
	}

	if _, err := gm.recordMgr.WriteRecord(node); err != nil {
		return fmt.Errorf("failed to write deleted node: %v", err)
	}

	if err := gm.indexManager.DeleteNode(nodeID); err != nil {
		return fmt.Errorf("failed to delete node from index: %v", err)
	}

	// Remove from nodeLabelMap and clean up empty entries
	for label := range gm.nodeLabelMap {
		newIDs := make([]int64, 0, len(gm.nodeLabelMap[label]))
		for _, id := range gm.nodeLabelMap[label] {
			if id != nodeID {
				newIDs = append(newIDs, id)
			}
		}
		if len(newIDs) == 0 {
			delete(gm.nodeLabelMap, label)
		} else {
			gm.nodeLabelMap[label] = newIDs
		}
	}

	return nil
}

// DeleteEdge marks an edge as inactive
func (gm *GraphManager) DeleteEdge(edgeID int64) error {
	edge, err := gm.GetEdge(edgeID)
	if err != nil {
		return fmt.Errorf("failed to get edge %d: %v", edgeID, err)
	}

	edge.Active = false
	_, err = gm.indexManager.SearchEdge(edgeID)
	if err != nil {
		return fmt.Errorf("failed to find edge %d in index: %v", edgeID, err)
	}

	if _, err := gm.recordMgr.WriteRecord(edge); err != nil {
		return fmt.Errorf("failed to write deleted edge: %v", err)
	}

	if err := gm.indexManager.DeleteEdge(edgeID); err != nil {
		return fmt.Errorf("failed to delete edge from index: %v", err)
	}

	return nil
}
