package graphdb

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/sirupsen/logrus"
)

// Executor executes parsed queries
type Executor struct {
	graph  *GraphManager
	txnMgr *TransactionManager
	vars   map[int64]map[string]interface{} // txnID -> varName -> []Node or []Edge
}

// NewExecutor initializes a new Executor
func NewExecutor(graph *GraphManager, txnMgr *TransactionManager) *Executor {
	return &Executor{
		graph:  graph,
		txnMgr: txnMgr,
		vars:   make(map[int64]map[string]interface{}),
	}
}

// Execute processes the AST and returns results
func (e *Executor) Execute(txnID int64, ast ASTNode) ([]map[string]interface{}, error) {
	if ast.Type != NodeQuery {
		return nil, fmt.Errorf("expected query node, got %v", ast.Type)
	}

	e.vars[txnID] = make(map[string]interface{})
	results := []map[string]interface{}{}

	for _, child := range ast.Children {
		switch child.Type {
		case NodeCreate:
			if err := e.executeCreate(txnID, child); err != nil {
				return nil, err
			}
		case NodeMatch:
			if err := e.executeMatch(txnID, child); err != nil {
				return nil, err
			}
		case NodeWhere:
			if err := e.executeWhere(txnID, child); err != nil {
				return nil, err
			}
		case NodeSet:
			if err := e.executeSet(txnID, child); err != nil {
				return nil, err
			}
		case NodeDelete:
			if err := e.executeDelete(txnID, child); err != nil {
				return nil, err
			}
		case NodeReturn:
			var err error
			results, err = e.executeReturn(txnID, child)
			if err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("unsupported AST node type: %v", child.Type)
		}
	}

	return results, nil
}

// executeCreate handles CREATE clauses
func (e *Executor) executeCreate(txnID int64, node ASTNode) error {
	if len(node.Children) != 1 || node.Children[0].Type != NodePattern {
		return fmt.Errorf("invalid CREATE pattern")
	}
	pattern := node.Children[0]

	if len(pattern.Children) == 1 && pattern.Children[0].Type == NodeNode {
		// Single node creation
		nodeNode := pattern.Children[0]
		newNode := Node{Properties: []Property{}, Active: true}
		varName := nodeNode.Value

		for _, child := range nodeNode.Children {
			if child.Type == NodeLabel {
				newNode.Labels = append(newNode.Labels, child.Value)
			} else if child.Type == NodeProperty {
				if len(child.Children) != 2 {
					return fmt.Errorf("invalid property in CREATE")
				}
				key := child.Children[0].Value
				valueNode := child.Children[1]
				if len(valueNode.Children) != 1 {
					return fmt.Errorf("invalid property value in CREATE")
				}
				propType := valueNode.Children[0].Value
				var value interface{}
				switch propType {
				case "int":
					v, err := strconv.ParseInt(valueNode.Value, 10, 64)
					if err != nil {
						return fmt.Errorf("invalid int value: %v", err)
					}
					value = v
					newNode.Properties = append(newNode.Properties, Property{Key: key, Value: value, Type: PropertyInt})
				case "string":
					value = valueNode.Value
					newNode.Properties = append(newNode.Properties, Property{Key: key, Value: value, Type: PropertyString})
				case "bool":
					value = strings.ToLower(valueNode.Value) == "true"
					newNode.Properties = append(newNode.Properties, Property{Key: key, Value: value, Type: PropertyBool})
				default:
					return fmt.Errorf("unsupported property type: %s", propType)
				}
			}
		}

		nodeID, err := e.graph.AddNode(newNode)
		if err != nil {
			return fmt.Errorf("failed to add node: %v", err)
		}

		if err := e.txnMgr.RecordOperation(txnID, TransactionOperation{
			Type:   OpAddNode,
			NodeID: nodeID,
		}); err != nil {
			return fmt.Errorf("failed to record operation: %v", err)
		}

		if varName != "" {
			node, err := e.graph.GetNode(nodeID)
			if err != nil {
				return fmt.Errorf("failed to retrieve created node: %v", err)
			}
			nodes, exists := e.vars[txnID][varName]
			if !exists {
				nodes = []Node{}
			}
			e.vars[txnID][varName] = append(nodes.([]Node), node)
		}

	} else if len(pattern.Children) == 3 && pattern.Children[0].Type == NodeNode && pattern.Children[1].Type == NodeRelationship && pattern.Children[2].Type == NodeNode {
		// Relationship creation
		sourceNode := pattern.Children[0]
		relNode := pattern.Children[1]
		targetNode := pattern.Children[2]

		var sourceID, targetID int64
		sourceVar := sourceNode.Value
		targetVar := targetNode.Value

		// Handle source node
		nodes, sourceExists := e.vars[txnID][sourceVar]
		if sourceExists && len(nodes.([]Node)) == 1 {
			// Source node is bound
			sourceID = nodes.([]Node)[0].ID
		} else {
			// Create new source node
			newSource := Node{Properties: []Property{}, Active: true}
			for _, child := range sourceNode.Children {
				if child.Type == NodeLabel {
					newSource.Labels = append(newSource.Labels, child.Value)
				} else if child.Type == NodeProperty {
					if len(child.Children) != 2 {
						return fmt.Errorf("invalid property in source node")
					}
					key := child.Children[0].Value
					valueNode := child.Children[1]
					if len(valueNode.Children) != 1 {
						return fmt.Errorf("invalid property value in source node")
					}
					propType := valueNode.Children[0].Value
					var value interface{}
					switch propType {
					case "int":
						v, err := strconv.ParseInt(valueNode.Value, 10, 64)
						if err != nil {
							return fmt.Errorf("invalid int value: %v", err)
						}
						value = v
						newSource.Properties = append(newSource.Properties, Property{Key: key, Value: value, Type: PropertyInt})
					case "string":
						value = valueNode.Value
						newSource.Properties = append(newSource.Properties, Property{Key: key, Value: value, Type: PropertyString})
					case "bool":
						value = strings.ToLower(valueNode.Value) == "true"
						newSource.Properties = append(newSource.Properties, Property{Key: key, Value: value, Type: PropertyBool})
					default:
						return fmt.Errorf("unsupported property type: %s", propType)
					}
				}
			}
			var err error
			sourceID, err = e.graph.AddNode(newSource)
			if err != nil {
				return fmt.Errorf("failed to create source node: %v", err)
			}
			if err := e.txnMgr.RecordOperation(txnID, TransactionOperation{
				Type:   OpAddNode,
				NodeID: sourceID,
			}); err != nil {
				return fmt.Errorf("failed to record source node operation: %v", err)
			}
			if sourceVar != "" {
				node, err := e.graph.GetNode(sourceID)
				if err != nil {
					return fmt.Errorf("failed to retrieve created source node: %v", err)
				}
				e.vars[txnID][sourceVar] = []Node{node}
			}
		}

		// Handle target node
		nodes, targetExists := e.vars[txnID][targetVar]
		if targetExists && len(nodes.([]Node)) == 1 {
			// Target node is bound
			targetID = nodes.([]Node)[0].ID
		} else {
			// Create new target node
			newTarget := Node{Properties: []Property{}, Active: true}
			for _, child := range targetNode.Children {
				if child.Type == NodeLabel {
					newTarget.Labels = append(newTarget.Labels, child.Value)
				} else if child.Type == NodeProperty {
					if len(child.Children) != 2 {
						return fmt.Errorf("invalid property in target node")
					}
					key := child.Children[0].Value
					valueNode := child.Children[1]
					if len(valueNode.Children) != 1 {
						return fmt.Errorf("invalid property value in target node")
					}
					propType := valueNode.Children[0].Value
					var value interface{}
					switch propType {
					case "int":
						v, err := strconv.ParseInt(valueNode.Value, 10, 64)
						if err != nil {
							return fmt.Errorf("invalid int value: %v", err)
						}
						value = v
						newTarget.Properties = append(newTarget.Properties, Property{Key: key, Value: value, Type: PropertyInt})
					case "string":
						value = valueNode.Value
						newTarget.Properties = append(newTarget.Properties, Property{Key: key, Value: value, Type: PropertyString})
					case "bool":
						value = strings.ToLower(valueNode.Value) == "true"
						newTarget.Properties = append(newTarget.Properties, Property{Key: key, Value: value, Type: PropertyBool})
					default:
						return fmt.Errorf("unsupported property type: %s", propType)
					}
				}
			}
			var err error
			targetID, err = e.graph.AddNode(newTarget)
			if err != nil {
				return fmt.Errorf("failed to create target node: %v", err)
			}
			if err := e.txnMgr.RecordOperation(txnID, TransactionOperation{
				Type:   OpAddNode,
				NodeID: targetID,
			}); err != nil {
				return fmt.Errorf("failed to record target node operation: %v", err)
			}
			if targetVar != "" {
				node, err := e.graph.GetNode(targetID)
				if err != nil {
					return fmt.Errorf("failed to retrieve created target node: %v", err)
				}
				e.vars[txnID][targetVar] = []Node{node}
			}
		}

		// Create relationship
		newEdge := Edge{
			Source:     sourceID,
			Target:     targetID,
			Properties: []Property{},
			Active:     true,
		}
		relVar := relNode.Value
		for _, child := range relNode.Children {
			if child.Type == NodeType {
				newEdge.Type = child.Value
			} else if child.Type == NodeProperty {
				if len(child.Children) != 2 {
					return fmt.Errorf("invalid property in relationship")
				}
				key := child.Children[0].Value
				valueNode := child.Children[1]
				if len(valueNode.Children) != 1 {
					return fmt.Errorf("invalid property value in relationship")
				}
				propType := valueNode.Children[0].Value
				var value interface{}
				switch propType {
				case "int":
					v, err := strconv.ParseInt(valueNode.Value, 10, 64)
					if err != nil {
						return fmt.Errorf("invalid int value: %v", err)
					}
					value = v
					newEdge.Properties = append(newEdge.Properties, Property{Key: key, Value: value, Type: PropertyInt})
				case "string":
					value = valueNode.Value
					newEdge.Properties = append(newEdge.Properties, Property{Key: key, Value: value, Type: PropertyString})
				case "bool":
					value = strings.ToLower(valueNode.Value) == "true"
					newEdge.Properties = append(newEdge.Properties, Property{Key: key, Value: value, Type: PropertyBool})
				default:
					return fmt.Errorf("unsupported property type: %s", propType)
				}
			}
		}
		if newEdge.Type == "" {
			return fmt.Errorf("relationship type required")
		}

		edgeID, err := e.graph.AddEdge(newEdge)
		if err != nil {
			return fmt.Errorf("failed to add edge: %v", err)
		}

		if err := e.txnMgr.RecordOperation(txnID, TransactionOperation{
			Type:   OpAddEdge,
			EdgeID: edgeID,
		}); err != nil {
			return fmt.Errorf("failed to record edge operation: %v", err)
		}

		if relVar != "" {
			edge, err := e.graph.GetEdge(edgeID)
			if err != nil {
				return fmt.Errorf("failed to retrieve created edge: %v", err)
			}
			edges, exists := e.vars[txnID][relVar]
			if !exists {
				edges = []Edge{}
			}
			e.vars[txnID][relVar] = append(edges.([]Edge), edge)
		}
	} else {
		return fmt.Errorf("invalid pattern in CREATE")
	}
	return nil
}

// executeMatch handles MATCH clauses
func (e *Executor) executeMatch(txnID int64, node ASTNode) error {
	if len(node.Children) != 1 || node.Children[0].Type != NodePattern {
		return fmt.Errorf("invalid MATCH pattern")
	}
	pattern := node.Children[0]

	if len(pattern.Children) == 1 && pattern.Children[0].Type == NodeNode {
		// Single node match
		nodeNode := pattern.Children[0]
		var label string
		varName := nodeNode.Value
		for _, child := range nodeNode.Children {
			if child.Type == NodeLabel {
				label = child.Value
				break
			}
		}

		if label == "" {
			return fmt.Errorf("MATCH requires a label")
		}

		nodeIDs, exists := e.graph.nodeLabelMap[label]
		if !exists || len(nodeIDs) == 0 {
			e.vars[txnID][varName] = []Node{}
			return nil
		}

		nodes := []Node{}
		for _, nodeID := range nodeIDs {
			node, err := e.graph.GetNode(nodeID)
			if err != nil {
				continue
			}
			if node.Active {
				nodes = append(nodes, node)
			}
		}

		if varName != "" {
			e.vars[txnID][varName] = nodes
		}
	} else if len(pattern.Children) == 3 && pattern.Children[0].Type == NodeNode && pattern.Children[1].Type == NodeRelationship && pattern.Children[2].Type == NodeNode {
		// Relationship match
		sourceNode := pattern.Children[0]
		relNode := pattern.Children[1]
		targetNode := pattern.Children[2]
		var relType string
		relVar := relNode.Value
		for _, child := range relNode.Children {
			if child.Type == NodeType {
				relType = child.Value
				break
			}
		}
		if relType == "" {
			return fmt.Errorf("MATCH requires a relationship type")
		}

		// Get all edge IDs from IndexManager
		edges := []Edge{}
		for edgeID := range e.graph.indexManager.edgeIndex {
			edge, err := e.graph.GetEdge(edgeID)
			if err != nil {
				continue
			}
			if edge.Active && edge.Type == relType {
				edges = append(edges, edge)
			}
		}

		if relVar != "" {
			e.vars[txnID][relVar] = edges
		}

		// Optionally bind source and target nodes
		if sourceNode.Value != "" {
			nodes := []Node{}
			for _, edge := range edges {
				node, err := e.graph.GetNode(edge.Source)
				if err != nil {
					continue
				}
				if node.Active {
					nodes = append(nodes, node)
				}
			}
			e.vars[txnID][sourceNode.Value] = nodes
		}
		if targetNode.Value != "" {
			nodes := []Node{}
			for _, edge := range edges {
				node, err := e.graph.GetNode(edge.Target)
				if err != nil {
					continue
				}
				if node.Active {
					nodes = append(nodes, node)
				}
			}
			e.vars[txnID][targetNode.Value] = nodes
		}
	} else {
		return fmt.Errorf("invalid pattern in MATCH")
	}
	return nil
}

// executeWhere handles WHERE clauses
func (e *Executor) executeWhere(txnID int64, node ASTNode) error {
	log := logrus.WithField("txn_id", txnID)
	if len(node.Children) != 1 || node.Children[0].Type != NodeExpression {
		return fmt.Errorf("invalid WHERE expression")
	}
	expr := node.Children[0]
	if len(expr.Children) != 3 {
		return fmt.Errorf("invalid expression format")
	}

	varName := expr.Children[0].Value
	key := expr.Children[1].Value
	valueNode := expr.Children[2]
	if len(valueNode.Children) != 1 {
		return fmt.Errorf("invalid expression value")
	}
	propType := valueNode.Children[0].Value

	var expectedValue interface{}
	switch propType {
	case "int":
		v, err := strconv.ParseInt(valueNode.Value, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid int value: %v", err)
		}
		expectedValue = v
	case "string":
		expectedValue = valueNode.Value
	case "bool":
		expectedValue = strings.ToLower(valueNode.Value) == "true"
	default:
		return fmt.Errorf("unsupported property type: %s", propType)
	}

	obj, exists := e.vars[txnID][varName]
	if !exists {
		return fmt.Errorf("variable %s not found in WHERE", varName)
	}

	if nodes, ok := obj.([]Node); ok {
		filteredNodes := []Node{}
		for _, node := range nodes {
			for _, prop := range node.Properties {
				if prop.Key == key && prop.Value == expectedValue {
					filteredNodes = append(filteredNodes, node)
					break
				}
			}
		}
		e.vars[txnID][varName] = filteredNodes
	} else if edges, ok := obj.([]Edge); ok {
		filteredEdges := []Edge{}
		for _, edge := range edges {
			for _, prop := range edge.Properties {
				if prop.Key == key && prop.Value == expectedValue {
					filteredEdges = append(filteredEdges, edge)
					break
				}
			}
		}
		e.vars[txnID][varName] = filteredEdges
	} else {
		return fmt.Errorf("variable %s is not a node or edge list", varName)
	}

	log.WithFields(logrus.Fields{
		"var_name": varName,
		"key":      key,
		"value":    expectedValue,
	}).Info("WHERE filter applied")
	return nil
}

// executeSet handles SET clauses
func (e *Executor) executeSet(txnID int64, node ASTNode) error {
	for _, child := range node.Children {
		if child.Type != NodeProperty || len(child.Children) != 3 {
			return fmt.Errorf("invalid SET property")
		}
		varName := child.Children[0].Value
		key := child.Children[1].Value
		valueNode := child.Children[2]
		if len(valueNode.Children) != 1 {
			return fmt.Errorf("invalid SET value")
		}
		propType := valueNode.Children[0].Value

		obj, exists := e.vars[txnID][varName]
		if !exists {
			return fmt.Errorf("variable %s not found", varName)
		}

		var prop Property
		switch propType {
		case "int":
			v, err := strconv.ParseInt(valueNode.Value, 10, 64)
			if err != nil {
				return fmt.Errorf("invalid int value: %v", err)
			}
			prop = Property{Key: key, Value: v, Type: PropertyInt}
		case "string":
			prop = Property{Key: key, Value: valueNode.Value, Type: PropertyString}
		case "bool":
			prop = Property{Key: key, Value: strings.ToLower(valueNode.Value) == "true", Type: PropertyBool}
		default:
			return fmt.Errorf("unsupported property type: %s", propType)
		}

		if nodes, ok := obj.([]Node); ok {
			for _, node := range nodes {
				if err := e.graph.UpdateNode(node.ID, []Property{prop}); err != nil {
					return fmt.Errorf("failed to update node %d: %v", node.ID, err)
				}

				if err := e.txnMgr.RecordOperation(txnID, TransactionOperation{
					Type:       OpUpdateNode,
					NodeID:     node.ID,
					Properties: []Property{prop},
				}); err != nil {
					return fmt.Errorf("failed to record operation: %v", err)
				}
			}
		} else if edges, ok := obj.([]Edge); ok {
			for _, edge := range edges {
				if err := e.graph.UpdateEdge(edge.ID, []Property{prop}); err != nil {
					return fmt.Errorf("failed to update edge %d: %v", edge.ID, err)
				}

				if err := e.txnMgr.RecordOperation(txnID, TransactionOperation{
					Type:       OpUpdateEdge,
					EdgeID:     edge.ID,
					Properties: []Property{prop},
				}); err != nil {
					return fmt.Errorf("failed to record operation: %v", err)
				}
			}
		} else {
			return fmt.Errorf("variable %s is not a node or edge list", varName)
		}
	}
	return nil
}

// executeDelete handles DELETE clauses
func (e *Executor) executeDelete(txnID int64, node ASTNode) error {
	for _, child := range node.Children {
		if child.Type != NodeIdentifier {
			return fmt.Errorf("invalid DELETE identifier")
		}
		varName := child.Value
		obj, exists := e.vars[txnID][varName]
		if !exists {
			return fmt.Errorf("variable %s not found", varName)
		}

		if nodes, ok := obj.([]Node); ok {
			for _, node := range nodes {
				if err := e.graph.DeleteNode(node.ID); err != nil {
					return fmt.Errorf("failed to delete node %d: %v", node.ID, err)
				}

				if err := e.txnMgr.RecordOperation(txnID, TransactionOperation{
					Type:   OpDeleteNode,
					NodeID: node.ID,
				}); err != nil {
					return fmt.Errorf("failed to record operation: %v", err)
				}

			}
		} else if edges, ok := obj.([]Edge); ok {
			for _, edge := range edges {
				if err := e.graph.DeleteEdge(edge.ID); err != nil {
					return fmt.Errorf("failed to delete edge %d: %v", edge.ID, err)
				}

				if err := e.txnMgr.RecordOperation(txnID, TransactionOperation{
					Type:   OpDeleteEdge,
					EdgeID: edge.ID,
				}); err != nil {
					return fmt.Errorf("failed to record operation: %v", err)
				}

			}
		} else {
			return fmt.Errorf("variable %s is not a node or edge list", varName)
		}

		delete(e.vars[txnID], varName)
	}
	return nil
}

// executeReturn handles RETURN clauses
func (e *Executor) executeReturn(txnID int64, node ASTNode) ([]map[string]interface{}, error) {
	results := []map[string]interface{}{}
	uniqueItems := make(map[string]map[string]interface{}) // key: type+id

	for _, child := range node.Children {
		if child.Type != NodeIdentifier {
			return nil, fmt.Errorf("invalid RETURN identifier")
		}
		varName := child.Value
		obj, exists := e.vars[txnID][varName]
		if !exists {
			continue
		}

		if nodes, ok := obj.([]Node); ok {
			for _, node := range nodes {
				key := fmt.Sprintf("node:%d", node.ID)
				if _, exists := uniqueItems[key]; !exists {
					result := map[string]interface{}{
						varName: map[string]interface{}{
							"id":         node.ID,
							"labels":     node.Labels,
							"properties": node.Properties,
						},
					}
					uniqueItems[key] = result
					results = append(results, result)
				}
			}
		} else if edges, ok := obj.([]Edge); ok {
			for _, edge := range edges {
				key := fmt.Sprintf("edge:%d", edge.ID)
				if _, exists := uniqueItems[key]; !exists {
					result := map[string]interface{}{
						varName: map[string]interface{}{
							"id":         edge.ID,
							"type":       edge.Type,
							"source":     edge.Source,
							"target":     edge.Target,
							"properties": edge.Properties,
						},
					}
					uniqueItems[key] = result
					results = append(results, result)
				}
			}
		} else {
			return nil, fmt.Errorf("variable %s is not a node or edge list", varName)
		}
	}

	return results, nil
}
