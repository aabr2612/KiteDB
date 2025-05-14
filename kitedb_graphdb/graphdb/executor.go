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
	vars   map[int64]map[string]interface{} // txnID -> varName -> []Node
}

// NewExecutor initializes a new Executor
func NewExecutor(graph *GraphManager, txnMgr *TransactionManager) *Executor {
	log := logrus.WithField("component", "Executor")
	log.Info("Initializing Executor")
	return &Executor{
		graph:  graph,
		txnMgr: txnMgr,
		vars:   make(map[int64]map[string]interface{}),
	}
}

// Execute processes the AST and returns results
func (e *Executor) Execute(txnID int64, ast ASTNode) ([]map[string]interface{}, error) {
	log := logrus.WithFields(logrus.Fields{
		"txn_id":       txnID,
		"ast_type":     ast.Type,
		"ast_children": len(ast.Children),
	})
	log.Debug("Executing query")

	if ast.Type != NodeQuery {
		return nil, fmt.Errorf("expected query node, got %v", ast.Type)
	}

	e.vars[txnID] = make(map[string]interface{})
	results := []map[string]interface{}{}

	for _, child := range ast.Children {
		switch child.Type {
		case NodeCreate:
			if err := e.executeCreate(txnID, child); err != nil {
				log.WithError(err).Error("CREATE execution failed")
				return nil, err
			}
		case NodeMatch:
			if err := e.executeMatch(txnID, child); err != nil {
				log.WithError(err).Error("MATCH execution failed")
				return nil, err
			}
		case NodeWhere:
			if err := e.executeWhere(txnID, child); err != nil {
				log.WithError(err).Error("WHERE execution failed")
				return nil, err
			}
		case NodeSet:
			if err := e.executeSet(txnID, child); err != nil {
				log.WithError(err).Error("SET execution failed")
				return nil, err
			}
		case NodeDelete:
			if err := e.executeDelete(txnID, child); err != nil {
				log.WithError(err).Error("DELETE execution failed")
				return nil, err
			}
		case NodeReturn:
			var err error
			results, err = e.executeReturn(txnID, child)
			if err != nil {
				log.WithError(err).Error("RETURN execution failed")
				return nil, err
			}
		default:
			log.WithField("node_type", child.Type).Error("Unsupported AST node")
			return nil, fmt.Errorf("unsupported AST node type: %v", child.Type)
		}
	}

	log.Info("Query executed successfully")
	return results, nil
}

// executeCreate handles CREATE clauses
func (e *Executor) executeCreate(txnID int64, node ASTNode) error {
	log := logrus.WithField("txn_id", txnID)
	if len(node.Children) != 1 || node.Children[0].Type != NodePattern {
		return fmt.Errorf("invalid CREATE pattern")
	}
	pattern := node.Children[0]
	if len(pattern.Children) != 1 || pattern.Children[0].Type != NodeNode {
		return fmt.Errorf("invalid node pattern")
	}
	nodeNode := pattern.Children[0]

	newNode := Node{Properties: []Property{}}
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

	log.WithField("node_id", nodeID).Info("Node created")
	return nil
}

// executeMatch handles MATCH clauses
func (e *Executor) executeMatch(txnID int64, node ASTNode) error {
	log := logrus.WithField("txn_id", txnID)
	if len(node.Children) != 1 || node.Children[0].Type != NodePattern {
		return fmt.Errorf("invalid MATCH pattern")
	}
	pattern := node.Children[0]
	if len(pattern.Children) != 1 || pattern.Children[0].Type != NodeNode {
		return fmt.Errorf("invalid node pattern")
	}
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
		log.WithField("label", label).Debug("No nodes found for label")
		e.vars[txnID][varName] = []Node{}
		return nil
	}

	nodes := []Node{}
	for _, nodeID := range nodeIDs {
		node, err := e.graph.GetNode(nodeID)
		if err != nil {
			log.WithError(err).WithField("node_id", nodeID).Warn("Failed to retrieve node")
			continue
		}
		if node.Active {
			nodes = append(nodes, node)
		}
	}

	if varName != "" {
		e.vars[txnID][varName] = nodes
	}

	log.WithFields(logrus.Fields{
		"label":      label,
		"node_count": len(nodes),
	}).Info("MATCH executed")
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
	nodes, ok := obj.([]Node)
	if !ok {
		return fmt.Errorf("variable %s is not a node list", varName)
	}

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

	log.WithFields(logrus.Fields{
		"var_name": varName,
		"key":      key,
		"value":    expectedValue,
	}).Info("WHERE filter applied")
	return nil
}

// executeSet handles SET clauses
func (e *Executor) executeSet(txnID int64, node ASTNode) error {
	log := logrus.WithField("txn_id", txnID)
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
		nodes, ok := obj.([]Node)
		if !ok {
			return fmt.Errorf("variable %s is not a node list", varName)
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

			log.WithFields(logrus.Fields{
				"node_id": node.ID,
				"key":     key,
			}).Info("SET property updated")
		}
	}
	return nil
}

// executeDelete handles DELETE clauses
func (e *Executor) executeDelete(txnID int64, node ASTNode) error {
	log := logrus.WithField("txn_id", txnID)
	for _, child := range node.Children {
		if child.Type != NodeIdentifier {
			return fmt.Errorf("invalid DELETE identifier")
		}
		varName := child.Value
		obj, exists := e.vars[txnID][varName]
		if !exists {
			return fmt.Errorf("variable %s not found", varName)
		}
		nodes, ok := obj.([]Node)
		if !ok {
			return fmt.Errorf("variable %s is not a node list", varName)
		}

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

			log.WithField("node_id", node.ID).Info("Node deleted")
		}

		delete(e.vars[txnID], varName)
	}
	return nil
}

// executeReturn handles RETURN clauses
func (e *Executor) executeReturn(txnID int64, node ASTNode) ([]map[string]interface{}, error) {
	log := logrus.WithField("txn_id", txnID)
	results := []map[string]interface{}{}
	uniqueNodes := make(map[int64]map[string]interface{})

	for _, child := range node.Children {
		if child.Type != NodeIdentifier {
			return nil, fmt.Errorf("invalid RETURN identifier")
		}
		varName := child.Value
		obj, exists := e.vars[txnID][varName]
		if !exists {
			log.WithField("var_name", varName).Warn("Variable not found in RETURN")
			continue
		}
		nodes, ok := obj.([]Node)
		if !ok {
			return nil, fmt.Errorf("variable %s is not a node list", varName)
		}

		for _, node := range nodes {
			if _, exists := uniqueNodes[node.ID]; !exists {
				result := map[string]interface{}{
					varName: map[string]interface{}{
						"id":         node.ID,
						"labels":     node.Labels,
						"properties": node.Properties,
					},
				}
				uniqueNodes[node.ID] = result
				results = append(results, result)
			}
		}
	}

	log.WithField("result_count", len(results)).Info("RETURN executed")
	return results, nil
}
