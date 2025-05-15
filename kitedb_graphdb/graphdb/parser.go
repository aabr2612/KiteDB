package graphdb

import (
	"fmt"
	"strings"
)

// Parser converts tokens into an AST
type Parser struct {
	tokens []Token
	pos    int
}

// NewParser initializes a new Parser
func NewParser(tokens []Token) *Parser {
	return &Parser{
		tokens: tokens,
		pos:    0,
	}
}

// Parse parses the query into an AST
func (p *Parser) Parse() (ASTNode, error) {
	if p.pos >= len(p.tokens) {
		return ASTNode{}, fmt.Errorf("empty query")
	}
	node, err := p.query()
	if err != nil {
		return ASTNode{}, err
	}
	if p.pos < len(p.tokens) && p.tokens[p.pos].Type != TokenEOF {
		return ASTNode{}, fmt.Errorf("unexpected tokens at position %d", p.pos)
	}
	return node, nil
}

// query parses a full query
func (p *Parser) query() (ASTNode, error) {
	node := ASTNode{Type: NodeQuery}
	for p.pos < len(p.tokens) && p.tokens[p.pos].Type != TokenEOF {
		if p.pos >= len(p.tokens) {
			break
		}
		switch strings.ToUpper(p.tokens[p.pos].Value) {
		case "CREATE":
			createNode, err := p.createClause()
			if err != nil {
				return ASTNode{}, err
			}
			node.Children = append(node.Children, createNode)
		case "MATCH":
			matchNode, err := p.matchClause()
			if err != nil {
				return ASTNode{}, err
			}
			node.Children = append(node.Children, matchNode)
		case "WHERE":
			whereNode, err := p.whereClause()
			if err != nil {
				return ASTNode{}, err
			}
			node.Children = append(node.Children, whereNode)
		case "SET":
			setNode, err := p.setClause()
			if err != nil {
				return ASTNode{}, err
			}
			node.Children = append(node.Children, setNode)
		case "DELETE":
			deleteNode, err := p.deleteClause()
			if err != nil {
				return ASTNode{}, err
			}
			node.Children = append(node.Children, deleteNode)
		case "RETURN":
			returnNode, err := p.returnClause()
			if err != nil {
				return ASTNode{}, err
			}
			node.Children = append(node.Children, returnNode)
		default:
			return ASTNode{}, fmt.Errorf("unexpected token %s at position %d", p.tokens[p.pos].Value, p.pos)
		}
	}
	return node, nil
}

// createClause parses a CREATE clause
func (p *Parser) createClause() (ASTNode, error) {
	if !p.expect(TokenKeyword, "CREATE") {
		return ASTNode{}, fmt.Errorf("expected CREATE at position %d", p.pos)
	}
	node := ASTNode{Type: NodeCreate}
	for {
		pattern, err := p.pattern()
		if err != nil {
			return ASTNode{}, err
		}
		node.Children = append(node.Children, pattern)
		if p.pos >= len(p.tokens) || !p.accept(TokenSymbol, ",") {
			break
		}
	}
	return node, nil
}

// matchClause parses a MATCH clause
func (p *Parser) matchClause() (ASTNode, error) {
	if !p.expect(TokenKeyword, "MATCH") {
		return ASTNode{}, fmt.Errorf("expected MATCH at position %d", p.pos)
	}
	node := ASTNode{Type: NodeMatch}
	for {
		pattern, err := p.pattern()
		if err != nil {
			return ASTNode{}, err
		}
		node.Children = append(node.Children, pattern)
		if p.pos >= len(p.tokens) || !p.accept(TokenSymbol, ",") {
			break
		}
	}
	return node, nil
}

// whereClause parses a WHERE clause
func (p *Parser) whereClause() (ASTNode, error) {
	if !p.expect(TokenKeyword, "WHERE") {
		return ASTNode{}, fmt.Errorf("expected WHERE at position %d", p.pos)
	}
	node := ASTNode{Type: NodeWhere}
	expr, err := p.expression()
	if err != nil {
		return ASTNode{}, err
	}
	node.Children = append(node.Children, expr)
	return node, nil
}

// setClause parses a SET clause
func (p *Parser) setClause() (ASTNode, error) {
	if !p.expect(TokenKeyword, "SET") {
		return ASTNode{}, fmt.Errorf("expected SET at position %d", p.pos)
	}
	node := ASTNode{Type: NodeSet}
	for p.pos < len(p.tokens) && p.tokens[p.pos].Type != TokenEOF {
		if p.tokens[p.pos].Type == TokenKeyword {
			break
		}
		prop, err := p.propertyAssignment()
		if err != nil {
			return ASTNode{}, err
		}
		node.Children = append(node.Children, prop)
		if !p.accept(TokenSymbol, ",") {
			break
		}
	}
	return node, nil
}

// deleteClause parses a DELETE clause
func (p *Parser) deleteClause() (ASTNode, error) {
	if !p.expect(TokenKeyword, "DELETE") {
		return ASTNode{}, fmt.Errorf("expected DELETE at position %d", p.pos)
	}
	node := ASTNode{Type: NodeDelete}
	for p.pos < len(p.tokens) && p.tokens[p.pos].Type != TokenEOF {
		if p.tokens[p.pos].Type == TokenKeyword {
			break
		}
		if !p.expect(TokenIdentifier, "") {
			return ASTNode{}, fmt.Errorf("expected identifier at position %d", p.pos)
		}
		node.Children = append(node.Children, ASTNode{
			Type:  NodeIdentifier,
			Value: p.tokens[p.pos-1].Value,
		})
		if !p.accept(TokenSymbol, ",") {
			break
		}
	}
	return node, nil
}

// returnClause parses a RETURN clause
func (p *Parser) returnClause() (ASTNode, error) {
	if !p.expect(TokenKeyword, "RETURN") {
		return ASTNode{}, fmt.Errorf("expected RETURN at position %d", p.pos)
	}
	node := ASTNode{Type: NodeReturn}
	for p.pos < len(p.tokens) && p.tokens[p.pos].Type != TokenEOF {
		if p.tokens[p.pos].Type == TokenKeyword {
			break
		}
		if !p.expect(TokenIdentifier, "") {
			return ASTNode{}, fmt.Errorf("expected identifier at position %d", p.pos)
		}
		node.Children = append(node.Children, ASTNode{
			Type:  NodeIdentifier,
			Value: p.tokens[p.pos-1].Value,
		})
		if !p.accept(TokenSymbol, ",") {
			break
		}
	}
	return node, nil
}

// pattern parses a node or relationship pattern
func (p *Parser) pattern() (ASTNode, error) {
	node := ASTNode{Type: NodePattern}
	if p.accept(TokenSymbol, "(") {
		// Single node pattern
		nodeNode, err := p.node()
		if err != nil {
			return ASTNode{}, err
		}
		node.Children = append(node.Children, nodeNode)
		if !p.expect(TokenSymbol, ")") {
			return ASTNode{}, fmt.Errorf("expected ) at position %d", p.pos)
		}
	} else {
		return ASTNode{}, fmt.Errorf("expected ( at position %d", p.pos)
	}

	// Check for relationship pattern
	if p.accept(TokenSymbol, "-") {
		rel, err := p.relationship()
		if err != nil {
			return ASTNode{}, err
		}
		node.Children = append(node.Children, rel)
		if !p.expect(TokenSymbol, "(") {
			return ASTNode{}, fmt.Errorf("expected ( after relationship at position %d", p.pos)
		}
		nodeNode, err := p.node()
		if err != nil {
			return ASTNode{}, err
		}
		node.Children = append(node.Children, nodeNode)
		if !p.expect(TokenSymbol, ")") {
			return ASTNode{}, fmt.Errorf("expected ) at position %d", p.pos)
		}
	}
	return node, nil
}

// node parses a node pattern (e.g., (n:Label {key: value}))
func (p *Parser) node() (ASTNode, error) {
	node := ASTNode{Type: NodeNode}
	if p.accept(TokenIdentifier, "") {
		node.Value = p.tokens[p.pos-1].Value
	}
	if p.accept(TokenSymbol, ":") {
		if !p.expect(TokenIdentifier, "") {
			return ASTNode{}, fmt.Errorf("expected label after : at position %d", p.pos)
		}
		node.Children = append(node.Children, ASTNode{
			Type:  NodeLabel,
			Value: p.tokens[p.pos-1].Value,
		})
	}
	if p.accept(TokenSymbol, "{") {
		for p.pos < len(p.tokens) && p.tokens[p.pos].Value != "}" {
			prop, err := p.property()
			if err != nil {
				return ASTNode{}, err
			}
			node.Children = append(node.Children, prop)
			if !p.accept(TokenSymbol, ",") {
				break
			}
		}
		if !p.expect(TokenSymbol, "}") {
			return ASTNode{}, fmt.Errorf("expected } at position %d", p.pos)
		}
	}
	return node, nil
}

// relationship parses a relationship pattern (e.g., [:RELATION])
func (p *Parser) relationship() (ASTNode, error) {
	node := ASTNode{Type: NodeRelationship}
	if !p.expect(TokenSymbol, "[") {
		return ASTNode{}, fmt.Errorf("expected [ at position %d", p.pos)
	}
	if p.accept(TokenIdentifier, "") {
		node.Value = p.tokens[p.pos-1].Value
	}
	if p.accept(TokenSymbol, ":") {
		if !p.expect(TokenIdentifier, "") {
			return ASTNode{}, fmt.Errorf("expected relationship type after : at position %d", p.pos)
		}
		node.Children = append(node.Children, ASTNode{
			Type:  NodeType,
			Value: p.tokens[p.pos-1].Value,
		})
	}
	if p.accept(TokenSymbol, "{") {
		for p.pos < len(p.tokens) && p.tokens[p.pos].Value != "}" {
			prop, err := p.property()
			if err != nil {
				return ASTNode{}, err
			}
			node.Children = append(node.Children, prop)
			if !p.accept(TokenSymbol, ",") {
				break
			}
		}
		if !p.expect(TokenSymbol, "}") {
			return ASTNode{}, fmt.Errorf("expected } at position %d", p.pos)
		}
	}
	if !p.expect(TokenSymbol, "]") {
		return ASTNode{}, fmt.Errorf("expected ] at position %d", p.pos)
	}
	if !p.expect(TokenSymbol, "->") {
		return ASTNode{}, fmt.Errorf("expected -> at position %d", p.pos)
	}
	return node, nil
}

// property parses a property key-value pair
func (p *Parser) property() (ASTNode, error) {
	if !p.expect(TokenIdentifier, "") {
		return ASTNode{}, fmt.Errorf("expected property key at position %d", p.pos)
	}
	key := p.tokens[p.pos-1].Value
	if !p.expect(TokenSymbol, ":") {
		return ASTNode{}, fmt.Errorf("expected : after property key at position %d", p.pos)
	}
	var value string
	var propType PropertyType
	switch p.tokens[p.pos].Type {
	case TokenString:
		value = p.tokens[p.pos].Value
		propType = PropertyString
		p.pos++
	case TokenNumber:
		value = p.tokens[p.pos].Value
		propType = PropertyInt
		p.pos++
	case TokenIdentifier:
		if strings.ToLower(p.tokens[p.pos].Value) == "true" || strings.ToLower(p.tokens[p.pos].Value) == "false" {
			value = p.tokens[p.pos].Value
			propType = PropertyBool
			p.pos++
		} else {
			return ASTNode{}, fmt.Errorf("invalid property value at position %d", p.pos)
		}
	default:
		return ASTNode{}, fmt.Errorf("expected property value at position %d", p.pos)
	}
	return ASTNode{
		Type: NodeProperty,
		Children: []ASTNode{
			{Type: NodeIdentifier, Value: key},
			{Type: NodeLiteral, Value: value, Children: []ASTNode{{Type: NodeLiteral, Value: propType.String()}}},
		},
	}, nil
}

// propertyAssignment parses a SET property assignment (e.g., n.key = value)
func (p *Parser) propertyAssignment() (ASTNode, error) {
	if !p.expect(TokenIdentifier, "") {
		return ASTNode{}, fmt.Errorf("expected identifier at position %d", p.pos)
	}
	varNode := p.tokens[p.pos-1].Value
	if !p.expect(TokenSymbol, ".") {
		return ASTNode{}, fmt.Errorf("expected . at position %d", p.pos)
	}
	if !p.expect(TokenIdentifier, "") {
		return ASTNode{}, fmt.Errorf("expected property key at position %d", p.pos)
	}
	key := p.tokens[p.pos-1].Value
	if !p.expect(TokenSymbol, "=") {
		return ASTNode{}, fmt.Errorf("expected = at position %d", p.pos)
	}
	var value string
	var propType PropertyType
	switch p.tokens[p.pos].Type {
	case TokenString:
		value = p.tokens[p.pos].Value
		propType = PropertyString
		p.pos++
	case TokenNumber:
		value = p.tokens[p.pos].Value
		propType = PropertyInt
		p.pos++
	case TokenIdentifier:
		if strings.ToLower(p.tokens[p.pos].Value) == "true" || strings.ToLower(p.tokens[p.pos].Value) == "false" {
			value = p.tokens[p.pos].Value
			propType = PropertyBool
			p.pos++
		} else {
			return ASTNode{}, fmt.Errorf("invalid property value at position %d", p.pos)
		}
	default:
		return ASTNode{}, fmt.Errorf("expected property value at position %d", p.pos)
	}
	return ASTNode{
		Type: NodeProperty,
		Children: []ASTNode{
			{Type: NodeIdentifier, Value: varNode},
			{Type: NodeIdentifier, Value: key},
			{Type: NodeLiteral, Value: value, Children: []ASTNode{{Type: NodeLiteral, Value: propType.String()}}},
		},
	}, nil
}

// expression parses a WHERE expression (e.g., n.key = value)
func (p *Parser) expression() (ASTNode, error) {
	if !p.expect(TokenIdentifier, "") {
		return ASTNode{}, fmt.Errorf("expected identifier at position %d", p.pos)
	}
	varNode := p.tokens[p.pos-1].Value
	if !p.expect(TokenSymbol, ".") {
		return ASTNode{}, fmt.Errorf("expected . at position %d", p.pos)
	}
	if !p.expect(TokenIdentifier, "") {
		return ASTNode{}, fmt.Errorf("expected property key at position %d", p.pos)
	}
	key := p.tokens[p.pos-1].Value
	if !p.expect(TokenSymbol, "=") {
		return ASTNode{}, fmt.Errorf("expected = at position %d", p.pos)
	}
	var value string
	var propType PropertyType
	switch p.tokens[p.pos].Type {
	case TokenString:
		value = p.tokens[p.pos].Value
		propType = PropertyString
		p.pos++
	case TokenNumber:
		value = p.tokens[p.pos].Value
		propType = PropertyInt
		p.pos++
	case TokenIdentifier:
		if strings.ToLower(p.tokens[p.pos].Value) == "true" || strings.ToLower(p.tokens[p.pos].Value) == "false" {
			value = p.tokens[p.pos].Value
			propType = PropertyBool
			p.pos++
		} else {
			return ASTNode{}, fmt.Errorf("invalid expression value at position %d", p.pos)
		}
	default:
		return ASTNode{}, fmt.Errorf("expected expression value at position %d", p.pos)
	}
	return ASTNode{
		Type: NodeExpression,
		Children: []ASTNode{
			{Type: NodeIdentifier, Value: varNode},
			{Type: NodeIdentifier, Value: key},
			{Type: NodeLiteral, Value: value, Children: []ASTNode{{Type: NodeLiteral, Value: propType.String()}}},
		},
	}, nil
}

// expect checks and consumes a token
func (p *Parser) expect(tokenType TokenType, value string) bool {
	if p.pos >= len(p.tokens) {
		return false
	}
	current := p.tokens[p.pos]
	if tokenType == TokenIdentifier && value == "" {
		if current.Type == tokenType {
			p.pos++
			return true
		}
		return false
	}
	if tokenType == TokenKeyword {
		if current.Type == tokenType && strings.ToUpper(current.Value) == value {
			p.pos++
			return true
		}
		return false
	}
	if current.Type == tokenType && current.Value == value {
		p.pos++
		return true
	}
	return false
}

// accept checks and optionally consumes a token
func (p *Parser) accept(tokenType TokenType, value string) bool {
	if p.expect(tokenType, value) {
		return true
	}
	return false
}

// PropertyType.String converts PropertyType to string for AST
func (pt PropertyType) String() string {
	switch pt {
	case PropertyInt:
		return "int"
	case PropertyString:
		return "string"
	case PropertyBool:
		return "bool"
	default:
		return "unknown"
	}
}
