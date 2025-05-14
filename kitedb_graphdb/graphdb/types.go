package graphdb

// PropertyType defines supported property types
type PropertyType int

const (
	PropertyInt PropertyType = iota
	PropertyString
	PropertyBool
)

// Property represents a key-value pair
type Property struct {
	Key   string
	Value interface{}
	Type  PropertyType
}

// Node represents a graph node
type Node struct {
	ID         int64
	Labels     []string
	Properties []Property
	Active     bool
}

// Edge represents a graph edge
type Edge struct {
	ID         int64
	Type       string
	Source     int64
	Target     int64
	Properties []Property
	Active     bool
}

// ASTNodeType defines types for AST nodes
type ASTNodeType int

const (
	NodeQuery ASTNodeType = iota
	NodeCreate
	NodeMatch
	NodeWhere
	NodeSet
	NodeDelete
	NodeReturn
	NodePattern
	NodeNode
	NodeRelationship
	NodeLabel
	NodeType
	NodeIdentifier
	NodeProperty
	NodeLiteral
	NodeExpression
)

// ASTNode represents a node in the Abstract Syntax Tree
type ASTNode struct {
	Type     ASTNodeType
	Value    string
	Children []ASTNode
}
