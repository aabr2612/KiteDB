package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"graphdb/graphdb"

	"github.com/sirupsen/logrus"
)

// replState holds the state of the REPL
type replState struct {
	db        *graphdb.GraphDB
	dbName    string
	dbDir     string
	logger    *logrus.Logger
	queryNum  int
	isRunning bool
}

// newReplState initializes the REPL state
func newReplState() *replState {
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)
	return &replState{
		db:        nil,
		dbName:    "",
		dbDir:     "databases",
		logger:    logger,
		queryNum:  0,
		isRunning: true,
	}
}

// initializeDB initializes a GraphDB instance for the given database name
func (rs *replState) initializeDB(dbName string) error {
	dbPath := filepath.Join(rs.dbDir, dbName+".db")
	db, err := graphdb.NewGraphDB(dbPath, 4096, 100)
	if err != nil {
		return fmt.Errorf("failed to initialize database %s: %v", dbName, err)
	}
	rs.db = db
	rs.dbName = dbName
	rs.logger.WithField("component", "Main").Infof("Using database: %s", dbName)
	return nil
}

// createDatabase creates a new database file
func (rs *replState) createDatabase(dbName string) error {
	if _, err := os.Stat(rs.dbDir); os.IsNotExist(err) {
		if err := os.Mkdir(rs.dbDir, 0755); err != nil {
			return fmt.Errorf("failed to create databases directory: %v", err)
		}
	}
	dbPath := filepath.Join(rs.dbDir, dbName+".db")
	if _, err := os.Stat(dbPath); err == nil {
		return fmt.Errorf("database %s already exists", dbName)
	}
	file, err := os.Create(dbPath)
	if err != nil {
		return fmt.Errorf("failed to create database %s: %v", dbName, err)
	}
	file.Close()
	rs.logger.WithField("component", "Main").Infof("Created database: %s", dbName)
	return nil
}

// useDatabase switches to the specified database
func (rs *replState) useDatabase(dbName string) error {
	dbPath := filepath.Join(rs.dbDir, dbName+".db")
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		return fmt.Errorf("database %s does not exist", dbName)
	}
	if rs.db != nil {
		rs.db.Close()
	}
	return rs.initializeDB(dbName)
}

// showDatabases lists all databases
func (rs *replState) showDatabases() ([]string, error) {
	if _, err := os.Stat(rs.dbDir); os.IsNotExist(err) {
		return []string{}, nil
	}
	files, err := os.ReadDir(rs.dbDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read databases directory: %v", err)
	}
	var dbs []string
	for _, file := range files {
		if !file.IsDir() && strings.HasSuffix(file.Name(), ".db") {
			dbs = append(dbs, strings.TrimSuffix(file.Name(), ".db"))
		}
	}
	return dbs, nil
}

// dropDatabase deletes the specified database
func (rs *replState) dropDatabase(dbName string) error {
	dbPath := filepath.Join(rs.dbDir, dbName+".db")
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		return fmt.Errorf("database %s does not exist", dbName)
	}
	if rs.dbName == dbName {
		rs.db.Close()
		rs.db = nil
		rs.dbName = ""
	}
	if err := os.Remove(dbPath); err != nil {
		return fmt.Errorf("failed to drop database %s: %v", dbName, err)
	}
	rs.logger.WithField("component", "Main").Infof("Dropped database: %s", dbName)
	return nil
}

// showNodes lists all nodes with the Person label in the current database
func (rs *replState) showNodes() error {
	if rs.db == nil {
		return fmt.Errorf("no database selected; use 'USE DATABASE <name>'")
	}
	fmt.Println("Note: Only nodes with label 'Person' are shown (MATCH (n) not supported)")
	query := "MATCH (n:Person) RETURN n"
	results, err := rs.db.ExecuteQuery(query)
	if err != nil {
		rs.logger.WithError(err).Error("Failed to execute SHOW NODES query")
		return fmt.Errorf("failed to show nodes: %v", err)
	}
	if len(results) == 0 {
		fmt.Println("No Person nodes found")
	} else {
		fmt.Println("Nodes (Person label):")
		for _, result := range results {
			node := result["n"].(map[string]interface{})
			id := node["id"]
			labels := node["labels"]
			properties := node["properties"]
			fmt.Printf("  ID: %v, Labels: %v, Properties: %v\n", id, labels, properties)
		}
	}
	return nil
}

// showEdges lists all edges in the current database
func (rs *replState) showEdges() error {
	if rs.db == nil {
		return fmt.Errorf("no database selected; use 'USE DATABASE <name>'")
	}
	// Placeholder until edge queries are supported
	fmt.Println("Edges: Not supported yet (edge queries require parser/executor updates)")
	return nil
}

// describeDatabase shows metadata about the current database
func (rs *replState) describeDatabase() error {
	if rs.db == nil {
		return fmt.Errorf("no database selected; use 'USE DATABASE <name>'")
	}
	fmt.Println("Note: Only nodes with label 'Person' are counted (MATCH (n) not supported)")
	query := "MATCH (n:Person) RETURN n"
	results, err := rs.db.ExecuteQuery(query)
	if err != nil {
		rs.logger.WithError(err).Error("Failed to execute DESCRIBE DATABASE query")
		return fmt.Errorf("failed to describe database: %v", err)
	}
	nodeCount := len(results)
	// Edge count is placeholder (0) until edge support is added
	edgeCount := 0
	fmt.Printf("Database: %s\n", rs.dbName)
	fmt.Printf("  Node Count (Person label): %d\n", nodeCount)
	fmt.Printf("  Edge Count: %d\n", edgeCount)
	fmt.Printf("  Page Size: %d bytes\n", 4096)
	fmt.Printf("  Buffer Capacity: %d pages\n", 100)
	return nil
}

// clearDatabase deletes all nodes with the Person label in the current database
func (rs *replState) clearDatabase() error {
	if rs.db == nil {
		return fmt.Errorf("no database selected; use 'USE DATABASE <name>'")
	}
	fmt.Println("Note: Only nodes with label 'Person' are deleted (MATCH (n) not supported)")
	query := "MATCH (n:Person) DELETE n"
	_, err := rs.db.ExecuteQuery(query)
	if err != nil {
		rs.logger.WithError(err).Error("Failed to execute CLEAR DATABASE query")
		return fmt.Errorf("failed to clear database: %v", err)
	}
	rs.logger.WithField("component", "Main").Info("Cleared Person nodes from database")
	fmt.Println("Person nodes cleared from database")
	return nil
}

// executeQuery executes a Cypher query
func (rs *replState) executeQuery(query string) error {
	if rs.db == nil {
		return fmt.Errorf("no database selected; use 'USE DATABASE <name>'")
	}
	rs.queryNum++
	log := rs.logger.WithFields(logrus.Fields{
		"component": "Main",
		"query":     query,
		"query_num": rs.queryNum,
	})
	log.Info("Executing query")
	results, err := rs.db.ExecuteQuery(query)
	if err != nil {
		log.WithError(err).Error("Failed to execute query")
		return fmt.Errorf("query execution failed: %v", err)
	}
	if len(results) > 0 {
		fmt.Println("Results:")
		for _, result := range results {
			fmt.Printf("%v\n", result)
		}
		log.Infof("Query executed successfully; results: %v", results)
	} else {
		fmt.Println("No results returned")
		log.Info("Query executed successfully; no results")
	}
	return nil
}

// printHelp displays the help message
func (rs *replState) printHelp() {
	fmt.Println("GraphDB REPL Commands:")
	fmt.Println("  .help                     Show this help message")
	fmt.Println("  .exit                     Exit the REPL")
	fmt.Println("  CREATE DATABASE <name>    Create a new database")
	fmt.Println("  USE DATABASE <name>       Switch to the specified database")
	fmt.Println("  SHOW DATABASES            List all databases")
	fmt.Println("  DROP DATABASE <name>      Delete the specified database")
	fmt.Println("  SHOW NODES                List all nodes with Person label")
	fmt.Println("  SHOW EDGES                List all edges (not supported yet)")
	fmt.Println("  DESCRIBE DATABASE         Show database metadata (Person nodes only)")
	fmt.Println("  CLEAR DATABASE            Delete all Person nodes")
	fmt.Println("Cypher Queries:")
	fmt.Println("  CREATE (n:Person {name: \"Alice\", age: 30})")
	fmt.Println("  MATCH (n:Person) WHERE n.name = \"Alice\" RETURN n")
	fmt.Println("  MATCH (n:Person) SET n.age = 31")
	fmt.Println("  MATCH (n:Person) DELETE n")
	fmt.Println("Type '.exit' or 'quit' to exit.")
}

// processCommand processes a REPL command or query
func (rs *replState) processCommand(input string) error {
	input = strings.TrimSpace(input)
	if input == "" {
		return nil
	}

	// Handle REPL commands
	if strings.HasPrefix(input, ".") {
		command := strings.ToLower(input)
		switch command {
		case ".help":
			rs.printHelp()
			return nil
		case ".exit", "quit":
			rs.isRunning = false
			return nil
		default:
			return fmt.Errorf("unknown command: %s; type '.help' for assistance", input)
		}
	}

	// Handle database commands
	lowerInput := strings.ToLower(input)
	if strings.HasPrefix(lowerInput, "create database ") {
		dbName := strings.TrimSpace(input[15:])
		if dbName == "" {
			return fmt.Errorf("database name required")
		}
		return rs.createDatabase(dbName)
	}
	if strings.HasPrefix(lowerInput, "use database ") {
		dbName := strings.TrimSpace(input[13:])
		if dbName == "" {
			return fmt.Errorf("database name required")
		}
		return rs.useDatabase(dbName)
	}
	if lowerInput == "show databases" {
		dbs, err := rs.showDatabases()
		if err != nil {
			return err
		}
		if len(dbs) == 0 {
			fmt.Println("No databases found")
		} else {
			fmt.Println("Databases:")
			for _, db := range dbs {
				fmt.Printf("  %s\n", db)
			}
		}
		return nil
	}
	if strings.HasPrefix(lowerInput, "drop database ") {
		dbName := strings.TrimSpace(input[14:])
		if dbName == "" {
			return fmt.Errorf("database name required")
		}
		return rs.dropDatabase(dbName)
	}
	if lowerInput == "show nodes" {
		return rs.showNodes()
	}
	if lowerInput == "show edges" {
		return rs.showEdges()
	}
	if lowerInput == "describe database" {
		return rs.describeDatabase()
	}
	if lowerInput == "clear database" {
		return rs.clearDatabase()
	}

	// Handle Cypher queries
	return rs.executeQuery(input)
}

// runREPL runs the REPL loop
func (rs *replState) runREPL() {
	rs.logger.WithField("component", "Main").Info("Starting GraphDB REPL")
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Println("Welcome to GraphDB REPL. Type '.help' for commands or 'quit' to exit.")

	for rs.isRunning {
		prompt := "graphdb"
		if rs.dbName != "" {
			prompt = fmt.Sprintf("graphdb(%s)", rs.dbName)
		}
		fmt.Printf("%s> ", prompt)
		if !scanner.Scan() {
			break
		}
		input := scanner.Text()
		if err := rs.processCommand(input); err != nil {
			fmt.Printf("Error: %v\n", err)
		}
	}

	if rs.db != nil {
		rs.db.Close()
	}
	fmt.Println("Goodbye!")
}

func main() {
	rs := newReplState()
	rs.runREPL()
}
