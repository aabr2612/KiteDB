package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"graphdb/graphdb"
)

// REPL manages the read-eval-print loop
type REPL struct {
	currentDB     *graphdb.GraphDB
	currentDBName string
	databases     map[string]*graphdb.GraphDB
}

// NewREPL initializes a new REPL
func NewREPL() *REPL {
	return &REPL{
		databases: make(map[string]*graphdb.GraphDB),
	}
}

// formatResults converts query results into a tree-like string
func formatResults(results []map[string]interface{}) string {
	if len(results) == 0 {
		return "No results returned"
	}

	var output strings.Builder
	output.WriteString("Results:\n")

	for i, result := range results {
		if i > 0 {
			output.WriteString("\n")
		}
		for varName, item := range result {
			itemMap, ok := item.(map[string]interface{})
			if !ok {
				output.WriteString(fmt.Sprintf("  %s: %v\n", varName, item))
				continue
			}

			// Determine if it's a node or edge
			isEdge := itemMap["type"] != nil
			if isEdge {
				output.WriteString(fmt.Sprintf("  Edge (%s):\n", varName))
				output.WriteString(fmt.Sprintf("    ID: %v\n", itemMap["id"]))
				output.WriteString(fmt.Sprintf("    Type: %v\n", itemMap["type"]))
				output.WriteString(fmt.Sprintf("    Source: %v\n", itemMap["source"]))
				output.WriteString(fmt.Sprintf("    Target: %v\n", itemMap["target"]))
			} else {
				output.WriteString(fmt.Sprintf("  Node (%s):\n", varName))
				output.WriteString(fmt.Sprintf("    ID: %v\n", itemMap["id"]))
				output.WriteString(fmt.Sprintf("    Labels: %v\n", itemMap["labels"]))
			}

			// Format properties
			props, ok := itemMap["properties"].([]graphdb.Property)
			if ok && len(props) > 0 {
				output.WriteString("    Properties:\n")
				for _, prop := range props {
					propType := ""
					switch prop.Type {
					case graphdb.PropertyInt:
						propType = "int"
					case graphdb.PropertyString:
						propType = "string"
					case graphdb.PropertyBool:
						propType = "bool"
					}
					output.WriteString(fmt.Sprintf("      - %s: %v (%s)\n", prop.Key, prop.Value, propType))
				}
			} else {
				output.WriteString("    Properties: []\n")
			}
		}
	}
	return output.String()
}

// help displays available commands
func (r *REPL) help() {
	fmt.Println(`GraphDB REPL Commands:
  .help                     Show this help message
  .exit                     Exit the REPL
  CREATE DATABASE <name>    Create a new database
  USE DATABASE <name>       Switch to the specified database
  SHOW DATABASES            List all databases
  DROP DATABASE <name>      Delete the specified database
  SHOW NODES                List all nodes with Person label
  SHOW EDGES                List all edges
  DESCRIBE DATABASE         Show database metadata (Person nodes and edges)
  CLEAR DATABASE            Delete all Person nodes and edges
Cypher Queries:
  CREATE (n:Person {name: "Alice", age: 30})
  CREATE (n:Person)-[:KNOWS {since: 2020}]->(m:Person)
  MATCH (n:Person) WHERE n.name = "Alice" RETURN n
  MATCH ()-[r:KNOWS]->() RETURN r
  MATCH (n:Person) SET n.age = 31
  MATCH (n:Person) DELETE n
  MATCH ()-[r:KNOWS]->() DELETE r
Type '.exit' or 'quit' to exit.`)
}

// run executes the REPL loop
func (r *REPL) run() {
	fmt.Println("Welcome to GraphDB REPL. Type '.help' for commands or 'quit' to exit.")

	scanner := bufio.NewScanner(os.Stdin)
	for {
		prompt := "graphdb"
		if r.currentDBName != "" {
			prompt += fmt.Sprintf("(%s)", r.currentDBName)
		}
		fmt.Printf("%s> ", prompt)

		if !scanner.Scan() {
			break
		}
		input := strings.TrimSpace(scanner.Text())
		if input == "" {
			continue
		}

		if input == ".exit" || input == "quit" {
			for name, db := range r.databases {
				if err := db.Close(); err != nil {
					fmt.Printf("Error: failed to close database %s: %v\n", name, err)
				}
			}
			fmt.Println("Goodbye!")
			return
		}

		if input == ".help" {
			r.help()
			continue
		}

		if strings.HasPrefix(input, "CREATE DATABASE ") {
			name := strings.TrimSpace(strings.TrimPrefix(input, "CREATE DATABASE "))
			if name == "" {
				fmt.Println("Error: database name required")
				continue
			}
			dbPath := filepath.Join("databases", name+".db")
			if err := os.MkdirAll("databases", 0755); err != nil {
				fmt.Printf("Error: failed to create databases directory: %v\n", err)
				continue
			}
			db, err := graphdb.NewGraphDB(dbPath, 4096, 100)
			if err != nil {
				fmt.Printf("Error: failed to create database: %v\n", err)
				continue
			}
			r.databases[name] = db
			fmt.Printf("Database %s created\n", name)
			continue
		}

		if strings.HasPrefix(input, "USE DATABASE ") {
			name := strings.TrimSpace(strings.TrimPrefix(input, "USE DATABASE "))
			if name == "" {
				fmt.Println("Error: database name required")
				continue
			}
			db, exists := r.databases[name]
			if !exists {
				dbPath := filepath.Join("databases", name+".db")
				var err error
				db, err = graphdb.NewGraphDB(dbPath, 4096, 100)
				if err != nil {
					fmt.Printf("Error: failed to open database: %v\n", err)
					continue
				}
				r.databases[name] = db
			}
			if r.currentDB != nil && r.currentDB != db {
				if err := r.currentDB.Close(); err != nil {
					fmt.Printf("Error: failed to close current database: %v\n", err)
				}
			}
			r.currentDB = db
			r.currentDBName = name
			fmt.Printf("Using database: %s\n", name)
			continue
		}

		if input == "SHOW DATABASES" {
			fmt.Println("Databases:")
			// Scan the databases folder for .db files
			files, err := os.ReadDir("databases")
			if err != nil {
				fmt.Printf("Error: failed to read databases directory: %v\n", err)
				continue
			}
			found := false
			for _, file := range files {
				if !file.IsDir() && strings.HasSuffix(file.Name(), ".db") {
					dbName := strings.TrimSuffix(file.Name(), ".db")
					fmt.Printf("  %s\n", dbName)
					found = true
				}
			}
			if !found {
				fmt.Println("  (none)")
			}
			continue
		}

		if strings.HasPrefix(input, "DROP DATABASE ") {
			name := strings.TrimSpace(strings.TrimPrefix(input, "DROP DATABASE "))
			if name == "" {
				fmt.Println("Error: database name required")
				continue
			}
			db, exists := r.databases[name]
			if exists {
				if err := db.Close(); err != nil {
					fmt.Printf("Error: failed to close database: %v\n", err)
					continue
				}
				delete(r.databases, name)
			}
			dbPath := filepath.Join("databases", name+".db")
			if _, err := os.Stat(dbPath); os.IsNotExist(err) {
				fmt.Printf("Error: database %s does not exist\n", name)
				continue
			}
			if err := os.Remove(dbPath); err != nil {
				fmt.Printf("Error: failed to delete database file: %v\n", err)
				continue
			}
			if r.currentDBName == name {
				r.currentDB = nil
				r.currentDBName = ""
			}
			fmt.Printf("Database %s dropped\n", name)
			continue
		}

		if input == "SHOW NODES" {
			if r.currentDB == nil {
				fmt.Println("Error: no database selected; use 'USE DATABASE <name>'")
				continue
			}
			fmt.Println("Note: Only nodes with label 'Person' are shown (MATCH (n) not supported)")
			results, err := r.currentDB.ExecuteQuery("MATCH (n:Person) RETURN n")
			if err != nil {
				fmt.Printf("Error: query execution failed: %v\n", err)
				continue
			}
			fmt.Println("Query Successful")
			fmt.Println(formatResults(results))
			continue
		}

		if input == "SHOW EDGES" {
			if r.currentDB == nil {
				fmt.Println("Error: no database selected; use 'USE DATABASE <name>'")
				continue
			}
			results, err := r.currentDB.ExecuteQuery("MATCH ()-[r]->() RETURN r")
			if err != nil {
				fmt.Printf("Error: query execution failed: %v\n", err)
				continue
			}
			fmt.Println("Query Successful")
			fmt.Println(formatResults(results))
			continue
		}

		if input == "DESCRIBE DATABASE" {
			if r.currentDB == nil {
				fmt.Println("Error: no database selected; use 'USE DATABASE <name>'")
				continue
			}
			nodeResults, err := r.currentDB.ExecuteQuery("MATCH (n:Person) RETURN n")
			if err != nil {
				fmt.Printf("Error: failed to fetch nodes: %v\n", err)
				continue
			}
			edgeResults, err := r.currentDB.ExecuteQuery("MATCH ()-[r]->() RETURN r")
			if err != nil {
				fmt.Printf("Error: failed to fetch edges: %v\n", err)
				continue
			}
			fmt.Printf("Database %s:\n", r.currentDBName)
			fmt.Printf("  Nodes (Person label): %d\n", len(nodeResults))
			fmt.Printf("  Edges: %d\n", len(edgeResults))
			continue
		}

		if input == "CLEAR DATABASE" {
			if r.currentDB == nil {
				fmt.Println("Error: no database selected; use 'USE DATABASE <name>'")
				continue
			}
			_, err := r.currentDB.ExecuteQuery("MATCH (n:Person) DELETE n")
			if err != nil {
				fmt.Printf("Error: failed to delete nodes: %v\n", err)
				continue
			}
			_, err = r.currentDB.ExecuteQuery("MATCH ()-[r]->() DELETE r")
			if err != nil {
				fmt.Printf("Error: failed to delete edges: %v\n", err)
				continue
			}
			fmt.Println("Database cleared")
			continue
		}

		// Handle Cypher queries
		if r.currentDB == nil {
			fmt.Println("Error: no database selected; use 'USE DATABASE <name>'")
			continue
		}

		results, err := r.currentDB.ExecuteQuery(input)
		if err != nil {
			fmt.Printf("Error: query execution failed: %v\n", err)
			continue
		}
		fmt.Println("Query Successful")
		fmt.Println(formatResults(results))
	}

	if err := scanner.Err(); err != nil {
		fmt.Printf("Error: failed to read input: %v\n", err)
	}
}

func main() {
	repl := NewREPL()
	repl.run()
}
