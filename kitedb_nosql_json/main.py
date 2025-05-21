import os
import json
import getpass
from src.core.database import Database
from src.query.query_parser import QueryParser
from src.config import logger
from src.core.exceptions import ValidationError, KiteDBError, TransactionError
from src.config import config

HELP_MESSAGE = """
KiteDB v2.0 Help
================

KiteDB is a NoSQL JSON database supporting collections, transactions, and complex queries.

General Commands
---------------
- login <username>                  Authenticate user (e.g., 'login admin', default password: 'password123').
- use <database_name>               Switch to a database (e.g., 'use mydb'). Creates if it doesn't exist.
- list                              List all databases in the storage root.
- create <collection_name> [schema] Create a collection with an optional JSON schema (e.g., 'create users {"validator": {"$jsonSchema": {"bsonType": "object", "required": ["name"]}}}').
- delete <collection_name>          Delete a collection (e.g., 'delete users').
- begin                             Start a transaction for atomic operations.
- commit                            Commit the current transaction.
- rollback                          Roll back the current transaction.
- exit                              Exit the KiteDB console.
- help                              Display this help message.

Collection Operations
--------------------
Format: <collection>.<operation>{<parameters>}
Parameters must be valid JSON objects or arrays, enclosed in curly braces {}.

1. add{<document> | [<document>, ...]}
   - Add one or more documents to a collection.
   - Single document: JSON object (e.g., collection_name.add{{"name": "Alice", "age": 25}}).
   - Multiple documents: Array of JSON objects (e.g., collection_name.add{[{"name": 1}, {"name": 2}]}).
   - Example: users.add{{"name": "Alice Smith", "age": 28, "address": {"city": "San Francisco"}}}
   - Example: users.add{[{"name": "Bob", "age": 34}, {"name": "Clara", "age": 25}]}
   - Note: Documents must match the collection's schema (if defined). Empty arrays are invalid.

2. find{<query>}
   - Find documents matching the query.
   - Query: JSON object with field-value pairs or operators (e.g., collection_name.find{{"name": "Alice"}, {"age": {"$gt": 25}}}).
   - Example: users.find{"address.city": "San Francisco", "age": {"$gte": 25, "$lt": 35}}
   - Example: users.find{"$or": [{"name": "Alice Smith"}, {"$and": [{"hobbies": "hiking"}, {"address.country": "USA"}]}]}
   - Note: Empty query {} returns all documents.

3. update{query_column, update_column, update_column, ...}
   - Update documents matching the query column with the specified update columns.
   - Format: Single JSON object where the first field is the query condition, and remaining fields are updates applied via $set.
   - Example: users.update{"name":1, "age":20, "class":1}
     - Query: {"name":1}
     - Update: {"$set": {"age":20, "class":1}}
   - Example: users.update{"name":"Alice Smith", "age":29, "status":"active"}
     - Query: {"name":"Alice Smith"}
     - Update: {"$set": {"age":29, "status":"active"}}
   - Example with nested fields: users.update{"name":1, "age":20, "address":{"zip":20, "ali":30}}
     - Query: {"name":1}
     - Update: {"$set": {"age":20, "address":{"zip":20, "ali":30}}}
   - Alternative Format: collection_name.update{<query>, <update> | [<update>, ...]}
     - Query: JSON object to select documents (e.g., {"name": "Bob"}).
     - Update: JSON object (e.g., {"$set": {"age": 35}}) or array of objects (e.g., [{"$set": {"role": "senior"}}, {"$inc": {"age": 1}}]).
     - Example: users.update{{"name": "Alice Smith"}, {"$set": {"status": "active"}, "$push": {"hobbies": "gardening"}}}
     - Example: users.update{{"age": {"$gte": 30}}, [{"$set": {"role": "senior"}}, {"$inc": {"scores.math": 5}}]}
   - Note: Ensure the query is precise, as multiple documents may match (e.g., multiple {"name": 1}). At least two fields are required for single-object syntax.

4. delete{<query>}
   - Delete documents matching the query.
   - Query: JSON object (e.g., collection_name.delete{"name": "Bob"}).
   - Example: users.delete{"$and": [{"address.country": "UK"}, {"scores.history": {"$gte": 80}}]}
   - Example: users.delete{}
   - Note: Empty query {} deletes all documents in the collection.

Supported Query Operators
------------------------
- Comparison: $eq (equal), $ne (not equal), $gt (greater than), $gte (greater than or equal), $lt (less than), $lte (less than or equal).
  - Example: {"age": {"$gt": 25, "$lte": 35}}
- Logical: $and, $or, $not
  - Example: {"$or": [{"name": "Alice"}, {"age": {"$gte": 30}}]}
- Dot notation for nested fields: e.g., "address.city": "San Francisco"

Schema Considerations
---------------------
- Collections may have a schema to enforce document structure (e.g., required fields, types).
- Example schema: {"validator": {"$jsonSchema": {"bsonType": "object", "required": ["name"], "properties": {"name": {"bsonType": ["string", "int"]}, "age": {"bsonType": ["int", "null"]}}}}}
- Common errors:
  - "Validation error: Document must be a dictionary": Ensure add payloads are valid JSON objects or arrays of objects.
  - Schema mismatch: Check that documents match required fields and types (e.g., "name" must be string or int).

Error Handling
--------------
- Invalid JSON: Ensure proper JSON syntax (e.g., use double quotes, correct brackets).
- Invalid query: Queries must be JSON objects (e.g., {"name": "Alice"}, not []).
- Transaction errors: Use begin, commit, rollback for atomic operations.
- Example: users.add{invalid} -> "Validation error: Invalid JSON"

Tips
----
- Use precise queries to avoid updating/deleting multiple documents (e.g., {"name": 1} may match multiple records).
- Verify updates with find: e.g., users.find{"name": "Alice Smith"} after users.update.
- Check database state with find{} to view all documents.
- Use transactions for critical operations: begin, then commit or rollback.

For further assistance, contact the KiteDB support team or check the documentation.
"""


class KiteDBConsole:
    def __init__(self):
        """Initialize the KiteDB console with default settings and user credentials."""
        self.current_db = None
        self.running = True
        self.authenticated = False
        self.users = {"admin": "password123"}

    def run(self):
        """Run the KiteDB console, handling user authentication and command input loop."""
        print("Welcome to KiteDB v2.0")
        while self.running:
            if not self.authenticated:
                self.handle_login()
                continue
            prompt = (
                f"kiteDB ({self.current_db.name}) > "
                if self.current_db
                else "kiteDB > "
            )
            try:
                cmd = input(prompt).strip()
                if not cmd:
                    continue
                self.handle_command(cmd)
            except EOFError:
                print("\nExiting KiteDB...")
                break
            except KeyboardInterrupt:
                print("\nOperation cancelled")
                continue
            except Exception as e:
                print(f"Unexpected error: {e}")
                logger.error(f"Console error: {e}")

    def handle_login(self):
        """Authenticate a user by prompting for username and password."""
        username = input("Username: ").strip()
        password = getpass.getpass("Password: ")
        if username in self.users and self.users[username] == password:
            self.authenticated = True
            print("Login successful")
            logger.info(f"User '{username}' logged in")
        else:
            print("Invalid credentials")
            logger.warning(f"Failed login attempt for '{username}'")

    def handle_command(self, cmd: str):
        """Parse and execute a database command or delegate to collection operations."""
        # Check for empty or whitespace-only commands to prevent unnecessary processing
        if not cmd or cmd.isspace():
            print("Error: Empty command")
            return
        parts = cmd.split(maxsplit=1)
        command = parts[0].lower()
        arg = parts[1] if len(parts) > 1 else ""
        if command in self.DB_COMMANDS:
            self.DB_COMMANDS[command](self, arg)
        else:
            self.handle_collection_operation(cmd)

    def handle_use(self, arg: str):
        """Switch to a specified database, creating it if it doesn't exist."""
        db_name = arg.strip()
        if not db_name:
            print("Usage: use <database_name>")
            return
        try:
            self.current_db = Database(db_name)
            db_path = os.path.abspath(self.current_db.storage.db_path)
            print(f"Database path: {db_path}")
            print(f"Switched to database '{db_name}'")
            logger.info(f"Switched to database '{db_name}' at '{db_path}'")
        except KiteDBError as e:
            print(f"Error: {e}")
            logger.error(f"Use database failed: {e}")

    def handle_list(self, arg: str):
        """List all available databases in the storage root directory."""
        try:
            databases = []
            data_root = config.get("storage.data_root")
            for entry in os.listdir(data_root):
                path = os.path.join(data_root, entry)
                if os.path.isdir(path):
                    databases.append(entry)
            databases.sort()
            if databases:
                print("Databases:")
                for db in databases:
                    print(f"  {db}")
            else:
                print("No databases found")
        except Exception as e:
            print(f"Error listing databases: {e}")
            logger.error(f"Error listing databases: {e}")

    def handle_create(self, arg: str):
        """Create a new collection in the current database with an optional schema."""
        if not self.current_db:
            print("Select a database first: use <name>")
            return
        parts = arg.strip().split(maxsplit=1)
        collection_name = parts[0]
        schema = parts[1] if len(parts) > 1 else None
        if not collection_name:
            print("Usage: create <collection_name> [schema]")
            return
        try:
            schema_dict = json.loads(schema) if schema else None
            self.current_db.create_collection(collection_name, schema_dict)
            print(f"Collection '{collection_name}' created")
            logger.info(
                f"Created collection '{collection_name}' in '{self.current_db.name}'"
            )
        except KiteDBError as e:
            print(f"Error: {e}")
            logger.error(f"Create collection failed: {e}")
        except json.JSONDecodeError as e:
            print(f"Invalid schema JSON: {e}")
            logger.error(f"Invalid schema JSON: {e}")

    def handle_delete(self, arg: str):
        """Delete a specified collection from the current database."""
        if not self.current_db:
            print("Select a database first: use <name>")
            return
        collection_name = arg.strip()
        if not collection_name:
            print("Usage: delete <collection_name>")
            return
        try:
            result = self.current_db.drop_collection(collection_name)
            if result == "logged":
                print("Collection deletion logged")
            else:
                print(f"Collection '{collection_name}' deleted")
            logger.info(
                f"Deleted collection '{collection_name}' from '{self.current_db.name}'"
            )
        except KiteDBError as e:
            print(f"Error: {e}")
            logger.error(f"Delete collection failed: {e}")

    def handle_begin(self, arg: str):
        """Start a new transaction for atomic operations in the current database."""
        if not self.current_db:
            print("Select a database first: use <name>")
            return
        try:
            self.current_db.begin_transaction()
            print("Transaction begun")
            logger.info(f"Transaction begun in '{self.current_db.name}'")
        except KiteDBError as e:
            print(f"Error: {e}")
            logger.error(f"Begin transaction failed: {e}")

    def handle_commit(self, arg: str):
        """Commit the active transaction in the current database."""
        if not self.current_db:
            print("Select a database first: use <name>")
            return
        if not self.current_db.transaction or not self.current_db.transaction.active:
            print("No active transaction")
            return
        try:
            self.current_db.transaction.commit()
            print("Transaction committed")
            logger.info(f"Transaction committed in '{self.current_db.name}'")
        except TransactionError as e:
            print(f"Error: {e}")
            logger.error(f"Commit transaction failed: {e}")

    def handle_rollback(self, arg: str):
        """Roll back the active transaction in the current database."""
        if not self.current_db:
            print("Select a database first: use <name>")
            return
        if not self.current_db.transaction or not self.current_db.transaction.active:
            print("No active transaction")
            return
        try:
            self.current_db.transaction.rollback()
            print("Transaction rolled back")
            logger.info(f"Transaction rolled back in '{self.current_db.name}'")
        except TransactionError as e:
            print(f"Error: {e}")
            logger.error(f"Rollback transaction failed: {e}")

    def handle_exit(self, arg: str):
        """Exit the KiteDB console, terminating the session."""
        self.running = False
        print("Exiting KiteDB...")
        logger.info("Console exited")

    def handle_help(self, arg: str):
        """Display the help message with available commands and usage details."""
        print(HELP_MESSAGE)

    def handle_collection_operation(self, cmd: str):
        """Handle collection-specific operations like add, find, update, or delete."""
        if not self.current_db:
            print("Select a database first: use <name>")
            return
        try:
            parsed = QueryParser.parse(cmd)
            op = parsed["operation"]
            collection_name = parsed["collection"]
            query = parsed.get("query", {})
            data = parsed.get("data", {})

            if op not in ["add", "find", "update", "delete"]:
                print(f"Unknown operation: {op}")
                logger.warning(f"Unknown operation attempted: {op}")
                return

            coll = self.current_db.get_collection(collection_name)
            logger.info(
                f"Executing {op} on '{collection_name}': query={query}, data={data}"
            )

            if op == "add":
                res = coll.insert(data)
                if res == "logged":
                    print("Insertion logged")
                else:
                    if isinstance(res, list):
                        print(f"Inserted {len(res)} documents with IDs: {res}")
                    else:
                        print(f"Inserted document with ID: {res}")
            elif op == "find":
                res = coll.find(query)
                if not res:
                    print("No documents found")
                else:
                    for doc in res:
                        print(json.dumps(doc, indent=2))
            elif op == "update":
                res = coll.update(query, data)
                if res == "logged":
                    print("Update logged")
                else:
                    print(f"Updated {res} documents")
            elif op == "delete":
                res = coll.delete(query)
                if res == "logged":
                    print("Delete logged")
                else:
                    print(f"Deleted {res} documents")

        except ValidationError as e:
            print(f"Validation error: {e}")
            logger.error(f"Validation error in command '{cmd}': {e}")
        except KiteDBError as e:
            print(f"Database error: {e}")
            logger.error(f"Database error in command '{cmd}': {e}")
        except Exception as e:
            print(f"Unexpected error: {e}")
            logger.error(f"Unexpected error in command '{cmd}': {e}")

    DB_COMMANDS = {
        "login": handle_login,
        "use": handle_use,
        "list": handle_list,
        "create": handle_create,
        "delete": handle_delete,
        "begin": handle_begin,
        "commit": handle_commit,
        "rollback": handle_rollback,
        "exit": handle_exit,
        "help": handle_help,
    }


if __name__ == "__main__":
    console = KiteDBConsole()
    console.run()
    