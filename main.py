import os
import json
import getpass
from src.core.database import Database
from src.query.query_parser import QueryParser
from src.config import logger
from src.core.exceptions import ValidationError, KiteDBError, TransactionError
from src.config import config

HELP_MESSAGE = """
Available commands:
  login <username>          - Authenticate user
  use <database_name>       - Select a database
  list                      - List all databases
  create <collection_name> [schema] - Create a new collection
  delete <collection_name>  - Delete a collection
  begin                     - Begin a transaction
  commit                    - Commit the current transaction
  rollback                  - Rollback the current transaction
  exit                      - Exit the console
  help                      - Show this help message

Collection operations:
  <collection>.add{<document> | [<document>, ...]} - Insert one or more documents
  <collection>.find{<query>}                         - Find documents
  <collection>.update{<query>, <update> | [<update>, ...]} - Update documents with query and update(s)
  <collection>.delete{<query>}                       - Delete documents matching query

Supported query operators:
  $eq, $ne, $gt, $gte, $lt, $lte - Comparison operators
  $and, $or, $not               - Logical operators

Note: For update, use comma-separated query and update (e.g., users.update{{"name": {"$eq": "Alice"}}, {"age": 31}}).
"""

class KiteDBConsole:
    def __init__(self):
        self.current_db = None
        self.running = True
        self.authenticated = False
        self.users = {"admin": "password123"}

    def run(self):
        print("Welcome to KiteDB v2.0")
        while self.running:
            if not self.authenticated:
                self.handle_login()
                continue
            prompt = f"kiteDB ({self.current_db.name}) > " if self.current_db else "kiteDB > "
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
        parts = cmd.split(maxsplit=1)
        command = parts[0].lower()
        arg = parts[1] if len(parts) > 1 else ''
        if command in self.DB_COMMANDS:
            self.DB_COMMANDS[command](self, arg)
        else:
            self.handle_collection_operation(cmd)

    def handle_use(self, arg: str):
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
        try:
            databases = []
            data_root = config.get('storage.data_root')
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
            logger.info(f"Created collection '{collection_name}' in '{self.current_db.name}'")
        except KiteDBError as e:
            print(f"Error: {e}")
            logger.error(f"Create collection failed: {e}")
        except json.JSONDecodeError as e:
            print(f"Invalid schema JSON: {e}")
            logger.error(f"Invalid schema JSON: {e}")

    def handle_delete(self, arg: str):
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
            logger.info(f"Deleted collection '{collection_name}' from '{self.current_db.name}'")
        except KiteDBError as e:
            print(f"Error: {e}")
            logger.error(f"Delete collection failed: {e}")

    def handle_begin(self, arg: str):
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
        self.running = False
        print("Exiting KiteDB...")
        logger.info("Console exited")

    def handle_help(self, arg: str):
        print(HELP_MESSAGE)

    def handle_collection_operation(self, cmd: str):
        if not self.current_db:
            print("Select a database first: use <name>")
            return
        try:
            parsed = QueryParser.parse(cmd)
            op = parsed['operation']
            collection_name = parsed['collection']
            query = parsed.get('query', {})
            data = parsed.get('data', {})

            if op not in ['add', 'find', 'update', 'delete']:
                print(f"Unknown operation: {op}")
                logger.warning(f"Unknown operation attempted: {op}")
                return

            coll = self.current_db.get_collection(collection_name)
            logger.info(f"Executing {op} on '{collection_name}': query={query}, data={data}")

            if op == 'add':
                res = coll.insert(data)
                if res == "logged":
                    print("Insertion logged")
                else:
                    if isinstance(res, list):
                        print(f"Inserted {len(res)} documents with IDs: {res}")
                    else:
                        print(f"Inserted document with ID: {res}")
            elif op == 'find':
                res = coll.find(query)
                if not res:
                    print("No documents found")
                else:
                    for doc in res:
                        print(json.dumps(doc, indent=2))
            elif op == 'update':
                res = coll.update(query, data)
                if res == "logged":
                    print("Update logged")
                else:
                    print(f"Updated {res} documents")
            elif op == 'delete':
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
        'login': handle_login,
        'use': handle_use,
        'list': handle_list,
        'create': handle_create,
        'delete': handle_delete,
        'begin': handle_begin,
        'commit': handle_commit,
        'rollback': handle_rollback,
        'exit': handle_exit,
        'help': handle_help,
    }

if __name__ == '__main__':
    console = KiteDBConsole()
    console.run()