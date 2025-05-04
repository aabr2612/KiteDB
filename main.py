import json
import os
from typing import Any, Dict
from src.core.database import Database
from src.query.query_parser import QueryParser
from src.logging.logger import logger
from src.core.exceptions import ValidationError, KiteDBError, TransactionError
from src.config import DATA_ROOT  # Import DATA_ROOT for listing databases

HELP_MESSAGE = """
Available commands:
  use <database_name>       - Select a database to use
  show databases           - List all databases
  create <collection_name>  - Create a new collection in the current database
  begin                     - Begin a transaction
  commit                    - Commit the current transaction
  rollback                  - Rollback the current transaction
  exit                      - Exit the console
  help                      - Show this help message

Collection operations:
  <collection>.insert{<document>}       - Insert a document into the collection
  <collection>.find{<query>}            - Find documents matching the query
  <collection>.update{<query> <update>} - Update documents matching the query
  <collection>.delete{<query>}          - Delete documents matching the query

Supported query operators:
  $eq, $ne, $gt, $gte, $lt, $lte - Comparison operators
  $and, $or                      - Logical operators

Examples:
  use mydb
  create users
  users.insert{"name": "ALI", "age": 25}
  users.find{"age": {"$gte": 18}}
  users.update{"name": "ALI"} {"age": 26}
  users.delete{"name": "ALI"}
"""

class KiteDBConsole:
    def __init__(self):
        self.current_db = None
        self.running = True

    def run(self):
        print("Welcome to KiteDB v1.0")
        while self.running:
            prompt = f"kiteDB ({self.current_db.name}) > " if self.current_db else "kiteDB > "
            try:
                cmd = input(prompt).strip()
                if not cmd:
                    continue
                self.handle_command(cmd)
            except EOFError:
                print("\nExiting KiteDB...")
                break
            except Exception as e:
                print(f"Unexpected error: {e}")
                logger.error(f"Console error: {e}")

    def handle_command(self, cmd: str):
        parts = cmd.split(maxsplit=1)
        command = parts[0].lower()  # Make command case-insensitive
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

    def handle_show(self, arg: str):
        arg = arg.lower().strip()
        if arg == 'databases':
            try:
                databases = []
                for entry in os.listdir(DATA_ROOT):
                    path = os.path.join(DATA_ROOT, entry)
                    if os.path.isdir(path):
                        try:
                            chunks = [f for f in os.listdir(path) if f.startswith('chunk_') and f.endswith('.json')]
                            if chunks:
                                databases.append(entry)
                        except Exception as e:
                            logger.warning(f"Error checking directory '{path}': {e}")
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
        else:
            print(f"Unknown show command: {arg}")

    def handle_create(self, arg: str):
        if not self.current_db:
            print("Select a database first: use <name>")
            return
        collection_name = arg.strip()
        if not collection_name:
            print("Usage: create <collection_name>")
            return
        try:
            self.current_db.create_collection(collection_name)
            print(f"Collection '{collection_name}' created")
            logger.info(f"Created collection '{collection_name}' in '{self.current_db.name}'")
        except KiteDBError as e:
            print(f"Error: {e}")
            logger.error(f"Create collection failed: {e}")

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

            if op not in ['insert', 'find', 'update', 'delete']:
                print(f"Unknown operation: {op}")
                logger.warning(f"Unknown operation attempted: {op}")
                return

            coll = self.current_db.get_collection(collection_name)
            logger.info(f"Executing {op} on '{collection_name}': query={query}, data={data}")

            if op == 'insert':
                res = coll.insert(data)
                if res == "logged":
                    print("Insertion logged")
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
        'use': handle_use,
        'show': handle_show,  # Added show command
        'create': handle_create,
        'begin': handle_begin,
        'commit': handle_commit,
        'rollback': handle_rollback,
        'exit': handle_exit,
        'help': handle_help,
    }

if __name__ == '__main__':
    console = KiteDBConsole()
    console.run()