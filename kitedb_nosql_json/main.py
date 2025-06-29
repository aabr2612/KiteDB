import os
import json
import getpass
import argparse
import bcrypt
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
- login <username>                  Authenticate user (e.g., 'login admin').
- use <database_name>               Switch to a database (e.g., 'use mydb'). Creates if it doesn't exist.
- exitdb                            Exit the current database context without exiting the program.
- list                              List all databases in the storage root.
- create <collection_name> [schema] Create a collection with an optional JSON schema.
- delete <collection_name>          Delete a collection (e.g., 'delete users').
- begin                             Start a transaction for atomic operations.
- commit                            Commit the current transaction.
- rollback                          Roll back the current transaction.
- exit                              Exit the KiteDB console.
- help                              Display this help message.
- adduser <username> <password>     Add a new user.
- removeuser <username>             Remove an existing user.
- setperm <username> <db_name> <coll_name> <perm1> [perm2 ...]  
  - Set permissions for a user on a database or collection.
  - Permissions: read, write, update, delete, create, access
  - Use 'access denied' to deny all access to a database (e.g., 'setperm ali testdb * access denied').
  - Use 'access allowed' to grant access to a database (e.g., 'setperm ali testdb * access allowed read write update').
  - Example: setperm ali testdb users read write update
- listperms [username]              List permissions for a user or all users.

Collection Operations
--------------------
Format: <collection>.<operation>{<parameters>}
Parameters must be valid JSON objects or arrays, enclosed in curly braces {}.

1. add{<document> | [<document>, ...]}
   - Add one or more documents to a collection.
   - Example: users.add{{"name": "Alice", "age": 25}}
2. find{<query>}
   - Find documents matching the query.
   - Example: users.find{"name": "Alice"}
3. update{<query>,<update>}
   - Update documents matching the query.
   - Example: users.update{"name": "Alice", "age": 26}
4. delete{<query>}
   - Delete documents matching the query.
   - Example: users.delete{"name": "Alice"}

Supported Query Operators
------------------------
- Comparison: $eq, $ne, $gt, $gte, $lt, $lte
- Logical: $and, $or, $not
- Dot notation for nested fields: e.g., "address.city": "San Francisco"
"""

class KiteDBConsole:
    def __init__(self):
        self.current_db = None
        self.running = True
        self.authenticated = False
        self.current_user = None
        self.users_file = "users.json"
        self.acl_file = "acl.json"
        self.users = self.load_users()
        self.acl = self.load_acl()

    def load_users(self):
        """Load users from users.json or create default if file doesn't exist."""
        if os.path.exists(self.users_file):
            with open(self.users_file, "r") as f:
                return json.load(f)
        default_users = {"admin": bcrypt.hashpw("admin".encode('utf-8'), bcrypt.gensalt()).decode('utf-8')}
        with open(self.users_file, "w") as f:
            json.dump(default_users, f, indent=2)
        return default_users

    def save_users(self):
        """Save users to users.json."""
        with open(self.users_file, "w") as f:
            json.dump(self.users, f, indent=2)

    def load_acl(self):
        """Load ACL from acl.json or create default if file doesn't exist."""
        if os.path.exists(self.acl_file):
            with open(self.acl_file, "r") as f:
                return json.load(f)
        # Default ACL only needs to initialize for non-admin users; admin gets all permissions implicitly
        default_acl = {
            "admin": {
                "databases": {}
            }
        }
        with open(self.acl_file, "w") as f:
            json.dump(default_acl, f, indent=2)
        return default_acl

    def save_acl(self):
        """Save ACL to acl.json."""
        with open(self.acl_file, "w") as f:
            json.dump(self.acl, f, indent=2)

    def hash_password(self, password: str) -> str:
        """Hash a password using bcrypt."""
        return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

    def check_password(self, password: str, hashed: str) -> bool:
        """Check if a password matches the hashed version."""
        return bcrypt.checkpw(password.encode('utf-8'), hashed.encode('utf-8'))

    def has_permission(self, user: str, db_name: str, collection_name: str, operation: str) -> bool:
        # Admin has all permissions by default
        if user == "admin":
            return True

        # Reload ACL to ensure we have the latest permissions
        self.acl = self.load_acl()
        if user not in self.acl:
            return False
        user_perms = self.acl[user]
        db_perms = user_perms.get("databases", {})
        
        # Check database-level access (deny if explicitly denied)
        if db_name in db_perms and "access" in db_perms[db_name] and db_perms[db_name]["access"] == "denied":
            return False
        
        # Check global permissions
        if "*" in db_perms:
            coll_perms = db_perms["*"].get("collections", {})
            if "*" in coll_perms and operation in coll_perms["*"]:
                return True
        
        # Check specific database permissions
        db_perm = db_perms.get(db_name, {})
        if "access" in db_perm and db_perm["access"] == "denied":
            return False
        coll_perms = db_perm.get("collections", {})
        
        # For database-level access (e.g., 'use'), allow if read permission exists on any collection
        if operation == "read" and collection_name == "*":
            if "*" in coll_perms and "read" in coll_perms["*"]:
                return True
            for coll, perms in coll_perms.items():
                if "read" in perms:
                    return True
            return False
        
        # For collection-specific operations
        if "*" in coll_perms and operation in coll_perms["*"]:
            return True
        if collection_name in coll_perms and operation in coll_perms[collection_name]:
            return True
        
        # Map operations to permissions
        perm_map = {"find": "read", "add": "write", "update": "update", "delete": "delete", "create": "create"}
        required_perm = perm_map.get(operation, operation)
        return required_perm in coll_perms.get("*", []) or required_perm in coll_perms.get(collection_name, [])

    def run(self):
        print("Welcome to KiteDB v2.0")
        while self.running:
            if not self.authenticated:
                self.handle_login()
                continue
            prompt = (
                f"kiteDB ({self.current_db.name if self.current_db else ''}) [{self.current_user}] > "
                if self.current_db
                else f"kiteDB [{self.current_user}] > "
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
        self.users = self.load_users()
        username = input("Username: ").strip()
        password = getpass.getpass("Password: ")
        if username in self.users and self.check_password(password, self.users[username]):
            self.authenticated = True
            self.current_user = username
            print("Login successful")
            logger.info(f"User '{username}' logged in")
        else:
            print("Invalid credentials")
            logger.warning(f"Failed login attempt for '{username}'")

    def handle_command(self, cmd: str):
        if not cmd or cmd.isspace():
            print("Invalid command. Use 'help' for available commands.")
            return
        parts = cmd.split(maxsplit=1)
        command = parts[0].lower()
        arg = parts[1] if len(parts) > 1 else ""
        if command in self.DB_COMMANDS:
            self.DB_COMMANDS[command](self, arg)
        else:
            self.handle_collection_operation(cmd)

    def handle_use(self, arg: str):
        db_name = arg.strip()
        if not db_name:
            print("Invalid command. Use 'help' for available commands.")
            return
        if not self.has_permission(self.current_user, db_name, "*", "read"):
            print("Permission denied: No access to database")
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

    def handle_exitdb(self, arg: str):
        if not self.current_db:
            print("Not currently in a database context.")
            return
        self.current_db = None
        print("Exited current database context.")
        logger.info("Exited current database context.")

    def handle_list(self, arg: str):
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
                    if self.has_permission(self.current_user, db, "*", "read"):
                        print(f"  {db}")
            else:
                print("No databases found")
        except Exception as e:
            print(f"Error listing databases: {e}")
            logger.error(f"Error listing databases: {e}")

    def handle_create(self, arg: str):
        if not self.current_db:
            print("Invalid command. Use 'help' for available commands.")
            return
        if not self.has_permission(self.current_user, self.current_db.name, "*", "create"):
            print("Permission denied: No create access to database")
            return
        parts = arg.strip().split(maxsplit=1)
        collection_name = parts[0]
        schema = parts[1] if len(parts) > 1 else None
        if not collection_name:
            print("Invalid command. Use 'help' for available commands.")
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
        if not self.current_db:
            print("Invalid command. Use 'help' for available commands.")
            return
        if not self.has_permission(self.current_user, self.current_db.name, arg.strip(), "delete"):
            print("Permission denied: No delete access to collection")
            return
        collection_name = arg.strip()
        if not collection_name:
            print("Invalid command. Use 'help' for available commands.")
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
        if not self.current_db:
            print("Invalid command. Use 'help' for available commands.")
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
            print("Invalid command. Use 'help' for available commands.")
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
            print("Invalid command. Use 'help' for available commands.")
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

    def handle_adduser(self, arg: str):
        parts = arg.strip().split()
        if len(parts) < 2:
            print("Invalid command. Use 'help' for available commands.")
            return
        username, password = parts[0], parts[1]
        if username in self.users:
            print("User already exists")
            return
        self.users[username] = self.hash_password(password)
        self.save_users()
        print(f"User '{username}' added")
        logger.info(f"Added user '{username}'")

    def handle_removeuser(self, arg: str):
        username = arg.strip()
        if not username:
            print("Invalid command. Use 'help' for available commands.")
            return
        if username not in self.users:
            print("User not found")
            return
        if username == self.current_user:
            print("Cannot remove current user")
            return
        del self.users[username]
        if username in self.acl:
            del self.acl[username]
        self.save_users()
        self.save_acl()
        print(f"User '{username}' removed")
        logger.info(f"Removed user '{username}'")

    def handle_setperm(self, arg: str):
        parts = arg.strip().split()
        if len(parts) < 4:
            print("Invalid command. Use 'help' for available commands.")
            return
        username, db_name, coll_name = parts[0], parts[1], parts[2]
        permissions = parts[3:]
        valid_perms = {"read", "write", "update", "delete", "create", "access"}

        # Check for 'access denied' or 'access allowed' as special cases
        access_setting = None
        if "access" in permissions:
            if "denied" in permissions:
                access_setting = "denied"
                permissions = [p for p in permissions if p not in ["access", "denied"]]
                # If denying access, no other permissions should be specified
                if permissions:
                    print("When denying database access, no other permissions should be specified.")
                    return
            elif "allowed" in permissions:
                access_setting = "allowed"
                permissions = [p for p in permissions if p not in ["access", "allowed"]]
            else:
                print("Use 'access denied' to deny database access or 'access allowed' to grant access.")
                return
        elif "denied" in permissions or "allowed" in permissions:
            print("'denied' or 'allowed' can only be used with 'access'.")
            return

        # Validate remaining permissions (if any)
        if permissions and not all(perm in valid_perms for perm in permissions):
            print(f"Invalid permissions. Use: {valid_perms}")
            return

        if username not in self.acl:
            self.acl[username] = {"databases": {}}
        if db_name not in self.acl[username]["databases"]:
            self.acl[username]["databases"][db_name] = {"collections": {}}

        # Handle database-level access
        if access_setting:
            self.acl[username]["databases"][db_name]["access"] = access_setting
            if access_setting == "denied":
                self.acl[username]["databases"][db_name]["collections"][coll_name] = []
                print(f"Access denied for '{username}' on database '{db_name}'")
            else:
                self.acl[username]["databases"][db_name]["collections"][coll_name] = permissions
                print(f"Access granted for '{username}' on '{db_name}.{coll_name}': {permissions}")
        else:
            self.acl[username]["databases"][db_name]["access"] = "allowed"
            self.acl[username]["databases"][db_name]["collections"][coll_name] = permissions
            print(f"Permissions set for '{username}' on '{db_name}.{coll_name}': {permissions}")

        self.save_acl()
        logger.info(f"Set permissions for '{username}' on '{db_name}.{coll_name}': {permissions}")

    def handle_listperms(self, arg: str):
        username = arg.strip()
        if username and username not in self.acl:
            print("User not found")
            return
        if username:
            print(f"Permissions for '{username}':")
            for db_name, db_perm in self.acl[username].get("databases", {}).items():
                access = db_perm.get("access", "allowed")
                print(f"  {db_name}: Access = {access}")
                for coll_name, perms in db_perm.get("collections", {}).items():
                    print(f"    {db_name}.{coll_name}: {perms}")
        else:
            print("Permissions for all users:")
            for user, perms in self.acl.items():
                print(f"  User: {user}")
                for db_name, db_perm in perms.get("databases", {}).items():
                    access = db_perm.get("access", "allowed")
                    print(f"    {db_name}: Access = {access}")
                    for coll_name, perms in db_perm.get("collections", {}).items():
                        print(f"      {db_name}.{coll_name}: {perms}")

    def handle_collection_operation(self, cmd: str):
        if not self.current_db:
            print("Invalid command. Use 'help' for available commands.")
            return
        try:
            parsed = QueryParser.parse(cmd)
            op = parsed["operation"]
            collection_name = parsed["collection"]
            if not self.has_permission(self.current_user, self.current_db.name, collection_name, op):
                print("Permission denied")
                logger.warning(f"Permission denied for user '{self.current_user}' on '{cmd}'")
                return
            query = parsed.get("query", {})
            data = parsed.get("data", {})

            if op not in ["add", "find", "update", "delete"]:
                print("Invalid command. Use 'help' for available commands.")
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
        "exitdb": handle_exitdb,
        "list": handle_list,
        "create": handle_create,
        "delete": handle_delete,
        "begin": handle_begin,
        "commit": handle_commit,
        "rollback": handle_rollback,
        "exit": handle_exit,
        "help": handle_help,
        "adduser": handle_adduser,
        "removeuser": handle_removeuser,
        "setperm": handle_setperm,
        "listperms": handle_listperms,
    }

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="KiteDB Console or Server")
    parser.add_argument("--server", action="store_true", help="Run as server")
    args = parser.parse_args()

    if args.server:
        from server import run_server
        run_server()
    else:
        console = KiteDBConsole()
        console.run()