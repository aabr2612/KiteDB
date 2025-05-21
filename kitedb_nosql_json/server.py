import socketserver
import json
import threading
import os
import bcrypt
from src.core.database import Database
from src.query.query_parser import QueryParser
from src.config import logger
from src.core.exceptions import ValidationError, KiteDBError, TransactionError
from src.config import config
from datetime import datetime

class KiteDBRequestHandler(socketserver.BaseRequestHandler):
    """Handle individual client connections and process their commands."""
    
    def __init__(self, request, client_address, server):
        self.users_file = "users.json"
        self.acl_file = "acl.json"
        self.authenticated = False
        self.current_user = None
        self.current_db = None
        self.session_lock = threading.Lock()
        print(f"Initialized handler for client {client_address}")
        super().__init__(request, client_address, server)

    def load_users(self):
        """Load users from users.json or create default if file doesn't exist."""
        print(os.path)
        if os.path.exists(self.users_file):
            with open(self.users_file, "r") as f:
                return json.load(f)
        default_users = {"admin": bcrypt.hashpw("admin".encode('utf-8'), bcrypt.gensalt()).decode('utf-8')}
        with open(self.users_file, "w") as f:
            json.dump(default_users, f, indent=2)
        return default_users

    def load_acl(self):
        """Load ACL from acl.json or create default if file doesn't exist."""
        print(f"Loading ACL from {self.acl_file}")
        if os.path.exists(self.acl_file):
            with open(self.acl_file, "r") as f:
                acl = json.load(f)
                print(f"Loaded ACL: {acl}")
                return acl
        # Default ACL only initializes an empty structure for admin; permissions are implicit
        default_acl = {
            "admin": {
                "databases": {}
            }
        }
        print(f"ACL file does not exist, creating default ACL: {default_acl}")
        with open(self.acl_file, "w") as f:
            json.dump(default_acl, f, indent=2)
        return default_acl

    def save_acl(self):
        """Save ACL to acl.json."""
        print(f"Saving ACL to {self.acl_file}: {self.acl}")
        with open(self.acl_file, "w") as f:
            json.dump(self.acl, f, indent=2)

    def has_permission(self, user: str, db_name: str, collection_name: str, operation: str) -> bool:
        """Check if the user has the required permission."""
        print(f"Checking permission for user '{user}' on db '{db_name}', collection '{collection_name}', operation '{operation}'")
        # Admin has all permissions by default
        if user == "admin":
            print(f"Admin user detected, granting all permissions")
            return True

        acl = self.load_acl()
        if user not in acl:
            print(f"User '{user}' not found in ACL")
            return False
        user_perms = acl[user]
        db_perms = user_perms.get("databases", {})
        print(f"User permissions: {user_perms}")
        
        # Check database-level access (deny if explicitly denied)
        if db_name in db_perms and "access" in db_perms[db_name] and db_perms[db_name]["access"] == "denied":
            print(f"Access denied for db '{db_name}'")
            return False
        
        # Check global permissions
        if "*" in db_perms:
            coll_perms = db_perms["*"].get("collections", {})
            if "*" in coll_perms and operation in coll_perms["*"]:
                print(f"Global permission granted: {operation} in {coll_perms['*']}")
                return True
        
        # Check specific database permissions
        db_perm = db_perms.get(db_name, {})
        if "access" in db_perm and db_perm["access"] == "denied":
            print(f"Access denied for db '{db_name}'")
            return False
        coll_perms = db_perm.get("collections", {})
        print(f"Collection permissions for db '{db_name}': {coll_perms}")
        
        # For database-level access (e.g., 'use'), allow if read permission exists on any collection
        if operation == "read" and collection_name == "*":
            if "*" in coll_perms and "read" in coll_perms["*"]:
                print("Read permission granted for all collections")
                return True
            for coll, perms in coll_perms.items():
                if "read" in perms:
                    print(f"Read permission granted for collection '{coll}'")
                    return True
            print("No read permission found for any collection")
            return False
        
        # For collection-specific operations
        if "*" in coll_perms and operation in coll_perms["*"]:
            print(f"Collection wildcard permission granted: {operation}")
            return True
        if collection_name in coll_perms and operation in coll_perms[collection_name]:
            print(f"Specific collection permission granted: {operation} in {collection_name}")
            return True
        
        # Map operations to permissions
        perm_map = {"find": "read", "add": "write", "update": "write", "delete": "delete", "create": "create"}
        required_perm = perm_map.get(operation, operation)
        granted = required_perm in coll_perms.get("*", []) or required_perm in coll_perms.get(collection_name, [])
        print(f"Permission check result: {granted} (required: {required_perm})")
        return granted

    def handle(self):
        """Process client requests in a loop until disconnection."""
        print(f"New connection from {self.client_address}")
        logger.info(f"New connection from {self.client_address}")
        try:
            while True:
                data = self.request.recv(1024).decode('utf-8').strip()
                print(f"Received raw data from {self.client_address}: '{data}'")
                if not data:
                    print(f"Empty data received, closing connection for {self.client_address}")
                    break
                logger.debug(f"Received from {self.client_address}: {data}")
                response = self.process_command(data)
                print(f"Sending response to {self.client_address}: {response}")
                self.request.sendall(json.dumps(response).encode('utf-8') + b'\n')
        except ConnectionError:
            print(f"Connection error: Client {self.client_address} disconnected")
            logger.info(f"Client {self.client_address} disconnected")
            if self.current_db and self.current_db.transaction and self.current_db.transaction.active:
                try:
                    self.current_db.transaction.rollback()
                    print(f"Rolled back transaction for disconnected client {self.client_address}")
                    logger.info(f"Rolled back transaction for disconnected client {self.client_address}")
                except TransactionError as e:
                    print(f"Rollback failed for {self.client_address}: {e}")
                    logger.error(f"Rollback failed for {self.client_address}: {e}")
        except Exception as e:
            print(f"Unexpected error handling client {self.client_address}: {e}")
            logger.error(f"Error handling client {self.client_address}: {e}")
        finally:
            print(f"Closing connection for {self.client_address}")
            self.request.close()

    def process_command(self, cmd: str) -> dict:
        """Process a client command and return a response as a dictionary."""
        print(f"Processing command: '{cmd}'")
        try:
            if not cmd or cmd.isspace():
                response = {"status": "error", "message": "Invalid command. Use 'help' for available commands."}
                print(f"Invalid command, returning: {response}")
                logger.info(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} > {cmd}\n{response['message']}")
                return response

            if not self.authenticated:
                print("Client not authenticated, handling login command")
                return self.handle_login_command(cmd)

            parts = cmd.split(maxsplit=1)
            command = parts[0].lower()
            arg = parts[1] if len(parts) > 1 else ""
            print(f"Parsed command: command='{command}', arg='{arg}'")

            if command in self.DB_COMMANDS:
                print(f"Executing DB command: {command}")
                response = self.DB_COMMANDS[command](self, arg)
                print(f"DB command response: {response}")
                logger.info(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} > {cmd}\n{response['message']}")
                return response
            else:
                print(f"Handling as collection operation: {cmd}")
                response = self.handle_collection_operation(cmd)
                print(f"Collection operation response: {response}")
                if response["status"] == "success" and "data" in response:
                    logger.info(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} > {cmd}\n{response['message']}\n{json.dumps(response['data'], indent=2)}")
                else:
                    logger.info(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} > {cmd}\n{response['message']}")
                return response
        except Exception as e:
            print(f"Error processing command '{cmd}': {e}")
            logger.error(f"Error processing command '{cmd}' from {self.client_address}: {e}")
            response = {"status": "error", "message": f"Invalid command. Use 'help' for available commands. Error: {e}"}
            logger.info(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} > {cmd}\n{response['message']}")
            print(f"Returning error response: {response}")
            return response

    def handle_login_command(self, cmd: str) -> dict:
        """Authenticate client credentials."""
        print(f"Handling login command: '{cmd}'")
        try:
            parts = cmd.split(maxsplit=2)
            if parts[0].lower() != "login" or len(parts) < 3:
                response = {"status": "error", "message": "Invalid command. Use 'help' for available commands."}
                print(f"Invalid login command, returning: {response}")
                return response
            username, password = parts[1], parts[2]
            print(f"Attempting login for username: '{username}'")
            users = self.load_users()
            if username in users and bcrypt.checkpw(password.encode('utf-8'), users[username].encode('utf-8')):
                self.authenticated = True
                self.current_user = username
                print(f"Login successful for '{username}'")
                logger.info(f"User '{username}' logged in from {self.client_address}")
                response = {"status": "success", "message": "Login successful"}
                print(f"Returning: {response}")
                return response
            else:
                print(f"Login failed for '{username}'")
                logger.warning(f"Failed login attempt for '{username}' from {self.client_address}")
                response = {"status": "error", "message": "Invalid credentials"}
                print(f"Returning: {response}")
                return response
        except Exception as e:
            print(f"Error in login command: {e}")
            response = {"status": "error", "message": f"Invalid command. Use 'help' for available commands. Error: {e}"}
            print(f"Returning: {response}")
            return response

    def handle_use(self, arg: str) -> dict:
        """Switch to a specified database for the client session."""
        print(f"Handling 'use' command with arg: '{arg}'")
        db_name = arg.strip()
        if not db_name:
            response = {"status": "error", "message": "Invalid command. Use 'help' for available commands."}
            print(f"Invalid db name, returning: {response}")
            return response
        if not self.authenticated:
            response = {"status": "error", "message": "Please login first"}
            print(f"Not authenticated, returning: {response}")
            return response
        if not self.has_permission(self.current_user, db_name, "*", "read"):
            response = {"status": "error", "message": "Permission denied: No access to database"}
            print(f"Permission denied, returning: {response}")
            return response
        try:
            with self.session_lock:
                self.current_db = Database(db_name)
                db_path = os.path.abspath(self.current_db.storage.db_path)
                print(f"Switched to database '{db_name}' at '{db_path}'")
                logger.info(f"Client {self.client_address} switched to database '{db_name}' at '{db_path}'")
                response = {"status": "success", "message": f"Switched to database '{db_name}', path: {db_path}"}
                print(f"Returning: {response}")
                return response
        except KiteDBError as e:
            print(f"Use database failed: {e}")
            logger.error(f"Use database failed for {self.client_address}: {e}")
            response = {"status": "error", "message": str(e)}
            print(f"Returning: {response}")
            return response

    def handle_list(self, arg: str) -> dict:
        """List all available databases."""
        print(f"Handling 'list' command with arg: '{arg}'")
        try:
            databases = []
            data_root = config.get("storage.data_root")
            print(f"Listing databases in {data_root}")
            for entry in os.listdir(data_root):
                path = os.path.join(data_root, entry)
                if os.path.isdir(path):
                    databases.append(entry)
            databases.sort()
            response = {"status": "success", "data": databases, "message": "Databases listed"}
            print(f"Returning: {response}")
            return response
        except Exception as e:
            print(f"Error listing databases: {e}")
            logger.error(f"Error listing databases for {self.client_address}: {e}")
            response = {"status": "error", "message": f"Invalid command. Use 'help' for available commands. Error: {e}"}
            print(f"Returning: {response}")
            return response

    def handle_create(self, arg: str) -> dict:
        """Create a new collection in the current database."""
        print(f"Handling 'create' command with arg: '{arg}'")
        if not self.current_db:
            response = {"status": "error", "message": "Invalid command. Use 'help' for available commands."}
            print(f"No current db, returning: {response}")
            return response
        if not self.has_permission(self.current_user, self.current_db.name, "*", "create"):
            response = {"status": "error", "message": "Permission denied: No create access to database"}
            print(f"Permission denied, returning: {response}")
            return response
        parts = arg.strip().split(maxsplit=1)
        collection_name = parts[0]
        schema = parts[1] if len(parts) > 1 else None
        print(f"Collection name: '{collection_name}', schema: '{schema}'")
        if not collection_name:
            response = {"status": "error", "message": "Invalid command. Use 'help' for available commands."}
            print(f"Invalid collection name, returning: {response}")
            return response
        try:
            schema_dict = json.loads(schema) if schema else None
            print(f"Parsed schema: {schema_dict}")
            self.current_db.create_collection(collection_name, schema_dict)
            print(f"Created collection '{collection_name}'")
            logger.info(f"Client {self.client_address} created collection '{collection_name}' in '{self.current_db.name}'")
            response = {"status": "success", "message": f"Collection '{collection_name}' created"}
            print(f"Returning: {response}")
            return response
        except KiteDBError as e:
            print(f"Create collection failed: {e}")
            logger.error(f"Create collection failed for {self.client_address}: {e}")
            response = {"status": "error", "message": str(e)}
            print(f"Returning: {response}")
            return response
        except json.JSONDecodeError as e:
            print(f"Invalid schema JSON: {e}")
            logger.error(f"Invalid schema JSON for {self.client_address}: {e}")
            response = {"status": "error", "message": f"Invalid command. Use 'help' for available commands. Error: {e}"}
            print(f"Returning: {response}")
            return response

    def handle_delete(self, arg: str) -> dict:
        """Delete a collection from the current database."""
        print(f"Handling 'delete' command with arg: '{arg}'")
        if not self.current_db:
            response = {"status": "error", "message": "Invalid command. Use 'help' for available commands."}
            print(f"No current db, returning: {response}")
            return response
        if not self.has_permission(self.current_user, self.current_db.name, arg.strip(), "delete"):
            response = {"status": "error", "message": "Permission denied: No delete access to collection"}
            print(f"Permission denied, returning: {response}")
            return response
        collection_name = arg.strip()
        if not collection_name:
            response = {"status": "error", "message": "Invalid command. Use 'help' for available commands."}
            print(f"Invalid collection name, returning: {response}")
            return response
        try:
            result = self.current_db.drop_collection(collection_name)
            message = "Collection deletion logged" if result == "logged" else f"Collection '{collection_name}' deleted"
            print(f"Delete result: {message}")
            logger.info(f"Client {self.client_address} deleted collection '{collection_name}' from '{self.current_db.name}'")
            response = {"status": "success", "message": message}
            print(f"Returning: {response}")
            return response
        except KiteDBError as e:
            print(f"Delete collection failed: {e}")
            logger.error(f"Delete collection failed for {self.client_address}: {e}")
            response = {"status": "error", "message": str(e)}
            print(f"Returning: {response}")
            return response

    def handle_begin(self, arg: str) -> dict:
        """Start a new transaction in the current database."""
        print(f"Handling 'begin' command with arg: '{arg}'")
        if not self.current_db:
            response = {"status": "error", "message": "Invalid command. Use 'help' for available commands."}
            print(f"No current db, returning: {response}")
            return response
        try:
            self.current_db.begin_transaction()
            print("Transaction begun")
            logger.info(f"Client {self.client_address} began transaction in '{self.current_db.name}'")
            response = {"status": "success", "message": "Transaction begun"}
            print(f"Returning: {response}")
            return response
        except KiteDBError as e:
            print(f"Begin transaction failed: {e}")
            logger.error(f"Begin transaction failed for {self.client_address}: {e}")
            response = {"status": "error", "message": str(e)}
            print(f"Returning: {response}")
            return response

    def handle_commit(self, arg: str) -> dict:
        """Commit the active transaction in the current database."""
        print(f"Handling 'commit' command with arg: '{arg}'")
        if not self.current_db:
            response = {"status": "error", "message": "Invalid command. Use 'help' for available commands."}
            print(f"No current db, returning: {response}")
            return response
        if not self.current_db.transaction or not self.current_db.transaction.active:
            response = {"status": "error", "message": "No active transaction"}
            print(f"No active transaction, returning: {response}")
            return response
        try:
            self.current_db.transaction.commit()
            print("Transaction committed")
            logger.info(f"Client {self.client_address} committed transaction in '{self.current_db.name}'")
            response = {"status": "success", "message": "Transaction committed"}
            print(f"Returning: {response}")
            return response
        except TransactionError as e:
            print(f"Commit transaction failed: {e}")
            logger.error(f"Commit transaction failed for {self.client_address}: {e}")
            response = {"status": "error", "message": str(e)}
            print(f"Returning: {response}")
            return response

    def handle_rollback(self, arg: str) -> dict:
        """Roll back the active transaction in the current database."""
        print(f"Handling 'rollback' command with arg: '{arg}'")
        if not self.current_db:
            response = {"status": "error", "message": "Invalid command. Use 'help' for available commands."}
            print(f"No current db, returning: {response}")
            return response
        if not self.current_db.transaction or not self.current_db.transaction.active:
            response = {"status": "error", "message": "No active transaction"}
            print(f"No active transaction, returning: {response}")
            return response
        try:
            self.current_db.transaction.rollback()
            print("Transaction rolled back")
            logger.info(f"Client {self.client_address} rolled back transaction in '{self.current_db.name}'")
            response = {"status": "success", "message": "Transaction rolled back"}
            print(f"Returning: {response}")
            return response
        except TransactionError as e:
            print(f"Rollback transaction failed: {e}")
            logger.error(f"Rollback transaction failed for {self.client_address}: {e}")
            response = {"status": "error", "message": str(e)}
            print(f"Returning: {response}")
            return response

    def handle_exit(self, arg: str) -> dict:
        """Close the client connection."""
        print(f"Handling 'exit' command with arg: '{arg}'")
        logger.info(f"Client {self.client_address} requested exit")
        if self.current_db and self.current_db.transaction and self.current_db.transaction.active:
            try:
                self.current_db.transaction.rollback()
                print(f"Rolled back transaction for exiting client")
                logger.info(f"Rolled back transaction for exiting client {self.client_address}")
            except TransactionError as e:
                print(f"Rollback failed: {e}")
                logger.error(f"Rollback failed for {self.client_address}: {e}")
        response = {"status": "success", "message": "Connection closed"}
        print(f"Returning: {response}")
        return response

    def handle_help(self, arg: str) -> dict:
        """Return the help message for client reference."""
        print(f"Handling 'help' command with arg: '{arg}'")
        from main import HELP_MESSAGE
        response = {"status": "success", "message": HELP_MESSAGE}
        print(f"Returning: {response}")
        return response

    def handle_collection_operation(self, cmd: str) -> dict:
        """Handle collection operations like add, find, update, or delete."""
        print(f"Handling collection operation: '{cmd}'")
        if not self.current_db:
            response = {"status": "error", "message": "Invalid command. Use 'help' for available commands."}
            print(f"No current db, returning: {response}")
            return response
        try:
            parsed = QueryParser.parse(cmd)
            print(f"Parsed query: {parsed}")
            op = parsed["operation"]
            collection_name = parsed["collection"]
            print(f"Operation: '{op}', Collection: '{collection_name}'")
            if not self.has_permission(self.current_user, self.current_db.name, collection_name, op):
                print(f"Permission denied for operation '{op}' on '{collection_name}'")
                logger.warning(f"Permission denied for user '{self.current_user}' on '{cmd}' from {self.client_address}")
                response = {"status": "error", "message": "Permission denied"}
                print(f"Returning: {response}")
                return response
            query = parsed.get("query", {})
            data = parsed.get("data", {})
            print(f"Query: {query}, Data: {data}")

            if op not in ["add", "find", "update", "delete"]:
                print(f"Unknown operation: '{op}'")
                logger.warning(f"Unknown operation attempted by {self.client_address}: {op}")
                response = {"status": "error", "message": "Invalid command. Use 'help' for available commands."}
                print(f"Returning: {response}")
                return response

            coll = self.current_db.get_collection(collection_name)
            print(f"Retrieved collection '{collection_name}'")
            logger.info(
                f"Client {self.client_address} executing {op} on '{collection_name}': query={query}, data={data}"
            )

            if op == "add":
                print(f"Inserting data: {data}")
                res = coll.insert(data)
                print(f"Insert result: {res}")
                if res == "logged":
                    response = {"status": "success", "message": "Insertion logged"}
                    print(f"Returning: {response}")
                    return response
                else:
                    message = (
                        f"Inserted {len(res)} documents with IDs: {res}"
                        if isinstance(res, list)
                        else f"Inserted document with ID: {res}"
                    )
                    response = {"status": "success", "message": message, "data": res}
                    print(f"Returning: {response}")
                    return response
            elif op == "find":
                print(f"Finding documents with query: {query}")
                res = coll.find(query)
                print(f"Find result: {res}")
                if not res:
                    response = {"status": "success", "message": "No documents found", "data": []}
                    print(f"Returning: {response}")
                    return response
                response = {"status": "success", "message": f"Found {len(res)} documents", "data": res}
                print(f"Returning: {response}")
                return response
            elif op == "update":
                print(f"Updating documents with query: {query}, data: {data}")
                res = coll.update(query, data)
                print(f"Update result: {res}")
                if res == "logged":
                    response = {"status": "success", "message": "Update logged"}
                    print(f"Returning: {response}")
                    return response
                response = {"status": "success", "message": f"Updated {res} documents", "data": res}
                print(f"Returning: {response}")
                return response
            elif op == "delete":
                print(f"Deleting documents with query: {query}")
                res = coll.delete(query)
                print(f"Delete result: {res}")
                if res == "logged":
                    response = {"status": "success", "message": "Delete logged"}
                    print(f"Returning: {response}")
                    return response
                response = {"status": "success", "message": f"Deleted {res} documents", "data": res}
                print(f"Returning: {response}")
                return response
        except ValidationError as e:
            print(f"Validation error: {e}")
            logger.error(f"Validation error in command '{cmd}' from {self.client_address}: {e}")
            response = {"status": "error", "message": f"Validation error: {e}"}
            print(f"Returning: {response}")
            return response
        except KiteDBError as e:
            print(f"Database error: {e}")
            logger.error(f"Database error in command '{cmd}' from {self.client_address}: {e}")
            response = {"status": "error", "message": f"Database error: {e}"}
            print(f"Returning: {response}")
            return response
        except Exception as e:
            print(f"Unexpected error: {e}")
            logger.error(f"Unexpected error in command '{cmd}' from {self.client_address}: {e}")
            response = {"status": "error", "message": f"Invalid command. Use 'help' for available commands. Error: {e}"}
            print(f"Returning: {response}")
            return response

    DB_COMMANDS = {
        "login": handle_login_command,
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

class KiteDBServer(socketserver.ThreadingTCPServer):
    """Multi-threaded TCP server for KiteDB."""
    allow_reuse_address = True

    def __init__(self, server_address, handler_class):
        print(f"Starting KiteDB server on {server_address}")
        super().__init__(server_address, handler_class)
        logger.info(f"KiteDB server started on {server_address}")

def run_server():
    """Start the KiteDB server using configured host and port."""
    host = config.get("server.host", "localhost")
    port = config.get("server.port", 5432)
    print(f"Configuring server on {host}:{port}")
    server = KiteDBServer((host, port), KiteDBRequestHandler)
    try:
        print("Server starting to serve forever")
        server.serve_forever()
    except KeyboardInterrupt:
        print("Keyboard interrupt received, shutting down server")
        server.shutdown()
        server.server_close()
        logger.info("KiteDB server stopped")

if __name__ == "__main__":
    run_server()