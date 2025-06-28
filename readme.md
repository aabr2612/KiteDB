# KiteDB

KiteDB is a secure, modular NoSQL JSON database implemented in Python. Designed for efficient data management in small to medium-scale applications, it features atomic transactions, role-based access control, AES encryption, and B-tree indexing. With a multi-threaded TCP server and interactive CLI, KiteDB serves as an educational tool for understanding NoSQL database internals.

## Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)
  - [Console Mode](#console-mode)
  - [Server Mode](#server-mode)
- [User Management Test Project](#user-management-test-project)
- [Project Structure](#project-structure)
- [Example Commands](#example-commands)
- [Testing](#testing)
- [Contributors](#contributors)
- [Future Enhancements](#future-enhancements)
- [References](#references)

## Features

- **NoSQL JSON Storage**: Stores JSON documents in collections with optional schema validation.
- **Atomic Transactions**: Supports `begin`, `commit`, and `rollback` with write-ahead logging for data integrity.
- **Role-Based Access Control**: Fine-grained permissions (read, write, update, delete, create, access) enforced via ACL.
- **AES-CBC Encryption**: Secure data storage with configurable 16/24/32-byte keys and chunked file handling.
- **B-tree Indexing**: In-memory B-tree indexes for O(log n) query performance.
- **Multi-threaded TCP Server**: Handles concurrent client connections (default port: 5432).
- **Query Language**: Supports complex queries with operators (`$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$and`, `$or`, `$not`) and dot notation for nested fields.
- **Interactive CLI**: Authenticated console with command validation and permission checks.
- **Comprehensive Logging**: Timestamped, rotating logs for debugging and auditing.

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/aabr2612/KiteDB
   cd kitedb
   ```
2. Install dependencies:
   - Run the runner.bat file
3. Configure settings in `config.yaml` (optional, defaults provided):
   - `storage.data_root`: Database storage directory (default: `./db`).
   - `storage.encryption_key`: AES encryption key (default: `thisisasecretkey`).
   - `logging.directory`: Log file directory (default: `./logs`).
   - `server.host` and `server.port`: Server settings (default: `localhost:5432`).

## Usage

### Console Mode

Run KiteDB in interactive console mode:
```bash
python main.py
```
- Default admin credentials: `username: admin`, `password: admin`.
- Key commands:
  - `login <username>`: Authenticate user.
  - `use <database_name>`: Switch to or create a database.
  - `create <collection_name> [schema]`: Create a collection with an optional JSON schema.
  - `adduser <username> <password>`: Add a new user (admin only).
  - `setperm <username> <db_name> <coll_name> <perm1> [perm2 ...]`: Set user permissions.
  - Collection operations: `<collection>.<operation>{<parameters>}` (e.g., `users.add{{"name": "Alice", "age": 25}}`).
  - `help`: Display full command list.

### Server Mode

Run KiteDB as a TCP server:
```bash
python server.py
```
- Connect using a TCP client (e.g., `telnet localhost 5432`) or the CLI (`python main.py`).
- Supports concurrent client connections via `ThreadingTCPServer`.

## User Management Test Project

A separate React-based User Management System was developed as a test project to validate KiteDB's backend functionality. Built with React and Tailwind CSS, it provides a graphical interface for user administration, including adding, viewing, editing, and deleting users. This project, located in the `frontend/` directory, uses KiteDB as its backend to demonstrate the database's capabilities in a real-world application scenario.

To run the test project:
```bash
cd kitedb_nosql_json/frontend
npm install
npm run dev
npm run server
```
- Access via browser (default: `http://localhost:3000`).
- Requires the KiteDB server (`python server.py`) to be running.

## Project Structure

- `config.yaml`: Configuration for storage, logging, and server settings.
- `main.py`: Console mode entry point with command handling and authentication.
- `server.py`: Multi-threaded TCP server for client connections.
- `config.py`: Loads and merges YAML configuration with defaults.
- `storage_engine.py`: Manages encrypted, chunked data storage using AES-CBC.
- `query_parser.py`: Parses JSON-based queries with regex and operator support.
- `logger.py`: Singleton logger with rotating file handler for event tracking.
- `index_manager.py`: Maintains B-tree indexes for query optimization.
- `collection.py`: Handles thread-safe CRUD operations with schema validation.
- `database.py`: Coordinates collections, schemas, and storage.
- `exceptions.py`: Custom exceptions for robust error handling.
- `transaction.py`: Ensures atomicity with write-ahead logging and rollback.
- `frontend/`: React-based User Management System (test project).

## Example Commands

```bash
login admin
use b
create users {"fields": {"name": "str", "age": "int"}}
users.add{{"name": "Alice", "age": 28}}
users.find{"age": {"$gt": 25}}
users.update{"name": "Alice", "age": 29}
users.delete{"name": "Alice"}
adduser testuser testpass
setperm testuser b users read write
```

## Testing

Test cases are documented in `test_queries.txt` and the project report:
- **KiteDB-NoSQL Tests**:
  - Validate `users.add` with schema enforcement (e.g., succeeds for valid types, fails for invalid).
  - Ensure concurrent `users.add` assigns unique IDs.
  - Verify transaction `commit` persists and `rollback` reverts.
  - Test complex queries (e.g., `users.find{"$and": [{"age": {"$gt": 25}}, {"age": {"$lt": 30}}]}`).
- **User Management Tests** (via test project):
  - Confirm form submission adds users.
  - Verify delete button removes users.
Run tests in console mode after setting up a database (e.g., `use b`).

## Contributors

- [**Mahad Saffi**](https://github.com/Mahad-Saffi) (2023-CS-59)
- [**Abdul Rehman**](https://github.com/aabr2612) (2023-CS-73)

Contributions are welcome! Submit issues or pull requests to the [GitHub repository](https://github.com/aabr2612/KiteDB).

## Future Enhancements

- Implement row-level locking for finer-grained concurrency.
- Extend to a client-server architecture for distributed scalability.
- Support advanced queries (e.g., joins, aggregations).
- Optimize performance with caching and asynchronous I/O.
- Enhance indexing for larger datasets.

## References

- Python Documentation: [https://www.python.org/doc/](https://www.python.org/doc/)
- Silberschatz, A., et al., *Database System Concepts*, 7th Edition
- MongoDB Documentation: [https://docs.mongodb.com/](https://docs.mongodb.com/)
- B-Tree: [https://en.wikipedia.org/wiki/B-tree](https://en.wikipedia.org/wiki/B-tree)
- AES: [https://en.wikipedia.org/wiki/Advanced_Encryption_Standard](https://en.wikipedia.org/wiki/Advanced_Encryption_Standard)
- React Documentation: [https://reactjs.org/docs/](https://reactjs.org/docs/)
- Tailwind CSS: [https://tailwindcss.com/docs/](https://tailwindcss.com/docs/)
