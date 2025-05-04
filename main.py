import json
from typing import Any, Dict
from src.core.database import Database
from src.query.query_parser import QueryParser
from src.logging.logger import logger
from src.core.exceptions import ValidationError

CMD_MAP = {
    'create': 'create_collection',
    'insert': 'insert', 'find': 'find',
    'update': 'update', 'delete': 'delete',
    'begin': 'begin_transaction', 'commit': 'commit', 'rollback': 'rollback'
}

def console():
    db = None
    print("Welcome to KiteDB v1.0")
    
    while True:
        try:
            cmd = input('> ').strip()
            if not cmd:
                continue
            
            # Handle 'use' command separately
            if cmd.startswith('use '):
                db_name = cmd[4:].strip()
                db = Database(db_name)
                print(f"Using {db_name}")
                continue

            # Parse the command using the updated QueryParser
            parsed = QueryParser.parse(cmd)
            op = parsed['operation']
            collection_name = parsed['collection']
            query = parsed.get('query', {})
            data = parsed.get('data', {})

            if op == 'exit':
                print("Exiting KiteDB...")
                break

            if not db:
                print("Select a database first: use <name>")
                continue

            func = CMD_MAP.get(op)
            if not func:
                print("Unknown command")
                continue

            # Handle database operations like create_collection, begin_transaction
            if func in ['create_collection', 'begin_transaction']:
                getattr(db, func)(*eval(f'[{repr(query)}]'))
                print(f"{op} OK")
            else:
                # Get the collection object from the database
                coll = db.get_collection(collection_name)

                # Ensure correct arguments are passed to the collection methods
                if op == 'insert':
                    res = coll.insert(data)
                elif op == 'find':
                    res = coll.find(query)
                elif op == 'update':
                    res = coll.update(query, data)
                elif op == 'delete':
                    res = coll.delete(query)

                print(res)

        except Exception as e:
            print(f"Error (main file): {e}")
            logger.error(e)

if __name__ == '__main__':
    console()
