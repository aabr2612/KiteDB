import json
from typing import Any, Dict
from src.core.exceptions import ValidationError
import re

class QueryParser:
    """
    Parses commands:
      <collection>.<operation>{parameters}

    Supports nested fields and logical/comparison operators for in-memory filtering.
    """

    @staticmethod
    def parse(command: str) -> Dict[str, Any]:
        # Regular expression to capture collection_name.operation{parameters}
        match = re.match(r'^(?P<collection>\w+)\.(?P<operation>\w+)\{(?P<parameters>.*)\}$', command.strip())
        
        if not match:
            raise ValidationError("Command format should be <collection>.<operation>{parameters}")
        
        # Extract collection, operation, and parameters from the command
        collection = match.group('collection')
        op = match.group('operation')
        payload = match.group('parameters').strip()
        
        if op == 'insert':
            try:
                doc = json.loads(payload or '{}')
            except json.JSONDecodeError as e:
                raise ValidationError(f"Invalid JSON for insert: {e}")
            return {'operation': 'insert', 'collection': collection, 'query': {}, 'data': doc}

        elif op in ('find', 'delete'):
            try:
                query = json.loads(payload or '{}')
            except json.JSONDecodeError as e:
                raise ValidationError(f"Invalid JSON for {op}: {e}")
            return {'operation': op, 'collection': collection, 'query': query}

        elif op == 'update':
            subparts = payload.strip().split(' ', 1)
            if len(subparts) != 2:
                raise ValidationError("Update requires both query and update JSON")
            try:
                query = json.loads(subparts[0])
                update = json.loads(subparts[1])
            except json.JSONDecodeError as e:
                raise ValidationError(f"Invalid JSON for update: {e}")
            return {'operation': 'update', 'collection': collection, 'query': query, 'data': update}

        else:
            raise ValidationError(f"Unsupported operation: {op}")

    @staticmethod
    def match(doc: Dict[str, Any], query: Dict[str, Any]) -> bool:
        for key, cond in query.items():
            if key == '$and':
                if not all(QueryParser.match(doc, c) for c in cond):
                    return False
            elif key == '$or':
                if not any(QueryParser.match(doc, c) for c in cond):
                    return False
            elif key.startswith('$'):
                raise ValidationError(f"Unknown top-level operator: {key}")
            else:
                parts = key.split('.')
                val = doc
                for p in parts:
                    if isinstance(val, dict) and p in val:
                        val = val[p]
                    else:
                        return False
                if isinstance(cond, dict):
                    op, expected = next(iter(cond.items()))
                    if not QueryParser._compare(val, op, expected):
                        return False
                elif val != cond:
                    return False
        return True

    @staticmethod
    def _compare(val: Any, op: str, expected: Any) -> bool:
        if op == '$gt': return val > expected
        if op == '$lt': return val < expected
        if op == '$gte': return val >= expected
        if op == '$lte': return val <= expected
        if op == '$eq': return val == expected
        if op == '$ne': return val != expected
        raise ValidationError(f"Unsupported comparison operator: {op}")