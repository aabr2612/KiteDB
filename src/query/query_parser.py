import json
import re
from typing import Any, Dict
from src.core.exceptions import ValidationError

class QueryParser:
    @staticmethod
    def parse(command: str) -> Dict[str, Any]:
        # Match the command format: <collection>.<operation>{<parameters>}
        match = re.match(r'^(?P<collection>\w+)\.(?P<operation>\w+)\s*\{(?P<parameters>.*)\}$', command.strip())
        if not match:
            raise ValidationError("Command format should be <collection>.<operation>{parameters}")
        
        collection = match.group('collection')
        op = match.group('operation')
        payload = match.group('parameters').strip()
        
        try:
            if op == 'insert':
                # Wrap payload in braces and parse as JSON
                doc = json.loads('{' + payload + '}')
                return {'operation': 'insert', 'collection': collection, 'query': {}, 'data': doc}
            
            elif op in ('find', 'delete'):
                # Wrap payload in braces and parse as JSON
                query = json.loads('{' + payload + '}')
                return {'operation': op, 'collection': collection, 'query': query}
            
            elif op == 'update':
                # Split payload into query and update parts
                query_str, update_str = QueryParser.split_update_payload(payload)
                query = json.loads('{' + query_str + '}')
                update = json.loads('{' + update_str + '}')
                return {'operation': 'update', 'collection': collection, 'query': query, 'data': update}
            
            else:
                raise ValidationError(f"Unsupported operation: {op}")
        except json.JSONDecodeError as e:
            raise ValidationError(f"Invalid JSON: {e}")

    @staticmethod
    def split_update_payload(payload: str) -> tuple:
        """Split update command payload into query and update JSON strings."""
        payload = payload.strip()
        brace_count = 0
        split_index = -1
        
        for i, char in enumerate(payload):
            if char == '{':
                brace_count += 1
            elif char == '}':
                brace_count -= 1
                if brace_count == 0 and i + 1 < len(payload) and payload[i + 1] == ' ':
                    split_index = i + 1
                    break
        
        if split_index == -1:
            raise ValidationError("Invalid update payload: must contain two JSON objects separated by a space")
        
        query_str = payload[:split_index].strip()
        update_str = payload[split_index:].strip()
        
        if not query_str or not update_str:
            raise ValidationError("Both query and update must be non-empty JSON objects")
        
        return query_str, update_str

    @staticmethod
    def match(doc: Dict[str, Any], query: Dict[str, Any]) -> bool:
        """Match a document against a query with support for operators."""
        for key, cond in query.items():
            if key == '$and':
                if not all(QueryParser.match(doc, c) for c in cond):
                    return False
            elif key == '$or':
                if not any(QueryParser.match(doc, c) for c in cond):
                    return False
            elif key == '$not':
                if QueryParser.match(doc, cond):
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
                    for op, expected in cond.items():
                        if not QueryParser._compare(val, op, expected):
                            return False
                elif val != cond:
                    return False
        return True

    @staticmethod
    def _compare(val: Any, op: str, expected: Any) -> bool:
        """Compare values using supported operators."""
        if op == '$gt': return val > expected
        if op == '$lt': return val < expected
        if op == '$gte': return val >= expected
        if op == '$lte': return val <= expected
        if op == '$eq': return val == expected
        if op == '$ne': return val != expected
        raise ValidationError(f"Unsupported operator: {op}")