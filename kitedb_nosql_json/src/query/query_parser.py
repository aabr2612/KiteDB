import json
import re
from typing import Any, Dict, Tuple
from src.core.exceptions import ValidationError

class QueryParser:
    @staticmethod
    def parse(command: str) -> Dict[str, Any]:
        """Parse a database command into its components: collection, operation, and parameters."""
        if not command or not command.strip():
            raise ValidationError("Command cannot be empty")
        match = re.match(r'^(?P<collection>\w+)\.(?P<operation>\w+)\s*\{(?P<parameters>.*)\}$', command.strip())
        if not match:
            raise ValidationError("Command format should be <collection>.<operation>{parameters}")
        
        collection = match.group('collection')
        op = match.group('operation')
        payload = match.group('parameters').strip()
        
        try:
            if op == 'add':
                if not payload:
                    raise ValidationError("Add payload cannot be empty")
                doc = json.loads(payload)
                if isinstance(doc, dict):
                    data = [doc]
                elif isinstance(doc, list):
                    if not doc:
                        raise ValidationError("Document list cannot be empty")
                    if not all(isinstance(item, dict) for item in doc):
                        raise ValidationError("All documents in the list must be dictionaries")
                    data = doc
                else:
                    raise ValidationError("Add payload must be a dictionary or a list of dictionaries")
                return {'operation': 'add', 'collection': collection, 'query': {}, 'data': data}
            
            elif op in ('find', 'delete'):
                if not payload:
                    return {'operation': op, 'collection': collection, 'query': {}}
                query = json.loads('{' + payload + '}')
                if not isinstance(query, dict):
                    raise ValidationError("Query must be a dictionary")
                return {'operation': op, 'collection': collection, 'query': query}
            
            elif op == 'update':
                query_str, update_str = QueryParser.split_update_payload(payload)
                query = json.loads('{' + query_str + '}')
                update = json.loads('[' + update_str + ']' if update_str.startswith('[') else '{' + update_str + '}')
                return {'operation': 'update', 'collection': collection, 'query': query, 'data': update}
            
            else:
                raise ValidationError(f"Unsupported operation: {op}")
        except json.JSONDecodeError as e:
            raise ValidationError(f"Invalid JSON: {e}")

    @staticmethod
    def split_update_payload(payload: str) -> Tuple[str, str]:
        """Split an update command payload into query and update components."""
        payload = payload.strip()
        if not payload:
            raise ValidationError("Update payload cannot be empty")
        brace_count = 0
        in_string = False
        escape = False
        split_index = -1
        
        for i, char in enumerate(payload):
            if char == '"' and not escape:
                in_string = not in_string
            elif char == '\\' and not escape:
                escape = True
                continue
            elif not in_string:
                if char == '{':
                    brace_count += 1
                elif char == '}':
                    brace_count -= 1
                elif char == ',' and brace_count == 0:
                    split_index = i
                    break
            escape = False
        
        if split_index == -1:
            for i, char in enumerate(payload):
                if char == '"' and not escape:
                    in_string = not in_string
                elif char == '\\' and not escape:
                    escape = True
                    continue
                elif not in_string:
                    if char == '{':
                        brace_count += 1
                    elif char == '}':
                        brace_count -= 1
                        if brace_count == 0:
                            j = i + 1
                            while j < len(payload) and payload[j].isspace():
                                j += 1
                            if j < len(payload) and payload[j] == '{':
                                split_index = i + 1
                                break
                escape = False
        
        if split_index == -1 or brace_count != 0:
            raise ValidationError("Invalid update payload: must contain query and update JSON objects separated by comma or whitespace")
        
        query_str = payload[:split_index].strip()
        update_str = payload[split_index + 1:].strip() if payload[split_index] == ',' else payload[split_index:].strip()
        
        if not query_str or not update_str:
            raise ValidationError("Both query and update must be non-empty JSON objects")
        
        return query_str, update_str

    @staticmethod
    def match(doc: Dict[str, Any], query: Dict[str, Any]) -> bool:
        """Check if a document matches the given query conditions."""
        for key, cond in query.items():
            if key == '$and':
                if not isinstance(cond, list):
                    raise ValidationError(f"Operator '$and' requires a list of conditions, got {type(cond).__name__}")
                for c in cond:
                    if not isinstance(c, dict):
                        raise ValidationError(f"Each condition in '$and' must be a dictionary, got {type(c).__name__}")
                    if not c:
                        raise ValidationError("Empty condition in '$and' operator")
                    if not QueryParser.match(doc, c):
                        return False
            elif key == '$or':
                if not isinstance(cond, list):
                    raise ValidationError(f"Operator '$or' requires a list of conditions, got {type(cond).__name__}")
                for c in cond:
                    if not isinstance(c, dict):
                        raise ValidationError(f"Each condition in '$or' must be a dictionary, got {type(c).__name__}")
                    if not c:
                        raise ValidationError("Empty condition in '$or' operator")
                    if QueryParser.match(doc, c):
                        return True
                return False
            elif key == '$not':
                if not isinstance(cond, dict):
                    raise ValidationError(f"Operator '$not' requires a condition dictionary, got {type(cond).__name__}")
                if not cond:
                    raise ValidationError("Empty condition in '$not' operator")
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
        """Compare a document value against an expected value using a specified operator."""
        if op == '$gt': return val > expected
        if op == '$lt': return val < expected
        if op == '$gte': return val >= expected
        if op == '$lte': return val <= expected
        if op == '$eq': return val == expected
        if op == '$ne': return val != expected
        raise ValidationError(f"Unsupported operator: {op}")