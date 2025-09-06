"""GolemBase filter evaluation utilities.

This module provides functionality for applying post-filter conditions
to query results for non-indexed columns that cannot be handled at the
GolemBase annotation query level.
"""

import re
from typing import Dict, List, Any


def _match_like_pattern(text: str, pattern: str) -> bool:
    """Match text against SQL LIKE pattern.
    
    Args:
        text: Text to match against
        pattern: SQL LIKE pattern with % and _ wildcards
        
    Returns:
        True if text matches pattern, False otherwise
    """
    # Convert SQL LIKE pattern to regex
    # Process character by character to handle escaping properly
    regex_chars = []
    i = 0
    while i < len(pattern):
        char = pattern[i]
        
        if char == '%':
            # % matches zero or more characters
            regex_chars.append('.*')
        elif char == '_':
            # _ matches exactly one character  
            regex_chars.append('.')
        elif char == '\\' and i + 1 < len(pattern):
            # Handle escaped characters
            next_char = pattern[i + 1]
            if next_char == '%':
                # Escaped % - literal percent
                regex_chars.append('%')
            elif next_char == '_':
                # Escaped _ - literal underscore
                regex_chars.append('_') 
            elif next_char == '\\':
                # Escaped backslash - literal backslash
                regex_chars.append('\\\\')
            else:
                # Other escaped character - treat as literal
                regex_chars.append(re.escape(next_char))
            i += 1  # Skip the next character
        else:
            # Regular character - escape if it's special in regex
            regex_chars.append(re.escape(char))
        
        i += 1
    
    # Join and anchor to match entire string
    regex_pattern = '^' + ''.join(regex_chars) + '$'
    
    return bool(re.match(regex_pattern, text))


def apply_post_filter(row_data: Dict[str, Any], conditions: List[Dict[str, Any]]) -> bool:
    """Apply post-filter conditions to a row for non-indexed columns.
    
    This function evaluates conditions that cannot be handled by GolemBase
    annotation queries, typically for non-indexed columns or complex
    expressions that require row-level evaluation.
    
    Args:
        row_data: Deserialized row data from GolemBase entity
        conditions: List of filter conditions, each containing:
            - column: Column name to filter on
            - operator: Comparison operator ('=', '<', '<=', '>', '>=', '!=', 'LIKE')
            - value: Expected value to compare against
            - column_type: SQL column type for proper type conversion
            
    Returns:
        True if row matches all conditions, False otherwise
        
    Examples:
        >>> row = {'age': 25, 'name': 'Alice', 'active': True}
        >>> conditions = [
        ...     {'column': 'age', 'operator': '>', 'value': 18, 'column_type': 'INTEGER'},
        ...     {'column': 'active', 'operator': '=', 'value': True, 'column_type': 'BOOLEAN'}
        ... ]
        >>> apply_post_filter(row, conditions)
        True
    """
    for condition in conditions:
        column = condition['column']
        operator = condition['operator']  
        expected_value = condition['value']
        column_type = condition['column_type']
        
        # Get actual value from row data
        actual_value = row_data.get(column)
        if actual_value is None:
            return False  # NULL values don't match any condition
        
        # Type conversion based on column type
        if column_type.upper() in ('INTEGER', 'INT', 'BIGINT', 'SMALLINT', 'TINYINT'):
            try:
                actual_value = int(actual_value)
                expected_value = int(expected_value)
            except (ValueError, TypeError):
                return False
        elif column_type.upper() in ('BOOLEAN', 'BOOL'):
            actual_value = bool(actual_value)
            expected_value = bool(expected_value)
        
        # Apply operator
        if operator == '=':
            if actual_value != expected_value:
                return False
        elif operator == '<':
            if actual_value >= expected_value:
                return False
        elif operator == '<=':
            if actual_value > expected_value:
                return False
        elif operator == '>':
            if actual_value <= expected_value:
                return False
        elif operator == '>=':
            if actual_value < expected_value:
                return False
        elif operator == '!=':
            if actual_value == expected_value:
                return False
        elif operator == 'LIKE':
            # SQL LIKE pattern matching for non-indexed columns
            if not isinstance(actual_value, str) or not isinstance(expected_value, str):
                return False
            if not _match_like_pattern(str(actual_value), expected_value):
                return False
        else:
            # Unsupported operator
            return False
    
    return True  # All conditions matched


def evaluate_filter_conditions(rows: List[Dict[str, Any]], conditions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Apply post-filter conditions to a list of rows.
    
    Args:
        rows: List of deserialized row data
        conditions: List of filter conditions
        
    Returns:
        Filtered list of rows that match all conditions
    """
    if not conditions:
        return rows
    
    return [row for row in rows if apply_post_filter(row, conditions)]


def has_post_filter_conditions(query_result) -> bool:
    """Check if a query result has post-filter conditions to apply.
    
    Args:
        query_result: QueryResult object from query translator
        
    Returns:
        True if post-filter conditions exist, False otherwise
    """
    return (hasattr(query_result, 'post_filter_conditions') and 
            query_result.post_filter_conditions and 
            len(query_result.post_filter_conditions) > 0)