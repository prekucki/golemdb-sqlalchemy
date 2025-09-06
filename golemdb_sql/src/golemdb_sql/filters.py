"""GolemBase filter evaluation utilities.

This module provides functionality for applying post-filter conditions
to query results for non-indexed columns that cannot be handled at the
GolemBase annotation query level.
"""

from typing import Dict, List, Any


def apply_post_filter(row_data: Dict[str, Any], conditions: List[Dict[str, Any]]) -> bool:
    """Apply post-filter conditions to a row for non-indexed columns.
    
    This function evaluates conditions that cannot be handled by GolemBase
    annotation queries, typically for non-indexed columns or complex
    expressions that require row-level evaluation.
    
    Args:
        row_data: Deserialized row data from GolemBase entity
        conditions: List of filter conditions, each containing:
            - column: Column name to filter on
            - operator: Comparison operator ('=', '<', '<=', '>', '>=', '!=')
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