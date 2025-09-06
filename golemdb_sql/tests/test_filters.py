"""Tests for filter evaluation functionality."""

import pytest
from golemdb_sql.filters import apply_post_filter, evaluate_filter_conditions, _match_like_pattern


class TestLikePatternMatching:
    """Test SQL LIKE pattern matching functionality."""
    
    def test_simple_patterns(self):
        """Test basic LIKE patterns."""
        # Exact match
        assert _match_like_pattern("hello", "hello") == True
        assert _match_like_pattern("hello", "world") == False
        
        # Prefix matching with %
        assert _match_like_pattern("hello world", "hello%") == True
        assert _match_like_pattern("hello", "hello%") == True
        assert _match_like_pattern("hell", "hello%") == False
        
        # Suffix matching with %
        assert _match_like_pattern("hello world", "%world") == True
        assert _match_like_pattern("world", "%world") == True
        assert _match_like_pattern("worlds", "%world") == False
        
        # Contains matching with %
        assert _match_like_pattern("hello world test", "%world%") == True
        assert _match_like_pattern("world", "%world%") == True
        assert _match_like_pattern("hello test", "%world%") == False
        
    def test_single_character_wildcard(self):
        """Test single character wildcard (_) patterns."""
        assert _match_like_pattern("cat", "c_t") == True
        assert _match_like_pattern("cot", "c_t") == True
        assert _match_like_pattern("cut", "c_t") == True
        assert _match_like_pattern("cart", "c_t") == False
        assert _match_like_pattern("ct", "c_t") == False
        
    def test_mixed_wildcards(self):
        """Test patterns with both % and _ wildcards."""
        assert _match_like_pattern("hello world", "h_llo%") == True
        assert _match_like_pattern("hallo world test", "h_llo%") == True
        assert _match_like_pattern("hello", "h_llo%") == True
        assert _match_like_pattern("hllo world", "h_llo%") == False
        
    def test_escaped_characters(self):
        """Test escaped special characters in patterns."""
        # Note: In SQL LIKE, \% and \_ are escaped literals
        # Our regex conversion should handle these correctly
        assert _match_like_pattern("file%name", r"file\%name") == True
        assert _match_like_pattern("filename", r"file\%name") == False
        
        assert _match_like_pattern("file_name", r"file\_name") == True
        assert _match_like_pattern("filename", r"file\_name") == False
        
    def test_edge_cases(self):
        """Test edge cases for LIKE pattern matching."""
        # Empty patterns
        assert _match_like_pattern("", "") == True
        assert _match_like_pattern("test", "") == False
        assert _match_like_pattern("", "%") == True
        assert _match_like_pattern("", "_") == False
        
        # Just wildcards
        assert _match_like_pattern("anything", "%") == True
        assert _match_like_pattern("a", "_") == True
        assert _match_like_pattern("ab", "_") == False
        

class TestPostFilterConditions:
    """Test post-filter condition evaluation."""
    
    def test_like_condition_matching(self):
        """Test LIKE condition evaluation in post-filtering."""
        row_data = {
            'name': 'John Smith',
            'email': 'john.smith@example.com',
            'description': 'Software engineer at ACME Corp'
        }
        
        # Test various LIKE conditions
        test_cases = [
            # (column, pattern, expected_result)
            ('name', 'John%', True),
            ('name', '%Smith', True), 
            ('name', '%Doe%', False),
            ('email', '%.com', True),
            ('email', 'john%', True),
            ('email', '%@example.%', True),
            ('description', '%engineer%', True),
            ('description', '%ACME%', True),
            ('description', '%Microsoft%', False),
        ]
        
        for column, pattern, expected in test_cases:
            conditions = [{'column': column, 'operator': 'LIKE', 'value': pattern, 'column_type': 'VARCHAR'}]
            result = apply_post_filter(row_data, conditions)
            assert result == expected, f"Failed for {column} LIKE {pattern}"
    
    def test_like_with_other_operators(self):
        """Test LIKE combined with other operators.""" 
        row_data = {
            'name': 'Alice Johnson',
            'age': 30,
            'department': 'Engineering'
        }
        
        # Multiple conditions: LIKE + equality + comparison
        conditions = [
            {'column': 'name', 'operator': 'LIKE', 'value': 'Alice%', 'column_type': 'VARCHAR'},
            {'column': 'age', 'operator': '>=', 'value': 25, 'column_type': 'INTEGER'},
            {'column': 'department', 'operator': '=', 'value': 'Engineering', 'column_type': 'VARCHAR'}
        ]
        
        assert apply_post_filter(row_data, conditions) == True
        
        # Change one condition to make it fail
        conditions[1]['value'] = 35  # age >= 35 (should fail)
        assert apply_post_filter(row_data, conditions) == False
        
    def test_like_non_string_values(self):
        """Test LIKE operator with non-string values (should fail)."""
        row_data = {'age': 30, 'active': True}
        
        # LIKE on non-string column should return False
        conditions = [{'column': 'age', 'operator': 'LIKE', 'value': '3%', 'column_type': 'INTEGER'}]
        assert apply_post_filter(row_data, conditions) == False
        
        conditions = [{'column': 'active', 'operator': 'LIKE', 'value': 'true%', 'column_type': 'BOOLEAN'}]
        assert apply_post_filter(row_data, conditions) == False
        
    def test_like_null_values(self):
        """Test LIKE operator with NULL values."""
        row_data = {'name': None, 'email': 'test@example.com'}
        
        # LIKE on NULL column should return False
        conditions = [{'column': 'name', 'operator': 'LIKE', 'value': '%', 'column_type': 'VARCHAR'}]
        assert apply_post_filter(row_data, conditions) == False
        
        # LIKE on existing column should work normally
        conditions = [{'column': 'email', 'operator': 'LIKE', 'value': 'test%', 'column_type': 'VARCHAR'}]
        assert apply_post_filter(row_data, conditions) == True


class TestFilterConditionEvaluation:
    """Test filter condition evaluation for lists of rows."""
    
    def test_evaluate_multiple_rows(self):
        """Test filtering multiple rows with LIKE conditions."""
        rows = [
            {'name': 'John Smith', 'email': 'john@company.com', 'role': 'Developer'},
            {'name': 'Jane Doe', 'email': 'jane@company.com', 'role': 'Manager'},
            {'name': 'Bob Johnson', 'email': 'bob@external.com', 'role': 'Contractor'},
            {'name': 'Alice Brown', 'email': 'alice@company.com', 'role': 'Developer'}
        ]
        
        # Filter for company emails only
        conditions = [
            {'column': 'email', 'operator': 'LIKE', 'value': '%@company.com', 'column_type': 'VARCHAR'}
        ]
        
        filtered = evaluate_filter_conditions(rows, conditions)
        assert len(filtered) == 3
        assert all(row['email'].endswith('@company.com') for row in filtered)
        
        # Filter for developers at company
        conditions = [
            {'column': 'email', 'operator': 'LIKE', 'value': '%@company.com', 'column_type': 'VARCHAR'},
            {'column': 'role', 'operator': '=', 'value': 'Developer', 'column_type': 'VARCHAR'}
        ]
        
        filtered = evaluate_filter_conditions(rows, conditions)
        assert len(filtered) == 2
        assert all(row['role'] == 'Developer' and row['email'].endswith('@company.com') for row in filtered)
        
    def test_no_conditions(self):
        """Test that no conditions returns all rows."""
        rows = [{'id': 1}, {'id': 2}, {'id': 3}]
        filtered = evaluate_filter_conditions(rows, [])
        assert len(filtered) == 3
        assert filtered == rows
        
    def test_no_matching_rows(self):
        """Test that non-matching conditions return empty list."""
        rows = [
            {'name': 'John Smith', 'email': 'john@company.com'},
            {'name': 'Jane Doe', 'email': 'jane@company.com'}
        ]
        
        conditions = [
            {'column': 'email', 'operator': 'LIKE', 'value': '%@external.com', 'column_type': 'VARCHAR'}
        ]
        
        filtered = evaluate_filter_conditions(rows, conditions)
        assert len(filtered) == 0