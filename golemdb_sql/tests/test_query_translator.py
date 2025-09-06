"""Tests for SQL query translation functionality."""

import pytest
from unittest.mock import Mock, patch
from golemdb_sql.query_translator import QueryTranslator, QueryResult
from golemdb_sql.schema_manager import SchemaManager, TableDefinition, ColumnDefinition
from golemdb_sql.exceptions import ProgrammingError


class TestQueryTranslator:
    """Test SQL query translation functionality."""
    
    @pytest.fixture
    def mock_schema_manager(self):
        """Create mock schema manager."""
        schema_manager = Mock(spec=SchemaManager)
        
        # Create test table definitions
        users_table = TableDefinition(
            name="users",
            columns=[
                ColumnDefinition(name="id", type="INTEGER", primary_key=True, indexed=True),
                ColumnDefinition(name="name", type="VARCHAR(100)", nullable=False, indexed=True),
                ColumnDefinition(name="email", type="VARCHAR(255)", unique=True, indexed=True),
                ColumnDefinition(name="age", type="INTEGER", indexed=True),
                ColumnDefinition(name="is_active", type="BOOLEAN", indexed=True),
                ColumnDefinition(name="created_at", type="DATETIME", indexed=True)
            ],
            indexes=[],
            foreign_keys=[]
        )
        
        posts_table = TableDefinition(
            name="posts",
            columns=[
                ColumnDefinition(name="id", type="INTEGER", primary_key=True, indexed=True),
                ColumnDefinition(name="user_id", type="INTEGER", indexed=True),
                ColumnDefinition(name="title", type="VARCHAR(200)", indexed=True),
                ColumnDefinition(name="content", type="TEXT"),
                ColumnDefinition(name="published", type="BOOLEAN", indexed=True)
            ],
            indexes=[],
            foreign_keys=[]
        )
        
        def get_table_side_effect(table_name):
            tables = {"users": users_table, "posts": posts_table}
            return tables.get(table_name)
        
        schema_manager.get_table.side_effect = get_table_side_effect
        schema_manager.get_ttl_for_table.return_value = 86400
        
        return schema_manager
    
    @pytest.fixture
    def translator(self, mock_schema_manager):
        """Create query translator with mock schema manager."""
        return QueryTranslator(mock_schema_manager)
    
    def test_translate_select_basic(self, translator):
        """Test basic SELECT query translation."""
        sql = "SELECT id, name, email FROM users"
        result = translator.translate_select(sql)
        
        assert result.operation_type == "SELECT"
        assert result.table_name == "users"
        assert set(result.columns) == {"id", "name", "email"}
        assert result.golem_query == 'table="users"'
        assert result.limit is None
        assert result.offset is None
    
    def test_translate_select_with_where(self, translator):
        """Test SELECT query with WHERE clause."""
        sql = "SELECT * FROM users WHERE age > 25 AND is_active = TRUE"
        result = translator.translate_select(sql)
        
        assert result.operation_type == "SELECT"
        assert result.table_name == "users"
        assert result.columns == ["*"]
        assert "age>25" in result.golem_query
        assert "is_active=1" in result.golem_query
        assert "&&" in result.golem_query
    
    def test_translate_select_with_parameters(self, translator):
        """Test SELECT query with parameters."""
        sql = "SELECT * FROM users WHERE age > :age AND name = :name"
        parameters = {"age": 30, "name": "John"}
        
        result = translator.translate_select(sql, parameters)
        
        assert "age>30" in result.golem_query
        assert 'name="John"' in result.golem_query
    
    def test_translate_select_with_limit_offset(self, translator):
        """Test SELECT query with LIMIT and OFFSET."""
        sql = "SELECT * FROM users ORDER BY name LIMIT 10 OFFSET 20"
        result = translator.translate_select(sql)
        
        assert result.limit == 10
        assert result.offset == 20
        assert result.sort_by == "name"
        assert result.sort_order == "asc"
    
    def test_translate_select_with_order_by_desc(self, translator):
        """Test SELECT query with ORDER BY DESC."""
        sql = "SELECT * FROM users ORDER BY created_at DESC"
        result = translator.translate_select(sql)
        
        assert result.sort_by == "created_at"
        assert result.sort_order == "desc"
    
    def test_translate_insert(self, translator):
        """Test INSERT query translation."""
        sql = "INSERT INTO users (name, email, age) VALUES ('John', 'john@example.com', 30)"
        result = translator.translate_insert(sql)
        
        assert result.operation_type == "INSERT"
        assert result.table_name == "users"
        assert result.insert_data["name"] == "John"
        assert result.insert_data["email"] == "john@example.com"
        assert result.insert_data["age"] == 30
    
    def test_translate_insert_with_parameters(self, translator):
        """Test INSERT query with parameters."""
        sql = "INSERT INTO users (name, email, age) VALUES (:name, :email, :age)"
        parameters = {"name": "Jane", "email": "jane@example.com", "age": 25}
        
        result = translator.translate_insert(sql, parameters)
        
        assert result.insert_data["name"] == "Jane"
        assert result.insert_data["email"] == "jane@example.com"
        assert result.insert_data["age"] == 25
    
    def test_translate_update(self, translator):
        """Test UPDATE query translation."""
        sql = "UPDATE users SET name = 'Jane', age = 26 WHERE id = 1"
        result = translator.translate_update(sql)
        
        assert result.operation_type == "UPDATE"
        assert result.table_name == "users"
        assert result.update_data["name"] == "Jane"
        assert result.update_data["age"] == 26
        assert 'id=1' in result.golem_query
    
    def test_translate_update_with_parameters(self, translator):
        """Test UPDATE query with parameters."""
        sql = "UPDATE users SET name = :name WHERE id = :user_id"
        parameters = {"name": "Updated Name", "user_id": 123}
        
        result = translator.translate_update(sql, parameters)
        
        assert result.update_data["name"] == "Updated Name"
        assert 'id=123' in result.golem_query
    
    def test_translate_delete(self, translator):
        """Test DELETE query translation."""
        sql = "DELETE FROM users WHERE age < 18"
        result = translator.translate_delete(sql)
        
        assert result.operation_type == "DELETE"
        assert result.table_name == "users"
        assert 'age<18' in result.golem_query
    
    def test_translate_delete_with_parameters(self, translator):
        """Test DELETE query with parameters."""
        sql = "DELETE FROM users WHERE id = :user_id"
        parameters = {"user_id": 456}
        
        result = translator.translate_delete(sql, parameters)
        
        assert 'id=456' in result.golem_query
    
    def test_complex_where_conditions(self, translator):
        """Test complex WHERE conditions."""
        sql = """
        SELECT * FROM users 
        WHERE (age BETWEEN 25 AND 65) 
        AND (name LIKE '%john%' OR email LIKE '%@company.com') 
        AND is_active = TRUE
        """
        
        result = translator.translate_select(sql)
        
        # Should contain all conditions
        query = result.golem_query
        assert 'age>=25' in query
        assert 'age<=65' in query
        assert 'is_active=1' in query
    
    def test_in_operator(self, translator):
        """Test IN operator translation."""
        sql = "SELECT * FROM users WHERE age IN (25, 30, 35)"
        result = translator.translate_select(sql)
        
        # IN should be converted to multiple OR conditions
        query = result.golem_query
        assert 'age=25' in query
        assert 'age=30' in query
        assert 'age=35' in query
        assert '||' in query  # OR operator
    
    def test_is_null_operator(self, translator):
        """Test IS NULL operator translation."""
        sql = "SELECT * FROM users WHERE email IS NULL"
        result = translator.translate_select(sql)
        
        # IS NULL should be handled appropriately
        assert 'email' in result.golem_query.lower()
    
    def test_like_operator_basic(self, translator, mock_schema_manager):
        """Test basic LIKE operator translation to ~ glob operator.""" 
        mock_schema_manager.project_id = "test_project"
        mock_schema_manager.table_exists.return_value = True
        
        # Test prefix pattern
        sql = "SELECT * FROM users WHERE name LIKE 'John%'"
        result = translator.translate_select(sql)
        
        # Should convert % to * for glob pattern
        assert 'idx_name ~ "John*"' in result.golem_query
        assert result.table_name == "users"
        
    def test_like_operator_patterns(self, translator, mock_schema_manager):
        """Test various LIKE patterns converted to glob patterns."""
        mock_schema_manager.project_id = "test_project"
        mock_schema_manager.table_exists.return_value = True
        
        test_cases = [
            # (SQL LIKE pattern, Expected glob pattern)
            ('John%', 'John*'),           # Prefix
            ('%Smith', '*Smith'),         # Suffix  
            ('%middle%', '*middle*'),     # Contains
            ('J_hn', 'J?hn'),            # Single char wildcard
            ('John', 'John'),             # Exact match (no wildcards)
            ('_%_%', '?*?*'),            # Mixed wildcards
        ]
        
        for like_pattern, expected_glob in test_cases:
            sql = f"SELECT * FROM users WHERE name LIKE '{like_pattern}'"
            result = translator.translate_select(sql)
            
            expected_query_part = f'idx_name ~ "{expected_glob}"'
            assert expected_query_part in result.golem_query, f"Failed for pattern {like_pattern}"
    
    def test_like_operator_escaped_chars(self, translator, mock_schema_manager):
        """Test LIKE operator with escaped special characters."""
        mock_schema_manager.project_id = "test_project"  
        mock_schema_manager.table_exists.return_value = True
        
        test_cases = [
            # (SQL LIKE pattern, Expected glob pattern)
            (r'file\%name', 'file[%]name'),      # Escaped %
            (r'file\_name', 'file_name'),        # Escaped _
            (r'path\\file', r'path\file'),       # Escaped backslash
            ('file*name', 'file[*]name'),        # Literal * (glob special char)
            ('file?name', 'file[?]name'),        # Literal ? (glob special char) 
            ('file[test]', 'file[[]test]'),      # Literal [ (glob special char)
        ]
        
        for like_pattern, expected_glob in test_cases:
            sql = f"SELECT * FROM users WHERE name LIKE '{like_pattern}'"
            result = translator.translate_select(sql)
            
            expected_query_part = f'idx_name ~ "{expected_glob}"'
            assert expected_query_part in result.golem_query, f"Failed for pattern {like_pattern}"
            
    def test_like_operator_non_indexed_column(self, translator, mock_schema_manager):
        """Test LIKE operator on non-indexed columns (should use post-filtering)."""
        mock_schema_manager.project_id = "test_project"
        mock_schema_manager.table_exists.return_value = True
        
        # Test with 'content' column which is not indexed in posts table
        sql = "SELECT * FROM posts WHERE content LIKE '%important%'"
        result = translator.translate_select(sql)
        
        # Should not be in the main GolemBase query (only relation filter)
        assert 'content' not in result.golem_query
        assert 'relation="test_project.posts"' in result.golem_query
        
        # Should be in post-filter conditions
        assert result.post_filter_conditions is not None
        assert len(result.post_filter_conditions) == 1
        
        condition = result.post_filter_conditions[0]
        assert condition['column'] == 'content'
        assert condition['operator'] == 'LIKE'
        assert condition['value'] == '%important%'
    
    def test_unsupported_sql_statement(self, translator):
        """Test error handling for unsupported SQL statements."""
        with pytest.raises(ProgrammingError, match="Unsupported SQL statement type"):
            translator.translate("CREATE TABLE test (id INTEGER)")
    
    def test_table_not_found(self, translator, mock_schema_manager):
        """Test error handling when table not found."""
        mock_schema_manager.get_table.return_value = None
        
        with pytest.raises(ProgrammingError, match="Table 'nonexistent' not found"):
            translator.translate_select("SELECT * FROM nonexistent")
    
    def test_invalid_sql_parsing(self, translator):
        """Test error handling for invalid SQL."""
        with pytest.raises(ProgrammingError, match="Failed to parse SQL"):
            translator.translate("INVALID SQL STATEMENT")
    
    def test_join_query_error(self, translator):
        """Test error handling for JOIN queries (not supported)."""
        sql = "SELECT * FROM users u JOIN posts p ON u.id = p.user_id"
        
        with pytest.raises(ProgrammingError, match="JOIN queries are not supported"):
            translator.translate_select(sql)
    
    def test_subquery_error(self, translator):
        """Test error handling for subqueries (not supported)."""
        sql = "SELECT * FROM users WHERE id IN (SELECT user_id FROM posts)"
        
        with pytest.raises(ProgrammingError, match="Subqueries are not supported"):
            translator.translate_select(sql)
    
    def test_aggregate_functions_error(self, translator):
        """Test error handling for aggregate functions (not supported)."""
        sql = "SELECT COUNT(*) FROM users"
        
        with pytest.raises(ProgrammingError, match="Aggregate functions are not supported"):
            translator.translate_select(sql)
    
    def test_convert_sql_operator_to_golem(self, translator):
        """Test SQL operator conversion to GolemBase format."""
        # Test various operators
        assert translator._convert_sql_operator_to_golem("=") == "="
        assert translator._convert_sql_operator_to_golem("!=") == "!="
        assert translator._convert_sql_operator_to_golem("<>") == "!="
        assert translator._convert_sql_operator_to_golem(">") == ">"
        assert translator._convert_sql_operator_to_golem(">=") == ">="
        assert translator._convert_sql_operator_to_golem("<") == "<"
        assert translator._convert_sql_operator_to_golem("<=") == "<="
    
    def test_format_value_for_annotation(self, translator):
        """Test value formatting for annotation queries."""
        # String values should be quoted
        assert translator._format_value_for_annotation("test") == '"test"'
        
        # Numeric values should not be quoted
        assert translator._format_value_for_annotation(123) == "123"
        assert translator._format_value_for_annotation(123.45) == "123.45"
        
        # Boolean values should be converted to 0/1
        assert translator._format_value_for_annotation(True) == "1"
        assert translator._format_value_for_annotation(False) == "0"
        
        # None should be handled
        assert translator._format_value_for_annotation(None) == "null"
    
    def test_extract_columns_from_select(self, translator):
        """Test column extraction from SELECT statements."""
        # Test wildcard
        columns = translator._extract_columns_from_select("SELECT * FROM users")
        assert columns == ["*"]
        
        # Test specific columns
        columns = translator._extract_columns_from_select("SELECT id, name, email FROM users")
        assert set(columns) == {"id", "name", "email"}
        
        # Test columns with aliases
        columns = translator._extract_columns_from_select("SELECT id, name AS full_name FROM users")
        assert "id" in columns
        # Alias handling may vary based on implementation
    
    def test_parameter_substitution(self, translator):
        """Test parameter substitution in queries."""
        sql = "SELECT * FROM users WHERE name = :name AND age = :age"
        parameters = {"name": "John", "age": 30}
        
        substituted = translator._substitute_parameters(sql, parameters)
        assert ":name" not in substituted
        assert ":age" not in substituted
        assert "John" in substituted
        assert "30" in substituted
    
    def test_named_and_numeric_parameters(self, translator):
        """Test both named (:name) and numeric (?) parameters."""
        # Named parameters
        sql = "SELECT * FROM users WHERE name = :name"
        parameters = {"name": "John"}
        result = translator.translate_select(sql, parameters)
        assert 'name="John"' in result.golem_query
        
        # List parameters (positional)
        sql = "SELECT * FROM users WHERE name = ? AND age = ?"
        parameters = ["Jane", 25]
        result = translator.translate_select(sql, parameters)
        assert 'name="Jane"' in result.golem_query
        assert 'age=25' in result.golem_query