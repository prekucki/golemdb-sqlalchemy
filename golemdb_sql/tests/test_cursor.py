"""Tests for cursor functionality."""

import pytest
from unittest.mock import Mock, patch
from golemdb_sql.cursor import Cursor
from golemdb_sql.exceptions import DatabaseError, InterfaceError


class TestCursor:
    """Test cursor functionality."""
    
    @pytest.fixture
    def mock_connection(self):
        """Create mock connection."""
        connection = Mock()
        connection._closed = False
        connection._check_connection.return_value = None
        connection._ensure_transaction.return_value = None
        return connection
    
    @pytest.fixture
    def cursor(self, mock_connection):
        """Create cursor with mock connection."""
        return Cursor(mock_connection)
    
    def test_cursor_initialization(self, cursor, mock_connection):
        """Test cursor initialization."""
        assert cursor._connection == mock_connection
        assert not cursor._closed
        assert cursor._results == []
        assert cursor._description is None
        assert cursor._rowcount == -1
        assert cursor._arraysize == 1
        assert cursor._rownumber is None
    
    def test_cursor_properties(self, cursor):
        """Test cursor properties."""
        # Test description property
        assert cursor.description is None
        cursor._description = [("id", "INTEGER", None, None, None, None, None)]
        assert cursor.description == [("id", "INTEGER", None, None, None, None, None)]
        
        # Test rowcount property
        assert cursor.rowcount == -1
        cursor._rowcount = 5
        assert cursor.rowcount == 5
        
        # Test arraysize property
        assert cursor.arraysize == 1
        cursor.arraysize = 10
        assert cursor.arraysize == 10
        
        with pytest.raises(ValueError, match="arraysize must be positive"):
            cursor.arraysize = 0
        
        # Test rownumber property
        assert cursor.rownumber is None
        cursor._rownumber = 2
        assert cursor.rownumber == 2
    
    def test_cursor_close(self, cursor):
        """Test cursor close."""
        cursor._results = [(1, "test")]
        cursor._description = [("id", "INTEGER")]
        cursor._rowcount = 1
        cursor._rownumber = 0
        
        cursor.close()
        
        assert cursor._closed
        assert cursor._results == []
        assert cursor._description is None
        assert cursor._rowcount == -1
        assert cursor._rownumber is None
    
    def test_check_cursor_closed(self, cursor):
        """Test operations on closed cursor."""
        cursor.close()
        
        with pytest.raises(InterfaceError, match="Cursor is closed"):
            cursor.execute("SELECT 1")
        
        with pytest.raises(InterfaceError, match="Cursor is closed"):
            cursor.fetchone()
    
    @patch.object(Cursor, '_execute_with_sdk')
    @patch.object(Cursor, '_process_result')
    def test_execute(self, mock_process, mock_execute, cursor):
        """Test execute method."""
        mock_execute.return_value = "mock_result"
        
        cursor.execute("SELECT * FROM users", {"id": 123})
        
        mock_execute.assert_called_once_with("SELECT * FROM users", {"id": 123})
        mock_process.assert_called_once_with("mock_result")
    
    def test_execute_error_handling(self, cursor):
        """Test execute error handling."""
        with patch.object(cursor, '_execute_with_sdk', side_effect=Exception("Test error")):
            with pytest.raises(DatabaseError, match="Error executing query"):
                cursor.execute("SELECT 1")
    
    @patch.object(Cursor, 'execute')
    def test_executemany(self, mock_execute, cursor):
        """Test executemany method."""
        # Mock execute to set rowcount
        def mock_execute_side_effect(operation, parameters):
            cursor._rowcount = 1
        
        mock_execute.side_effect = mock_execute_side_effect
        
        parameters_list = [
            {"name": "Alice"},
            {"name": "Bob"},
            {"name": "Charlie"}
        ]
        
        cursor.executemany("INSERT INTO users (name) VALUES (:name)", parameters_list)
        
        assert mock_execute.call_count == 3
        assert cursor._rowcount == 3  # Total of all executions
    
    def test_fetchone(self, cursor):
        """Test fetchone method."""
        # No results
        assert cursor.fetchone() is None
        
        # With results
        cursor._results = [(1, "Alice"), (2, "Bob"), (3, "Charlie")]
        cursor._rowcount = 3
        
        row = cursor.fetchone()
        assert row == (1, "Alice")
        assert len(cursor._results) == 2
        
        # Fetch remaining
        assert cursor.fetchone() == (2, "Bob")
        assert cursor.fetchone() == (3, "Charlie")
        assert cursor.fetchone() is None
    
    def test_fetchmany(self, cursor):
        """Test fetchmany method."""
        cursor._results = [(1, "Alice"), (2, "Bob"), (3, "Charlie"), (4, "David")]
        cursor._arraysize = 2
        
        # Default arraysize
        rows = cursor.fetchmany()
        assert rows == [(1, "Alice"), (2, "Bob")]
        assert len(cursor._results) == 2
        
        # Custom size
        rows = cursor.fetchmany(1)
        assert rows == [(3, "Charlie")]
        assert len(cursor._results) == 1
        
        # Fetch more than available
        rows = cursor.fetchmany(5)
        assert rows == [(4, "David")]
        assert len(cursor._results) == 0
        
        # Negative size
        with pytest.raises(ValueError, match="fetch size must be non-negative"):
            cursor.fetchmany(-1)
    
    def test_fetchall(self, cursor):
        """Test fetchall method."""
        cursor._results = [(1, "Alice"), (2, "Bob"), (3, "Charlie")]
        
        rows = cursor.fetchall()
        assert rows == [(1, "Alice"), (2, "Bob"), (3, "Charlie")]
        assert cursor._results == []
        
        # Second call returns empty list
        assert cursor.fetchall() == []
    
    def test_setinputsizes(self, cursor):
        """Test setinputsizes method (no-op)."""
        # Should not raise any exceptions
        cursor.setinputsizes([str, int, float])
    
    def test_setoutputsize(self, cursor):
        """Test setoutputsize method (no-op)."""
        # Should not raise any exceptions
        cursor.setoutputsize(100)
        cursor.setoutputsize(100, 0)
    
    def test_convert_parameters(self, cursor):
        """Test parameter conversion."""
        # None parameters
        assert cursor._convert_parameters(None) == {}
        
        # Dict parameters
        params = {"name": "Alice", "age": 30}
        assert cursor._convert_parameters(params) == params
        
        # List parameters
        params = ["Alice", 30]
        assert cursor._convert_parameters(params) == params
        
        # Tuple parameters
        params = ("Alice", 30)
        assert cursor._convert_parameters(params) == params
    
    def test_process_result_with_cursor_like(self, cursor):
        """Test processing result with cursor-like object."""
        mock_result = Mock()
        mock_result.fetchall.return_value = [
            ["1", "Alice"],
            ["2", "Bob"]
        ]
        mock_result.description = [
            ("id", "INTEGER", None, None, None, None, None),
            ("name", "VARCHAR", None, None, None, None, None)
        ]
        
        cursor._process_result(mock_result)
        
        assert cursor._results == [("1", "Alice"), ("2", "Bob")]
        assert cursor._rowcount == 2
        assert cursor._description == mock_result.description
    
    def test_process_result_with_rows_attribute(self, cursor):
        """Test processing result with rows attribute."""
        mock_result = Mock()
        mock_result.rows = [("1", "Alice"), ("2", "Bob")]
        mock_result.columns = ["id", "name"]
        
        cursor._process_result(mock_result)
        
        assert cursor._results == [("1", "Alice"), ("2", "Bob")]
        assert cursor._rowcount == 2
        assert len(cursor._description) == 2
        assert cursor._description[0][0] == "id"
        assert cursor._description[1][0] == "name"
    
    def test_process_result_with_list(self, cursor):
        """Test processing result with direct list."""
        result = [("1", "Alice"), ("2", "Bob")]
        
        cursor._process_result(result)
        
        assert cursor._results == [("1", "Alice"), ("2", "Bob")]
        assert cursor._rowcount == 2
    
    def test_process_result_with_rowcount(self, cursor):
        """Test processing result with rowcount (non-SELECT)."""
        mock_result = Mock()
        mock_result.rowcount = 5
        
        cursor._process_result(mock_result)
        
        assert cursor._results == []
        assert cursor._rowcount == 5
    
    def test_convert_description(self, cursor):
        """Test description conversion."""
        # None description
        assert cursor._convert_description(None) is None
        
        # Already compatible format
        sdk_desc = [
            ("id", "INTEGER", None, None, None, None, None),
            ("name", "VARCHAR", 100, None, None, None, True)
        ]
        result = cursor._convert_description(sdk_desc)
        assert result == sdk_desc
        
        # Column objects with attributes
        mock_col1 = Mock()
        mock_col1.name = "id"
        mock_col1.type = "INTEGER"
        
        mock_col2 = Mock()
        mock_col2.name = "name"
        mock_col2.type_code = "VARCHAR"
        mock_col2.display_size = 100
        
        sdk_desc = [mock_col1, mock_col2]
        result = cursor._convert_description(sdk_desc)
        
        assert len(result) == 2
        assert result[0][0] == "id"
        assert result[0][1] == "INTEGER"
        assert result[1][0] == "name"
        assert result[1][1] == "VARCHAR"
        assert result[1][2] == 100
    
    def test_build_description_from_columns(self, cursor):
        """Test building description from column information."""
        # None columns
        assert cursor._build_description_from_columns(None) is None
        
        # String column names
        columns = ["id", "name", "email"]
        result = cursor._build_description_from_columns(columns)
        
        assert len(result) == 3
        assert result[0][0] == "id"
        assert result[1][0] == "name" 
        assert result[2][0] == "email"
        
        # Dictionary column info
        columns = [
            {"name": "id", "type": "INTEGER", "null_ok": False},
            {"name": "name", "type": "VARCHAR", "display_size": 100}
        ]
        result = cursor._build_description_from_columns(columns)
        
        assert len(result) == 2
        assert result[0][0] == "id"
        assert result[0][1] == "INTEGER"
        assert result[0][6] == False  # null_ok
        assert result[1][0] == "name"
        assert result[1][1] == "VARCHAR"
        assert result[1][2] == 100  # display_size
    
    def test_update_rownumber(self, cursor):
        """Test row number update."""
        cursor._rowcount = 5
        cursor._results = [(1, "a"), (2, "b"), (3, "c")]
        
        cursor._update_rownumber()
        assert cursor._rownumber == 1  # 5 - 3 - 1 = 1
        
        cursor._results = []
        cursor._update_rownumber()
        assert cursor._rownumber == 4  # 5 - 0 - 1 = 4
    
    def test_iterator_protocol(self, cursor):
        """Test cursor iterator protocol."""
        cursor._results = [(1, "Alice"), (2, "Bob"), (3, "Charlie")]
        cursor._rowcount = 3
        
        # Test iterator
        assert iter(cursor) is cursor
        
        # Test iteration
        rows = list(cursor)
        assert rows == [(1, "Alice"), (2, "Bob"), (3, "Charlie")]
        assert cursor._results == []
        
        # Second iteration should be empty
        assert list(cursor) == []
    
    def test_iterator_empty_results(self, cursor):
        """Test iterator with empty results."""
        cursor._results = []
        
        rows = list(cursor)
        assert rows == []
    
    @patch.object(Cursor, '_execute_with_sdk')
    def test_execute_with_sdk_integration(self, mock_execute, cursor):
        """Test _execute_with_sdk method."""
        # Test when connection has execute method
        mock_sdk_connection = Mock()
        mock_sdk_connection.execute.return_value = "result"
        cursor._connection._connection = mock_sdk_connection
        
        result = cursor._execute_with_sdk("SELECT 1", None)
        assert result == "result"
        mock_sdk_connection.execute.assert_called_once_with("SELECT 1", {})
        
        # Test when connection has query method
        mock_sdk_connection = Mock()
        mock_sdk_connection.query.return_value = "query_result"
        del mock_sdk_connection.execute  # Remove execute method
        cursor._connection._connection = mock_sdk_connection
        
        result = cursor._execute_with_sdk("SELECT 1", None)
        assert result == "query_result"
        mock_sdk_connection.query.assert_called_once_with("SELECT 1", {})
        
        # Test when connection has cursor method
        mock_sdk_connection = Mock()
        mock_sdk_cursor = Mock()
        mock_sdk_connection.cursor.return_value = mock_sdk_cursor
        del mock_sdk_connection.query  # Remove query method
        cursor._connection._connection = mock_sdk_connection
        
        result = cursor._execute_with_sdk("SELECT 1", {"param": "value"})
        assert result == mock_sdk_cursor
        mock_sdk_connection.cursor.assert_called_once()
        mock_sdk_cursor.execute.assert_called_once_with("SELECT 1", {"param": "value"})
        
        # Test when no compatible method exists
        mock_sdk_connection = Mock(spec=[])  # Empty spec means no methods
        cursor._connection._connection = mock_sdk_connection
        
        with pytest.raises(NotImplementedError, match="golem-base-sdk does not provide"):
            cursor._execute_with_sdk("SELECT 1", None)