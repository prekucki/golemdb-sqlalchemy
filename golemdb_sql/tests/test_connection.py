"""Tests for database connection functionality."""

import pytest
import asyncio
import threading
from unittest.mock import Mock, patch, AsyncMock
from golemdb_sql.connection import Connection, connect
from golemdb_sql.cursor import Cursor
from golemdb_sql.exceptions import DatabaseError, InterfaceError, ProgrammingError
from .mock_golem_client import MockGolemBaseClient


class TestConnection:
    """Test database connection functionality."""
    
    @pytest.fixture
    def mock_connection_params(self):
        """Create mock connection parameters."""
        return {
            "rpc_url": "https://test.golembase.com/rpc",
            "ws_url": "wss://test.golembase.com/ws", 
            "private_key": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            "app_id": "testapp",
            "schema_id": "testschema"
        }
    
    @patch('golemdb_sql.connection.GolemBaseClient', MockGolemBaseClient)
    @patch('golemdb_sql.connection.parse_connection_kwargs')
    def test_connection_initialization(self, mock_parse, mock_connection_params):
        """Test connection initialization."""
        from golemdb_sql.connection_parser import GolemBaseConnectionParams
        
        mock_params = GolemBaseConnectionParams(**mock_connection_params)
        mock_parse.return_value = mock_params
        
        conn = Connection(**mock_connection_params)
        
        assert not conn._closed
        assert not conn._autocommit
        assert not conn._in_transaction
        assert conn._params == mock_params
        # Client should be initialized
        assert conn._client is not None
    
    @patch('golemdb_sql.connection.GolemBaseClient', MockGolemBaseClient)
    @patch('golemdb_sql.connection.parse_connection_kwargs')
    def test_connection_close(self, mock_parse, mock_connection_params):
        """Test connection close."""
        from golemdb_sql.connection_parser import GolemBaseConnectionParams
        
        mock_params = GolemBaseConnectionParams(**mock_connection_params)
        mock_parse.return_value = mock_params
        
        conn = Connection(**mock_connection_params)
        assert not conn.closed
        
        conn.close()
        assert conn.closed
        
        # Second close should be safe
        conn.close()
        assert conn.closed
    
    @patch('golemdb_sql.connection.GolemBaseClient', MockGolemBaseClient)
    @patch('golemdb_sql.connection.parse_connection_kwargs')
    def test_cursor_creation(self, mock_parse, mock_connection_params):
        """Test cursor creation."""
        from golemdb_sql.connection_parser import GolemBaseConnectionParams
        
        mock_params = GolemBaseConnectionParams(**mock_connection_params)
        mock_parse.return_value = mock_params
        
        conn = Connection(**mock_connection_params)
        cursor = conn.cursor()
        
        assert isinstance(cursor, Cursor)
        assert cursor.connection == conn
    
    @patch('golemdb_sql.connection.GolemBaseClient', MockGolemBaseClient)
    @patch('golemdb_sql.connection.parse_connection_kwargs')
    def test_cursor_creation_closed_connection(self, mock_parse, mock_connection_params):
        """Test cursor creation on closed connection."""
        from golemdb_sql.connection_parser import GolemBaseConnectionParams
        
        mock_params = GolemBaseConnectionParams(**mock_connection_params)
        mock_parse.return_value = mock_params
        
        conn = Connection(**mock_connection_params)
        conn.close()
        
        with pytest.raises(InterfaceError, match="Connection is closed"):
            conn.cursor()
    
    @patch('golemdb_sql.connection.GolemBaseClient', MockGolemBaseClient)
    @patch('golemdb_sql.connection.parse_connection_kwargs')
    def test_transaction_begin_commit(self, mock_parse, mock_connection_params):
        """Test transaction begin and commit."""
        from golemdb_sql.connection_parser import GolemBaseConnectionParams
        
        mock_params = GolemBaseConnectionParams(**mock_connection_params)
        mock_parse.return_value = mock_params
        
        conn = Connection(**mock_connection_params)
        
        # Begin transaction
        conn.begin()
        assert conn._in_transaction
        
        # Add some operations
        conn.add_pending_operation({
            'type': 'create',
            'entity': {'data': b'test', 'string_annotations': {}, 'numeric_annotations': {}}
        })
        assert len(conn._pending_operations) == 1
        
        # Commit transaction
        conn.commit()
        assert not conn._in_transaction
        assert len(conn._pending_operations) == 0
    
    @patch('golemdb_sql.connection.GolemBaseClient', MockGolemBaseClient)
    @patch('golemdb_sql.connection.parse_connection_kwargs')
    def test_transaction_rollback(self, mock_parse, mock_connection_params):
        """Test transaction rollback."""
        from golemdb_sql.connection_parser import GolemBaseConnectionParams
        
        mock_params = GolemBaseConnectionParams(**mock_connection_params)
        mock_parse.return_value = mock_params
        
        conn = Connection(**mock_connection_params)
        
        # Begin transaction and add operations
        conn.begin()
        conn.add_pending_operation({
            'type': 'create', 
            'entity': {'data': b'test', 'string_annotations': {}, 'numeric_annotations': {}}
        })
        assert len(conn._pending_operations) == 1
        
        # Rollback transaction
        conn.rollback()
        assert not conn._in_transaction
        assert len(conn._pending_operations) == 0
    
    @patch('golemdb_sql.connection.GolemBaseClient', MockGolemBaseClient)
    @patch('golemdb_sql.connection.parse_connection_kwargs')
    def test_autocommit_mode(self, mock_parse, mock_connection_params):
        """Test autocommit mode."""
        from golemdb_sql.connection_parser import GolemBaseConnectionParams
        
        mock_params = GolemBaseConnectionParams(**mock_connection_params)
        mock_parse.return_value = mock_params
        
        conn = Connection(**mock_connection_params)
        
        # Enable autocommit
        conn.autocommit = True
        assert conn.autocommit
        
        # Operations should execute immediately
        conn.add_pending_operation({
            'type': 'create',
            'entity': {'data': b'test', 'string_annotations': {}, 'numeric_annotations': {}}
        })
        # In autocommit mode, operations don't get queued
        assert len(conn._pending_operations) == 0
    
    @patch('golemdb_sql.connection.GolemBaseClient', MockGolemBaseClient)
    @patch('golemdb_sql.connection.parse_connection_kwargs')
    def test_context_manager_success(self, mock_parse, mock_connection_params):
        """Test context manager with successful transaction."""
        from golemdb_sql.connection_parser import GolemBaseConnectionParams
        
        mock_params = GolemBaseConnectionParams(**mock_connection_params)
        mock_parse.return_value = mock_params
        
        with Connection(**mock_connection_params) as conn:
            conn.begin()
            conn.add_pending_operation({
                'type': 'create',
                'entity': {'data': b'test', 'string_annotations': {}, 'numeric_annotations': {}}
            })
        
        # Connection should be closed after context exit
        assert conn.closed
    
    @patch('golemdb_sql.connection.GolemBaseClient', MockGolemBaseClient)
    @patch('golemdb_sql.connection.parse_connection_kwargs')
    def test_context_manager_exception(self, mock_parse, mock_connection_params):
        """Test context manager with exception (rollback)."""
        from golemdb_sql.connection_parser import GolemBaseConnectionParams
        
        mock_params = GolemBaseConnectionParams(**mock_connection_params)
        mock_parse.return_value = mock_params
        
        try:
            with Connection(**mock_connection_params) as conn:
                conn.begin()
                conn.add_pending_operation({
                    'type': 'create',
                    'entity': {'data': b'test', 'string_annotations': {}, 'numeric_annotations': {}}
                })
                raise ValueError("Test exception")
        except ValueError:
            pass
        
        # Connection should be closed after exception
        assert conn.closed
    
    @patch('golemdb_sql.connection.GolemBaseClient', MockGolemBaseClient)
    @patch('golemdb_sql.connection.parse_connection_kwargs')
    @patch('golemdb_sql.schema_manager.SchemaManager')
    def test_execute_convenience_method(self, mock_schema_class, mock_parse, mock_connection_params, sample_table_definition):
        """Test execute convenience method."""
        from golemdb_sql.connection_parser import GolemBaseConnectionParams
        
        mock_params = GolemBaseConnectionParams(**mock_connection_params)
        mock_parse.return_value = mock_params
        
        # Mock schema manager to include users table
        mock_schema = mock_schema_class.return_value
        mock_schema.table_exists.return_value = True
        mock_schema.get_table.return_value = sample_table_definition
        
        conn = Connection(**mock_connection_params)
        
        # Execute should return cursor
        cursor = conn.execute("SELECT * FROM users")
        assert isinstance(cursor, Cursor)
    
    @patch('golemdb_sql.connection.GolemBaseClient', MockGolemBaseClient)
    @patch('golemdb_sql.connection.parse_connection_kwargs')
    @patch('golemdb_sql.schema_manager.SchemaManager')
    def test_executemany_convenience_method(self, mock_schema_class, mock_parse, mock_connection_params, sample_table_definition):
        """Test executemany convenience method."""
        from golemdb_sql.connection_parser import GolemBaseConnectionParams
        
        mock_params = GolemBaseConnectionParams(**mock_connection_params)
        mock_parse.return_value = mock_params
        
        # Mock schema manager to include users table
        mock_schema = mock_schema_class.return_value
        mock_schema.table_exists.return_value = True
        mock_schema.get_table.return_value = sample_table_definition
        
        conn = Connection(**mock_connection_params)
        
        # Execute many should return cursor
        cursor = conn.executemany(
            "INSERT INTO users (name) VALUES (?)",
            [["Alice"], ["Bob"], ["Charlie"]]
        )
        assert isinstance(cursor, Cursor)
    
    @patch('golemdb_sql.connection.GolemBaseClient', MockGolemBaseClient)
    @patch('golemdb_sql.connection.parse_connection_kwargs')
    def test_double_begin_error(self, mock_parse, mock_connection_params):
        """Test error when beginning transaction twice."""
        from golemdb_sql.connection_parser import GolemBaseConnectionParams
        
        mock_params = GolemBaseConnectionParams(**mock_connection_params)
        mock_parse.return_value = mock_params
        
        conn = Connection(**mock_connection_params)
        conn.begin()
        
        with pytest.raises(ProgrammingError, match="Transaction already in progress"):
            conn.begin()
    
    def test_connect_function(self):
        """Test module-level connect function."""
        with patch('golemdb_sql.connection.Connection') as mock_connection:
            mock_instance = Mock()
            mock_connection.return_value = mock_instance
            
            result = connect(
                rpc_url="https://test.com",
                ws_url="wss://test.com",
                private_key="0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
            )
            
            assert result == mock_instance
            mock_connection.assert_called_once()
    
    @patch('golemdb_sql.connection.parse_connection_kwargs')
    def test_connection_initialization_error(self, mock_parse):
        """Test connection initialization error handling."""
        mock_parse.side_effect = Exception("Connection error")
        
        with pytest.raises(DatabaseError, match="Failed to connect to GolemBase"):
            Connection(rpc_url="invalid")
    
    @patch('golemdb_sql.connection.GolemBaseClient')
    @patch('golemdb_sql.connection.parse_connection_kwargs')
    def test_async_client_timeout(self, mock_parse, mock_client_class):
        """Test async client initialization timeout."""
        from golemdb_sql.connection_parser import GolemBaseConnectionParams
        
        mock_params = GolemBaseConnectionParams(
            rpc_url="https://test.com/rpc",
            ws_url="wss://test.com/ws",
            private_key="0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            app_id="test",
            schema_id="test"
        )
        mock_parse.return_value = mock_params
        
        # Mock client creation to never complete
        async def never_complete():
            await asyncio.sleep(100)  # Long delay
            return Mock()
        
        mock_client_class.create = never_complete
        
        with pytest.raises(DatabaseError, match="Timeout waiting for GolemBase client"):
            Connection(rpc_url="https://test.com")
    
    @patch('golemdb_sql.connection.GolemBaseClient', MockGolemBaseClient)
    @patch('golemdb_sql.connection.parse_connection_kwargs')
    def test_run_async_method(self, mock_parse, mock_connection_params):
        """Test _run_async method."""
        from golemdb_sql.connection_parser import GolemBaseConnectionParams
        
        mock_params = GolemBaseConnectionParams(**mock_connection_params)
        mock_parse.return_value = mock_params
        
        conn = Connection(**mock_connection_params)
        
        # Test running async coroutine
        async def test_coro():
            return "test_result"
        
        result = conn._run_async(test_coro())
        assert result == "test_result"
    
    @patch('golemdb_sql.connection.GolemBaseClient', MockGolemBaseClient)
    @patch('golemdb_sql.connection.parse_connection_kwargs')
    def test_run_async_closed_connection(self, mock_parse, mock_connection_params):
        """Test _run_async on closed connection."""
        from golemdb_sql.connection_parser import GolemBaseConnectionParams
        
        mock_params = GolemBaseConnectionParams(**mock_connection_params)
        mock_parse.return_value = mock_params
        
        conn = Connection(**mock_connection_params)
        conn.close()
        
        async def test_coro():
            return "test_result"
        
        with pytest.raises(InterfaceError, match="Connection is closed"):
            conn._run_async(test_coro())
    
    @patch('golemdb_sql.connection.GolemBaseClient', MockGolemBaseClient)
    @patch('golemdb_sql.connection.parse_connection_kwargs')
    def test_batch_operations_execution(self, mock_parse, mock_connection_params):
        """Test batch operations execution."""
        from golemdb_sql.connection_parser import GolemBaseConnectionParams
        
        mock_params = GolemBaseConnectionParams(**mock_connection_params)
        mock_parse.return_value = mock_params
        
        conn = Connection(**mock_connection_params)
        conn.begin()
        
        # Add different types of operations - create entities first
        conn.add_pending_operation({
            'type': 'create',
            'entity': {'id': '1', 'data': b'create1', 'string_annotations': {}, 'numeric_annotations': {}}
        })
        conn.add_pending_operation({
            'type': 'create',
            'entity': {'id': '2', 'data': b'create2', 'string_annotations': {}, 'numeric_annotations': {}}
        })
        conn.add_pending_operation({
            'type': 'create',
            'entity': {'id': '3', 'data': b'create3', 'string_annotations': {}, 'numeric_annotations': {}}
        })
        
        # Commit should execute all operations
        conn.commit()
        assert len(conn._pending_operations) == 0
        assert not conn._in_transaction
    
    @patch('golemdb_sql.connection.GolemBaseClient', MockGolemBaseClient)  
    @patch('golemdb_sql.connection.parse_connection_kwargs')
    def test_properties(self, mock_parse, mock_connection_params):
        """Test connection properties."""
        from golemdb_sql.connection_parser import GolemBaseConnectionParams
        
        mock_params = GolemBaseConnectionParams(**mock_connection_params)
        mock_parse.return_value = mock_params
        
        conn = Connection(**mock_connection_params)
        
        # Test client property
        assert conn.client is not None
        
        # Test params property
        assert conn.params == mock_params
        
        # Test closed property
        assert not conn.closed
        conn.close()
        assert conn.closed
        
        # Test properties on closed connection
        with pytest.raises(InterfaceError):
            _ = conn.client
        
        with pytest.raises(InterfaceError):
            _ = conn.params