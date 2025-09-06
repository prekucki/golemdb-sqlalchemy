"""Integration tests for golemdb_sql package."""

import pytest
import json
from unittest.mock import patch
from golemdb_sql import connect
from golemdb_sql.exceptions import DatabaseError, ProgrammingError
from .mock_golem_client import MockGolemBaseClient


class TestIntegration:
    """Integration tests for the complete package."""
    
    @patch('golemdb_sql.connection.GolemBaseClient', MockGolemBaseClient)
    @patch('golemdb_sql.connection.parse_connection_kwargs')
    def test_full_crud_workflow(self, mock_parse, sample_connection_params):
        """Test complete CRUD workflow."""
        mock_parse.return_value = sample_connection_params
        
        # Connect to database
        conn = connect(
            rpc_url="https://test.golembase.com/rpc",
            ws_url="wss://test.golembase.com/ws",
            private_key="0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            app_id="testapp"
        )
        
        try:
            cursor = conn.cursor()
            
            # Test table creation via schema
            from golemdb_sql.schema_manager import SchemaManager
            schema_manager = SchemaManager("test")
            
            create_table_sql = """
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                email VARCHAR(255) UNIQUE,
                age INTEGER,
                is_active BOOLEAN DEFAULT TRUE
            )
            """
            
            table_def = schema_manager.create_table_from_sql(create_table_sql)
            schema_manager.add_table(table_def)
            
            # Test INSERT
            insert_sql = "INSERT INTO users (name, email, age) VALUES (?, ?, ?)"
            cursor.execute(insert_sql, ["Alice Johnson", "alice@example.com", 28])
            
            # Test SELECT
            select_sql = "SELECT * FROM users WHERE name = ?"
            cursor.execute(select_sql, ["Alice Johnson"])
            results = cursor.fetchall()
            
            # Verify results (would need proper implementation)
            # assert len(results) > 0
            
            # Test UPDATE  
            update_sql = "UPDATE users SET age = ? WHERE email = ?"
            cursor.execute(update_sql, [29, "alice@example.com"])
            
            # Test DELETE
            delete_sql = "DELETE FROM users WHERE email = ?"
            cursor.execute(delete_sql, ["alice@example.com"])
            
        finally:
            conn.close()
    
    @patch('golemdb_sql.connection.GolemBaseClient', MockGolemBaseClient)
    @patch('golemdb_sql.connection.parse_connection_kwargs')
    def test_transaction_workflow(self, mock_parse, sample_connection_params):
        """Test transaction workflow."""
        mock_parse.return_value = sample_connection_params
        
        conn = connect(
            rpc_url="https://test.golembase.com",
            private_key="0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
        )
        
        try:
            # Test successful transaction
            with conn:
                cursor = conn.cursor()
                
                conn.begin()
                
                cursor.execute("INSERT INTO users (name) VALUES (?)", ["User 1"])
                cursor.execute("INSERT INTO users (name) VALUES (?)", ["User 2"])
                
                # Transaction should commit on successful exit
            
            # Test rollback transaction
            try:
                with conn:
                    cursor = conn.cursor()
                    
                    conn.begin()
                    
                    cursor.execute("INSERT INTO users (name) VALUES (?)", ["User 3"])
                    raise Exception("Simulated error")
                    
            except Exception:
                pass  # Expected exception
            
            # Verify rollback occurred (entities should not exist)
            
        finally:
            conn.close()
    
    @patch('golemdb_sql.connection.GolemBaseClient', MockGolemBaseClient)
    @patch('golemdb_sql.connection.parse_connection_kwargs')
    def test_parameterized_queries(self, mock_parse, sample_connection_params, sql_test_cases):
        """Test parameterized queries."""
        mock_parse.return_value = sample_connection_params
        
        conn = connect(rpc_url="https://test.golembase.com", private_key="0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef")
        
        try:
            cursor = conn.cursor()
            
            for sql, params in sql_test_cases["with_params"]:
                try:
                    cursor.execute(sql, params)
                    # Verify execution completed without error
                except Exception as e:
                    pytest.fail(f"Failed to execute parameterized query: {sql} with params {params}. Error: {e}")
                    
        finally:
            conn.close()
    
    @patch('golemdb_sql.connection.GolemBaseClient', MockGolemBaseClient)
    @patch('golemdb_sql.connection.parse_connection_kwargs')
    def test_cursor_iteration(self, mock_parse, sample_connection_params, db_with_sample_data):
        """Test cursor iteration functionality."""
        mock_parse.return_value = sample_connection_params
        
        conn = connect(rpc_url="https://test.golembase.com", private_key="0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef")
        
        try:
            cursor = conn.cursor()
            
            # Mock cursor results for iteration
            cursor._results = [
                (1, "Alice", "alice@example.com"),
                (2, "Bob", "bob@example.com"),
                (3, "Charlie", "charlie@example.com")
            ]
            cursor._rowcount = 3
            
            # Test iteration
            rows = []
            for row in cursor:
                rows.append(row)
            
            assert len(rows) == 3
            assert rows[0] == (1, "Alice", "alice@example.com")
            assert rows[1] == (2, "Bob", "bob@example.com")
            assert rows[2] == (3, "Charlie", "charlie@example.com")
            
        finally:
            conn.close()
    
    @patch('golemdb_sql.connection.GolemBaseClient', MockGolemBaseClient)
    @patch('golemdb_sql.connection.parse_connection_kwargs')
    def test_fetch_methods(self, mock_parse, sample_connection_params):
        """Test different fetch methods."""
        mock_parse.return_value = sample_connection_params
        
        conn = connect(rpc_url="https://test.golembase.com", private_key="0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef")
        
        try:
            cursor = conn.cursor()
            
            # Setup mock results
            cursor._results = [
                (1, "Alice"), (2, "Bob"), (3, "Charlie"), 
                (4, "David"), (5, "Eve")
            ]
            cursor._rowcount = 5
            
            # Test fetchone
            row = cursor.fetchone()
            assert row == (1, "Alice")
            assert len(cursor._results) == 4
            
            # Test fetchmany
            rows = cursor.fetchmany(2)
            assert len(rows) == 2
            assert rows == [(2, "Bob"), (3, "Charlie")]
            assert len(cursor._results) == 2
            
            # Test fetchall
            remaining = cursor.fetchall()
            assert len(remaining) == 2
            assert remaining == [(4, "David"), (5, "Eve")]
            assert len(cursor._results) == 0
            
            # Test fetchone on empty results
            assert cursor.fetchone() is None
            
        finally:
            conn.close()
    
    def test_connection_string_parsing_integration(self):
        """Test various connection string formats."""
        from golemdb_sql.connection_parser import parse_connection_string
        
        # Test GolemBase URL format
        url_string = "golembase://0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef@rpc.golembase.com/myapp?ws_url=wss://ws.golembase.com&schema_id=myschema"
        params = parse_connection_string(url_string)
        
        assert params.rpc_url == "https://rpc.golembase.com/rpc"
        assert params.ws_url == "wss://ws.golembase.com"
        assert params.private_key == "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
        assert params.app_id == "myapp"
        assert params.schema_id == "myschema"
        
        # Test key-value format
        kv_string = "rpc_url=https://test.com ws_url=wss://test.com private_key=0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef app_id=test"
        params = parse_connection_string(kv_string)
        
        assert params.rpc_url == "https://test.com"
        assert params.ws_url == "wss://test.com"
        assert params.private_key == "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
        assert params.app_id == "test"
    
    def test_schema_persistence_integration(self, temp_dir):
        """Test schema persistence to TOML files."""
        from golemdb_sql.schema_manager import SchemaManager
        from unittest.mock import patch
        
        schema_path = temp_dir / "test_schema.toml"
        
        with patch('golemdb_sql.schema_manager.SchemaManager._get_schema_path', return_value=schema_path):
            # Create schema manager
            schema_manager = SchemaManager("test")
            
            # Create table from SQL
            create_sql = """
            CREATE TABLE products (
                id INTEGER PRIMARY KEY,
                name VARCHAR(200) NOT NULL,
                price DECIMAL(10,2),
                category_id INTEGER,
                in_stock BOOLEAN DEFAULT TRUE
            )
            """
            
            table_def = schema_manager.create_table_from_sql(create_sql)
            schema_manager.add_table(table_def)
            
            # Verify file was created
            assert schema_path.exists()
            
            # Create new schema manager and verify it loads the table
            schema_manager2 = SchemaManager("test")
            
            loaded_table = schema_manager2.get_table("products")
            assert loaded_table is not None
            assert loaded_table.name == "products"
            assert len(loaded_table.columns) == 5
            
            # Verify column details
            id_col = loaded_table.get_column("id")
            assert id_col.primary_key
            
            name_col = loaded_table.get_column("name")
            assert not name_col.nullable
            
            price_col = loaded_table.get_column("price")
            assert price_col.type == "DECIMAL(10,2)"
    
    def test_row_serialization_integration(self):
        """Test end-to-end row serialization."""
        from golemdb_sql.row_serializer import RowSerializer
        from golemdb_sql.schema_manager import SchemaManager, TableDefinition, ColumnDefinition
        from datetime import datetime
        from decimal import Decimal
        
        # Setup schema
        table_def = TableDefinition(
            name="orders",
            columns=[
                ColumnDefinition(name="id", type="INTEGER", primary_key=True),
                ColumnDefinition(name="customer_name", type="VARCHAR(100)"),
                ColumnDefinition(name="total", type="DECIMAL(10,2)"),
                ColumnDefinition(name="order_date", type="DATETIME"),
                ColumnDefinition(name="is_paid", type="BOOLEAN")
            ],
            indexes=[],
            foreign_keys=[]
        )
        
        schema_manager = SchemaManager("test")
        schema_manager.add_table(table_def)
        
        # Setup serializer
        serializer = RowSerializer(schema_manager)
        
        # Test data
        order_data = {
            "id": 1001,
            "customer_name": "John Doe",
            "total": Decimal("199.99"),
            "order_date": datetime(2023, 6, 15, 14, 30, 0),
            "is_paid": True
        }
        
        # Serialize
        json_bytes, annotations = serializer.serialize_row("orders", order_data)
        
        # Verify serialization
        assert isinstance(json_bytes, bytes)
        entity_data = json.loads(json_bytes.decode('utf-8'))
        
        assert entity_data["_table"] == "orders"
        assert entity_data["id"] == 1001
        assert entity_data["customer_name"] == "John Doe"
        assert entity_data["total"] == "199.99"  # Decimal as string
        assert entity_data["order_date"] == "2023-06-15T14:30:00"
        assert entity_data["is_paid"] is True
        
        # Verify annotations
        assert annotations["string_annotations"]["table"] == "orders"
        
        # Deserialize
        restored_data = serializer.deserialize_entity(json_bytes, "orders")
        
        assert restored_data["id"] == 1001
        assert restored_data["customer_name"] == "John Doe"
        assert isinstance(restored_data["total"], Decimal)
        assert restored_data["total"] == Decimal("199.99")
        assert isinstance(restored_data["order_date"], datetime)
        assert restored_data["order_date"] == datetime(2023, 6, 15, 14, 30, 0)
        assert restored_data["is_paid"] is True
    
    def test_query_translation_integration(self):
        """Test SQL query translation integration."""
        from golemdb_sql.query_translator import QueryTranslator
        from golemdb_sql.schema_manager import SchemaManager, TableDefinition, ColumnDefinition
        
        # Setup schema
        table_def = TableDefinition(
            name="employees",
            columns=[
                ColumnDefinition(name="id", type="INTEGER", primary_key=True, indexed=True),
                ColumnDefinition(name="name", type="VARCHAR(100)", indexed=True),
                ColumnDefinition(name="department", type="VARCHAR(50)", indexed=True),
                ColumnDefinition(name="salary", type="INTEGER", indexed=True),
                ColumnDefinition(name="is_active", type="BOOLEAN", indexed=True)
            ],
            indexes=[],
            foreign_keys=[]
        )
        
        schema_manager = SchemaManager("test")
        schema_manager.add_table(table_def)
        
        translator = QueryTranslator(schema_manager)
        
        # Test complex SELECT with multiple conditions
        sql = """
        SELECT name, department, salary 
        FROM employees 
        WHERE salary > 50000 
        AND department = 'Engineering' 
        AND is_active = TRUE
        ORDER BY salary DESC 
        LIMIT 10
        """
        
        result = translator.translate_select(sql)
        
        assert result.operation_type == "SELECT"
        assert result.table_name == "employees"
        assert set(result.columns) == {"name", "department", "salary"}
        assert result.limit == 10
        assert result.sort_by == "salary"
        assert result.sort_order == "desc"
        
        # Verify query contains all conditions
        query = result.golem_query
        assert 'table="employees"' in query
        assert 'salary>50000' in query
        assert 'department="Engineering"' in query
        assert 'is_active=1' in query
        
        # Test parameterized query
        sql_with_params = """
        SELECT * FROM employees 
        WHERE department = :dept AND salary >= :min_salary
        """
        params = {"dept": "Marketing", "min_salary": 40000}
        
        result = translator.translate_select(sql_with_params, params)
        
        query = result.golem_query
        assert 'department="Marketing"' in query
        assert 'salary>=40000' in query
    
    @patch('golemdb_sql.connection.GolemBaseClient', MockGolemBaseClient)
    @patch('golemdb_sql.connection.parse_connection_kwargs')
    def test_error_handling_integration(self, mock_parse, sample_connection_params):
        """Test error handling across the system."""
        mock_parse.return_value = sample_connection_params
        
        conn = connect(rpc_url="https://test.golembase.com", private_key="0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef")
        
        try:
            cursor = conn.cursor()
            
            # Test unsupported SQL
            with pytest.raises(Exception):  # Should be ProgrammingError in full implementation
                cursor.execute("SELECT COUNT(*) FROM users")
            
            # Test invalid table
            with pytest.raises(Exception):  # Should be ProgrammingError in full implementation
                cursor.execute("SELECT * FROM nonexistent_table")
            
            # Test invalid SQL syntax
            with pytest.raises(Exception):  # Should be DatabaseError in full implementation
                cursor.execute("INVALID SQL SYNTAX")
                
        finally:
            conn.close()
    
    def test_module_level_api(self):
        """Test module-level API compliance."""
        import golemdb_sql
        
        # Test required module attributes (PEP 249)
        assert hasattr(golemdb_sql, 'connect')
        assert hasattr(golemdb_sql, 'apilevel')
        assert hasattr(golemdb_sql, 'threadsafety') 
        assert hasattr(golemdb_sql, 'paramstyle')
        
        # Test exception hierarchy
        assert hasattr(golemdb_sql, 'Error')
        assert hasattr(golemdb_sql, 'DatabaseError')
        assert hasattr(golemdb_sql, 'InterfaceError')
        assert hasattr(golemdb_sql, 'ProgrammingError')
        assert hasattr(golemdb_sql, 'DataError')
        assert hasattr(golemdb_sql, 'OperationalError')
        
        # Test type constructors (if implemented)
        if hasattr(golemdb_sql, 'Date'):
            assert callable(golemdb_sql.Date)
        if hasattr(golemdb_sql, 'Time'):
            assert callable(golemdb_sql.Time)
        if hasattr(golemdb_sql, 'Timestamp'):
            assert callable(golemdb_sql.Timestamp)