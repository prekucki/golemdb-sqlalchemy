"""Tests for row serialization functionality."""

import json
import pytest
from datetime import datetime, date, time
from decimal import Decimal
from unittest.mock import Mock, patch
from golemdb_sql.row_serializer import RowSerializer
from golemdb_sql.schema_manager import SchemaManager, TableDefinition, ColumnDefinition
from golemdb_sql.exceptions import DataError, ProgrammingError


class TestRowSerializer:
    """Test row serialization functionality."""
    
    @pytest.fixture
    def mock_schema_manager(self):
        """Create mock schema manager."""
        schema_manager = Mock(spec=SchemaManager)
        
        # Create test table definition
        table_def = TableDefinition(
            name="test_table",
            columns=[
                ColumnDefinition(name="id", type="INTEGER", primary_key=True),
                ColumnDefinition(name="name", type="VARCHAR(100)", nullable=False),
                ColumnDefinition(name="email", type="VARCHAR(255)", unique=True),
                ColumnDefinition(name="age", type="INTEGER"),
                ColumnDefinition(name="salary", type="DECIMAL(10,2)"),
                ColumnDefinition(name="is_active", type="BOOLEAN"),
                ColumnDefinition(name="created_at", type="DATETIME"),
                ColumnDefinition(name="birth_date", type="DATE"),
                ColumnDefinition(name="login_time", type="TIME"),
                ColumnDefinition(name="profile_data", type="JSON"),
                ColumnDefinition(name="avatar", type="BLOB")
            ],
            indexes=[],
            foreign_keys=[]
        )
        
        schema_manager.get_table.return_value = table_def
        schema_manager.get_entity_annotations_for_table.return_value = {
            "string_annotations": {"table": "test_table", "name": "John"},
            "numeric_annotations": {"id": 123, "age": 30}
        }
        
        return schema_manager
    
    @pytest.fixture
    def serializer(self, mock_schema_manager):
        """Create row serializer with mock schema manager."""
        return RowSerializer(mock_schema_manager)
    
    def test_serialize_row_basic(self, serializer, mock_schema_manager):
        """Test basic row serialization."""
        row_data = {
            "id": 123,
            "name": "John Doe",
            "email": "john@example.com",
            "age": 30,
            "is_active": True
        }
        
        json_bytes, annotations = serializer.serialize_row("test_table", row_data)
        
        # Check that we get bytes
        assert isinstance(json_bytes, bytes)
        
        # Parse JSON to check contents
        entity_data = json.loads(json_bytes.decode('utf-8'))
        assert entity_data["_table"] == "test_table"
        assert entity_data["_version"] == 1
        assert "_created_at" in entity_data
        assert entity_data["id"] == 123
        assert entity_data["name"] == "John Doe"
        assert entity_data["email"] == "john@example.com"
        assert entity_data["age"] == 30
        assert entity_data["is_active"] is True
        
        # Check annotations
        assert annotations == {
            "string_annotations": {"table": "test_table", "name": "John"},
            "numeric_annotations": {"id": 123, "age": 30}
        }
    
    def test_serialize_row_with_datetime(self, serializer, mock_schema_manager):
        """Test row serialization with datetime values."""
        test_datetime = datetime(2023, 1, 15, 14, 30, 0)
        test_date = date(2023, 1, 15)
        test_time = time(14, 30, 0)
        
        row_data = {
            "id": 1,
            "created_at": test_datetime,
            "birth_date": test_date,
            "login_time": test_time
        }
        
        json_bytes, _ = serializer.serialize_row("test_table", row_data)
        entity_data = json.loads(json_bytes.decode('utf-8'))
        
        assert entity_data["created_at"] == test_datetime.isoformat()
        assert entity_data["birth_date"] == test_date.isoformat()
        assert entity_data["login_time"] == test_time.isoformat()
    
    def test_serialize_row_with_decimal(self, serializer, mock_schema_manager):
        """Test row serialization with decimal values."""
        row_data = {
            "id": 1,
            "salary": Decimal("50000.50")
        }
        
        json_bytes, _ = serializer.serialize_row("test_table", row_data)
        entity_data = json.loads(json_bytes.decode('utf-8'))
        
        assert entity_data["salary"] == "50000.50"
    
    def test_serialize_row_with_json(self, serializer, mock_schema_manager):
        """Test row serialization with JSON data."""
        profile_data = {"skills": ["Python", "SQL"], "experience": 5}
        row_data = {
            "id": 1,
            "profile_data": profile_data
        }
        
        json_bytes, _ = serializer.serialize_row("test_table", row_data)
        entity_data = json.loads(json_bytes.decode('utf-8'))
        
        assert entity_data["profile_data"] == profile_data
    
    def test_serialize_row_with_blob(self, serializer, mock_schema_manager):
        """Test row serialization with binary data."""
        binary_data = b"\\x00\\x01\\x02\\x03"
        row_data = {
            "id": 1,
            "avatar": binary_data
        }
        
        json_bytes, _ = serializer.serialize_row("test_table", row_data)
        entity_data = json.loads(json_bytes.decode('utf-8'))
        
        # Should be base64 encoded
        import base64
        expected_encoded = base64.b64encode(binary_data).decode('ascii')
        assert entity_data["avatar"] == expected_encoded
    
    def test_serialize_row_table_not_found(self, serializer, mock_schema_manager):
        """Test error handling when table not found."""
        mock_schema_manager.get_table.return_value = None
        
        with pytest.raises(ProgrammingError, match="Table 'nonexistent' not found"):
            serializer.serialize_row("nonexistent", {"id": 1})
    
    def test_deserialize_entity_basic(self, serializer, mock_schema_manager):
        """Test basic entity deserialization."""
        entity_data = {
            "_table": "test_table",
            "_version": 1,
            "_created_at": "2023-01-15T14:30:00",
            "id": 123,
            "name": "John Doe",
            "email": "john@example.com",
            "age": 30,
            "is_active": True
        }
        
        json_bytes = json.dumps(entity_data).encode('utf-8')
        row_data = serializer.deserialize_entity(json_bytes, "test_table")
        
        assert row_data["id"] == 123
        assert row_data["name"] == "John Doe"
        assert row_data["email"] == "john@example.com"
        assert row_data["age"] == 30
        assert row_data["is_active"] is True
        
        # Metadata fields should be excluded
        assert "_table" not in row_data
        assert "_version" not in row_data
        assert "_created_at" not in row_data
    
    def test_deserialize_entity_with_datetime(self, serializer, mock_schema_manager):
        """Test entity deserialization with datetime values."""
        entity_data = {
            "_table": "test_table",
            "id": 1,
            "created_at": "2023-01-15T14:30:00",
            "birth_date": "2023-01-15",
            "login_time": "14:30:00"
        }
        
        json_bytes = json.dumps(entity_data).encode('utf-8')
        row_data = serializer.deserialize_entity(json_bytes, "test_table")
        
        assert isinstance(row_data["created_at"], datetime)
        assert row_data["created_at"] == datetime(2023, 1, 15, 14, 30, 0)
        assert isinstance(row_data["birth_date"], date)
        assert row_data["birth_date"] == date(2023, 1, 15)
        assert isinstance(row_data["login_time"], time)
        assert row_data["login_time"] == time(14, 30, 0)
    
    def test_deserialize_entity_with_decimal(self, serializer, mock_schema_manager):
        """Test entity deserialization with decimal values."""
        entity_data = {
            "_table": "test_table",
            "id": 1,
            "salary": "50000.50"
        }
        
        json_bytes = json.dumps(entity_data).encode('utf-8')
        row_data = serializer.deserialize_entity(json_bytes, "test_table")
        
        assert isinstance(row_data["salary"], Decimal)
        assert row_data["salary"] == Decimal("50000.50")
    
    def test_deserialize_entity_with_blob(self, serializer, mock_schema_manager):
        """Test entity deserialization with binary data."""
        binary_data = b"\\x00\\x01\\x02\\x03"
        import base64
        encoded_data = base64.b64encode(binary_data).decode('ascii')
        
        entity_data = {
            "_table": "test_table",
            "id": 1,
            "avatar": encoded_data
        }
        
        json_bytes = json.dumps(entity_data).encode('utf-8')
        row_data = serializer.deserialize_entity(json_bytes, "test_table")
        
        assert isinstance(row_data["avatar"], bytes)
        assert row_data["avatar"] == binary_data
    
    def test_deserialize_entity_table_mismatch(self, serializer, mock_schema_manager):
        """Test error handling for table mismatch."""
        entity_data = {
            "_table": "other_table",
            "id": 1
        }
        
        json_bytes = json.dumps(entity_data).encode('utf-8')
        
        with pytest.raises(DataError, match="Entity table mismatch"):
            serializer.deserialize_entity(json_bytes, "test_table")
    
    def test_deserialize_entity_invalid_json(self, serializer, mock_schema_manager):
        """Test error handling for invalid JSON."""
        invalid_json = b"invalid json data"
        
        with pytest.raises(DataError, match="Failed to parse entity JSON data"):
            serializer.deserialize_entity(invalid_json, "test_table")
    
    def test_create_row_from_columns_values(self, serializer, mock_schema_manager):
        """Test creating row from columns and values."""
        columns = ["id", "name", "email"]
        values = [123, "John Doe", "john@example.com"]
        
        row_data = serializer.create_row_from_columns_values("test_table", columns, values)
        
        assert row_data["id"] == 123
        assert row_data["name"] == "John Doe"
        assert row_data["email"] == "john@example.com"
    
    def test_create_row_column_value_mismatch(self, serializer, mock_schema_manager):
        """Test error handling for column/value count mismatch."""
        columns = ["id", "name"]
        values = [123]  # Missing value
        
        with pytest.raises(DataError, match="Column count .* doesn't match value count"):
            serializer.create_row_from_columns_values("test_table", columns, values)
    
    def test_create_row_with_defaults(self, serializer, mock_schema_manager):
        """Test creating row with default values."""
        # Add column with default value
        table_def = mock_schema_manager.get_table.return_value
        table_def.columns.append(
            ColumnDefinition(name="status", type="VARCHAR(20)", default="'active'")
        )
        
        columns = ["id", "name"]
        values = [123, "John Doe"]
        
        row_data = serializer.create_row_from_columns_values("test_table", columns, values)
        
        assert row_data["id"] == 123
        assert row_data["name"] == "John Doe"
        assert row_data["status"] == "active"  # Default value applied
    
    def test_update_row_data(self, serializer, mock_schema_manager):
        """Test updating existing row data."""
        existing_data = {
            "id": 123,
            "name": "John Doe",
            "email": "john@example.com",
            "age": 30
        }
        
        updates = {
            "name": "Jane Doe",
            "age": 31
        }
        
        updated_data = serializer.update_row_data(existing_data, updates, "test_table")
        
        assert updated_data["id"] == 123  # Unchanged
        assert updated_data["name"] == "Jane Doe"  # Updated
        assert updated_data["email"] == "john@example.com"  # Unchanged
        assert updated_data["age"] == 31  # Updated
    
    def test_update_row_data_invalid_column(self, serializer, mock_schema_manager):
        """Test error handling for updating non-existent column."""
        existing_data = {"id": 123}
        updates = {"nonexistent_column": "value"}
        
        # Mock get_column to return None for non-existent column
        table_def = mock_schema_manager.get_table.return_value
        table_def.get_column.return_value = None
        
        with pytest.raises(ProgrammingError, match="Column 'nonexistent_column' does not exist"):
            serializer.update_row_data(existing_data, updates, "test_table")
    
    def test_parse_default_value(self, serializer):
        """Test parsing default values."""
        # Integer default
        assert serializer._parse_default_value("123", "INTEGER") == 123
        
        # Float default
        assert serializer._parse_default_value("123.45", "FLOAT") == 123.45
        
        # Boolean defaults
        assert serializer._parse_default_value("TRUE", "BOOLEAN") is True
        assert serializer._parse_default_value("FALSE", "BOOLEAN") is False
        assert serializer._parse_default_value("1", "BOOLEAN") is True
        assert serializer._parse_default_value("0", "BOOLEAN") is False
        
        # String defaults (with quotes)
        assert serializer._parse_default_value("'active'", "VARCHAR") == "active"
        assert serializer._parse_default_value("\"default\"", "VARCHAR") == "default"
        
        # NULL default
        assert serializer._parse_default_value("NULL", "VARCHAR") is None
        
        # CURRENT_TIMESTAMP
        result = serializer._parse_default_value("CURRENT_TIMESTAMP", "DATETIME")
        assert isinstance(result, datetime)
    
    def test_make_json_serializable(self, serializer):
        """Test making values JSON serializable."""
        # Basic types
        assert serializer._make_json_serializable(123) == 123
        assert serializer._make_json_serializable("test") == "test"
        assert serializer._make_json_serializable(True) is True
        assert serializer._make_json_serializable(None) is None
        
        # DateTime types
        dt = datetime(2023, 1, 15, 14, 30, 0)
        assert serializer._make_json_serializable(dt) == dt.isoformat()
        
        d = date(2023, 1, 15)
        assert serializer._make_json_serializable(d) == d.isoformat()
        
        t = time(14, 30, 0)
        assert serializer._make_json_serializable(t) == t.isoformat()
        
        # Decimal
        decimal_val = Decimal("123.45")
        assert serializer._make_json_serializable(decimal_val) == "123.45"
        
        # Bytes
        binary_data = b"test"
        result = serializer._make_json_serializable(binary_data)
        import base64
        expected = base64.b64encode(binary_data).decode('ascii')
        assert result == expected
        
        # Lists and dicts
        assert serializer._make_json_serializable([1, 2, 3]) == [1, 2, 3]
        assert serializer._make_json_serializable({"key": "value"}) == {"key": "value"}
        
        # Complex nested structure
        complex_data = {
            "datetime": datetime(2023, 1, 15),
            "list": [Decimal("1.23"), date(2023, 1, 15)],
            "nested": {"decimal": Decimal("4.56")}
        }
        
        result = serializer._make_json_serializable(complex_data)
        assert result["datetime"] == "2023-01-15T00:00:00"
        assert result["list"][0] == "1.23"
        assert result["list"][1] == "2023-01-15"
        assert result["nested"]["decimal"] == "4.56"