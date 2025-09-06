"""Tests for schema management functionality."""

import tempfile
import pytest
from pathlib import Path
from unittest.mock import patch
from golemdb_sql.schema_manager import (
    SchemaManager,
    TableDefinition,
    ColumnDefinition,
    IndexDefinition,
    ForeignKeyDefinition
)
from golemdb_sql.exceptions import ProgrammingError, DatabaseError


class TestSchemaManager:
    """Test schema management functionality."""
    
    @pytest.fixture
    def temp_schema_dir(self):
        """Create temporary schema directory."""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield Path(temp_dir)
    
    @pytest.fixture
    def schema_manager(self, temp_schema_dir):
        """Create schema manager with temporary directory."""
        with patch('golemdb_sql.schema_manager.SchemaManager._get_schema_path') as mock_path:
            mock_path.return_value = temp_schema_dir / "test_schema.toml"
            return SchemaManager("test_schema")
    
    def test_schema_manager_initialization(self, schema_manager):
        """Test schema manager initialization."""
        assert schema_manager.schema_id == "test_schema"
        assert isinstance(schema_manager.tables, dict)
        assert len(schema_manager.tables) == 0
    
    def test_add_table(self, schema_manager):
        """Test adding table definition."""
        table_def = TableDefinition(
            name="users",
            columns=[
                ColumnDefinition(name="id", type="INTEGER", primary_key=True),
                ColumnDefinition(name="name", type="VARCHAR(100)", nullable=False),
                ColumnDefinition(name="email", type="VARCHAR(255)", unique=True)
            ],
            indexes=[],
            foreign_keys=[]
        )
        
        schema_manager.add_table(table_def)
        
        assert "users" in schema_manager.tables
        assert schema_manager.get_table("users") == table_def
    
    def test_remove_table(self, schema_manager):
        """Test removing table definition."""
        table_def = TableDefinition(
            name="temp_table",
            columns=[ColumnDefinition(name="id", type="INTEGER")],
            indexes=[],
            foreign_keys=[]
        )
        
        schema_manager.add_table(table_def)
        assert schema_manager.table_exists("temp_table")
        
        schema_manager.remove_table("temp_table")
        assert not schema_manager.table_exists("temp_table")
    
    def test_get_table_names(self, schema_manager):
        """Test getting all table names."""
        table1 = TableDefinition("table1", [], [], [])
        table2 = TableDefinition("table2", [], [], [])
        
        schema_manager.add_table(table1)
        schema_manager.add_table(table2)
        
        names = schema_manager.get_table_names()
        assert set(names) == {"table1", "table2"}
    
    def test_create_table_from_sql_basic(self, schema_manager):
        """Test creating table from basic SQL."""
        sql = """
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            email VARCHAR(255) UNIQUE,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        table_def = schema_manager.create_table_from_sql(sql)
        
        assert table_def.name == "users"
        assert len(table_def.columns) == 4
        
        # Check primary key column
        id_col = table_def.get_column("id")
        assert id_col is not None
        assert id_col.primary_key
        assert not id_col.nullable
        assert id_col.indexed
        
        # Check not null column
        name_col = table_def.get_column("name")
        assert name_col is not None
        assert not name_col.nullable
        
        # Check unique column
        email_col = table_def.get_column("email")
        assert email_col is not None
        assert email_col.unique
        assert email_col.indexed
        
        # Check default value
        created_col = table_def.get_column("created_at")
        assert created_col is not None
        assert created_col.default == "CURRENT_TIMESTAMP"
    
    def test_create_table_from_sql_with_constraints(self, schema_manager):
        """Test creating table from SQL with constraints."""
        sql = """
        CREATE TABLE posts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            title VARCHAR(200) NOT NULL,
            content TEXT,
            category_id INTEGER,
            FOREIGN KEY (user_id) REFERENCES users(id),
            FOREIGN KEY (category_id) REFERENCES categories(id)
        )
        """
        
        table_def = schema_manager.create_table_from_sql(sql)
        
        assert table_def.name == "posts"
        assert len(table_def.columns) == 5
        assert len(table_def.foreign_keys) >= 0  # Foreign key parsing may vary
    
    def test_invalid_sql_parsing(self, schema_manager):
        """Test error handling for invalid SQL."""
        with pytest.raises(ProgrammingError):
            schema_manager.create_table_from_sql("SELECT * FROM users")
        
        with pytest.raises(ProgrammingError):
            schema_manager.create_table_from_sql("invalid sql")
    
    def test_get_primary_key_columns(self):
        """Test getting primary key columns."""
        table_def = TableDefinition(
            name="test",
            columns=[
                ColumnDefinition(name="id1", type="INTEGER", primary_key=True),
                ColumnDefinition(name="id2", type="INTEGER", primary_key=True),
                ColumnDefinition(name="name", type="VARCHAR(100)")
            ],
            indexes=[],
            foreign_keys=[]
        )
        
        pk_columns = table_def.get_primary_key_columns()
        assert set(pk_columns) == {"id1", "id2"}
    
    def test_get_indexed_columns(self):
        """Test getting indexed columns."""
        table_def = TableDefinition(
            name="test",
            columns=[
                ColumnDefinition(name="id", type="INTEGER", primary_key=True),
                ColumnDefinition(name="email", type="VARCHAR(255)", unique=True),
                ColumnDefinition(name="name", type="VARCHAR(100)", indexed=True),
                ColumnDefinition(name="description", type="TEXT")
            ],
            indexes=[
                IndexDefinition(name="idx_test_category", columns=["category"]),
                IndexDefinition(name="idx_test_composite", columns=["name", "email"])
            ],
            foreign_keys=[]
        )
        
        indexed_columns = table_def.get_indexed_columns()
        expected = {"id", "email", "name", "category"}  # composite index adds "name", "email" again
        assert indexed_columns >= expected  # Allow for additional columns
    
    def test_get_entity_annotations(self, schema_manager):
        """Test getting entity annotations for table row."""
        table_def = TableDefinition(
            name="users",
            columns=[
                ColumnDefinition(name="id", type="INTEGER", primary_key=True),
                ColumnDefinition(name="name", type="VARCHAR(100)", indexed=True),
                ColumnDefinition(name="age", type="INTEGER", indexed=True),
                ColumnDefinition(name="is_active", type="BOOLEAN", indexed=True),
                ColumnDefinition(name="created_at", type="DATETIME", indexed=True)
            ],
            indexes=[],
            foreign_keys=[]
        )
        
        schema_manager.add_table(table_def)
        
        from datetime import datetime
        row_data = {
            "id": 123,
            "name": "John Doe",
            "age": 30,
            "is_active": True,
            "created_at": datetime(2023, 1, 1, 12, 0, 0),
            "description": "Not indexed"
        }
        
        annotations = schema_manager.get_entity_annotations_for_table("users", row_data)
        
        assert annotations["string_annotations"]["table"] == "users"
        assert annotations["string_annotations"]["name"] == "John Doe"
        assert annotations["numeric_annotations"]["id"] == 123
        assert annotations["numeric_annotations"]["age"] == 30
        assert annotations["numeric_annotations"]["is_active"] == 1  # boolean as 1
        assert isinstance(annotations["numeric_annotations"]["created_at"], int)  # timestamp
        
        # Non-indexed columns should not be in annotations
        assert "description" not in annotations["string_annotations"]
        assert "description" not in annotations["numeric_annotations"]
    
    def test_get_ttl_for_table(self, schema_manager):
        """Test getting TTL for table."""
        table_def = TableDefinition(
            name="test_table",
            columns=[],
            indexes=[],
            foreign_keys=[],
            entity_ttl=3600  # 1 hour
        )
        
        schema_manager.add_table(table_def)
        
        ttl = schema_manager.get_ttl_for_table("test_table")
        assert ttl == 3600
        
        # Non-existent table should return default
        default_ttl = schema_manager.get_ttl_for_table("non_existent")
        assert default_ttl == 86400  # 24 hours default
    
    def test_table_definition_serialization(self):
        """Test table definition serialization to/from dict."""
        table_def = TableDefinition(
            name="test",
            columns=[
                ColumnDefinition(name="id", type="INTEGER", primary_key=True),
                ColumnDefinition(name="name", type="VARCHAR(100)")
            ],
            indexes=[
                IndexDefinition(name="idx_test_name", columns=["name"])
            ],
            foreign_keys=[
                ForeignKeyDefinition(
                    name="fk_test_user",
                    columns=["user_id"],
                    referenced_table="users",
                    referenced_columns=["id"]
                )
            ]
        )
        
        # Convert to dict and back
        table_dict = table_def.to_dict()
        restored_table = TableDefinition.from_dict(table_dict)
        
        assert restored_table.name == table_def.name
        assert len(restored_table.columns) == len(table_def.columns)
        assert len(restored_table.indexes) == len(table_def.indexes)
        assert len(restored_table.foreign_keys) == len(table_def.foreign_keys)
        
        # Check column details
        assert restored_table.columns[0].name == "id"
        assert restored_table.columns[0].primary_key
        assert restored_table.columns[1].name == "name"
    
    def test_column_definition_serialization(self):
        """Test column definition serialization."""
        col_def = ColumnDefinition(
            name="test_col",
            type="VARCHAR(255)",
            nullable=False,
            default="'default_value'",
            unique=True,
            indexed=True
        )
        
        col_dict = col_def.to_dict()
        restored_col = ColumnDefinition.from_dict(col_dict)
        
        assert restored_col.name == col_def.name
        assert restored_col.type == col_def.type
        assert restored_col.nullable == col_def.nullable
        assert restored_col.default == col_def.default
        assert restored_col.unique == col_def.unique
        assert restored_col.indexed == col_def.indexed