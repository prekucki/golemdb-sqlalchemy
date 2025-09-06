"""pytest configuration and fixtures for golemdb_sql tests."""

import pytest
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch
from golemdb_sql.schema_manager import SchemaManager, TableDefinition, ColumnDefinition
from golemdb_sql.connection_parser import GolemBaseConnectionParams
from .mock_golem_client import MockGolemBaseClient


@pytest.fixture
def temp_dir():
    """Create temporary directory for test files."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield Path(temp_dir)


@pytest.fixture  
def mock_schema_path(temp_dir):
    """Mock schema file path."""
    return temp_dir / "test_schema.toml"


@pytest.fixture
def sample_connection_params():
    """Sample connection parameters for testing."""
    return GolemBaseConnectionParams(
        rpc_url="https://test.golembase.com/rpc",
        ws_url="wss://test.golembase.com/ws",
        private_key="0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
        app_id="testapp",
        schema_id="testschema"
    )


@pytest.fixture
def sample_table_definition():
    """Sample table definition for testing."""
    return TableDefinition(
        name="users",
        columns=[
            ColumnDefinition(name="id", type="INTEGER", primary_key=True, indexed=True),
            ColumnDefinition(name="name", type="VARCHAR(100)", nullable=False, indexed=True),
            ColumnDefinition(name="email", type="VARCHAR(255)", unique=True, indexed=True),
            ColumnDefinition(name="age", type="INTEGER", indexed=True),
            ColumnDefinition(name="is_active", type="BOOLEAN", indexed=True, default="TRUE"),
            ColumnDefinition(name="created_at", type="DATETIME", indexed=True),
            ColumnDefinition(name="profile_data", type="JSON"),
            ColumnDefinition(name="avatar", type="BLOB")
        ],
        indexes=[],
        foreign_keys=[],
        entity_ttl=3600
    )


@pytest.fixture
def mock_schema_manager(sample_table_definition):
    """Mock schema manager with sample table."""
    schema_manager = Mock(spec=SchemaManager)
    schema_manager.get_table.return_value = sample_table_definition
    schema_manager.get_ttl_for_table.return_value = 3600
    schema_manager.get_entity_annotations_for_table.return_value = {
        "string_annotations": {"table": "users"},
        "numeric_annotations": {}
    }
    return schema_manager


@pytest.fixture
def mock_golem_client():
    """Mock GolemBase client for testing."""
    return MockGolemBaseClient()


@pytest.fixture
def patch_golem_client(mock_golem_client):
    """Patch GolemBase client creation."""
    with patch('golemdb_sql.connection.GolemBaseClient', return_value=mock_golem_client):
        yield mock_golem_client


# Test data fixtures

@pytest.fixture
def sample_users_data():
    """Sample users data for testing."""
    return [
        {
            "id": 1,
            "name": "Alice Johnson",
            "email": "alice@example.com", 
            "age": 28,
            "is_active": True,
            "created_at": "2023-01-15T10:30:00"
        },
        {
            "id": 2,
            "name": "Bob Smith",
            "email": "bob@example.com",
            "age": 32,
            "is_active": True, 
            "created_at": "2023-01-20T14:45:00"
        },
        {
            "id": 3,
            "name": "Charlie Brown",
            "email": "charlie@example.com",
            "age": 25,
            "is_active": False,
            "created_at": "2023-02-01T09:15:00"
        }
    ]


@pytest.fixture
def sample_sql_queries():
    """Sample SQL queries for testing."""
    return {
        "select_all": "SELECT * FROM users",
        "select_with_where": "SELECT id, name, email FROM users WHERE age > 25 AND is_active = TRUE",
        "select_with_params": "SELECT * FROM users WHERE name = :name AND age = :age",
        "select_with_limit": "SELECT * FROM users ORDER BY name LIMIT 10 OFFSET 5",
        "insert_basic": "INSERT INTO users (name, email, age) VALUES ('John', 'john@example.com', 30)",
        "insert_with_params": "INSERT INTO users (name, email, age) VALUES (:name, :email, :age)",
        "update_basic": "UPDATE users SET name = 'Jane' WHERE id = 1",
        "update_with_params": "UPDATE users SET name = :name, age = :age WHERE id = :user_id",
        "delete_basic": "DELETE FROM users WHERE age < 18",
        "delete_with_params": "DELETE FROM users WHERE id = :user_id"
    }


@pytest.fixture
def create_table_sql():
    """CREATE TABLE SQL for testing."""
    return """
    CREATE TABLE users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name VARCHAR(100) NOT NULL,
        email VARCHAR(255) UNIQUE,
        age INTEGER,
        is_active BOOLEAN DEFAULT TRUE,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        profile_data JSON,
        avatar BLOB
    )
    """


# Database setup fixtures

@pytest.fixture
def db_with_sample_data(mock_golem_client, sample_users_data):
    """Mock database with sample data."""
    # Add test entities to mock client
    for i, user_data in enumerate(sample_users_data):
        entity_id = f"user_{user_data['id']:06d}"
        
        # Serialize user data to JSON
        import json
        json_data = json.dumps({
            '_table': 'users',
            '_version': 1,
            '_created_at': user_data['created_at'],
            **user_data
        }).encode('utf-8')
        
        # Create annotations
        string_annotations = {
            'table': 'users',
            'name': user_data['name'],
            'email': user_data['email']
        }
        
        numeric_annotations = {
            'id': user_data['id'],
            'age': user_data['age'],
            'is_active': 1 if user_data['is_active'] else 0
        }
        
        mock_golem_client.add_test_entity(
            entity_id=entity_id,
            data=json_data,
            string_annotations=string_annotations,
            numeric_annotations=numeric_annotations
        )
    
    return mock_golem_client


# Error simulation fixtures

@pytest.fixture
def failing_golem_client():
    """Mock GolemBase client that raises errors."""
    client = Mock()
    client.create_entities.side_effect = Exception("Network error")
    client.update_entities.side_effect = Exception("Network error")
    client.delete_entities.side_effect = Exception("Network error")
    client.query_entities.side_effect = Exception("Network error")
    return client


# SQL dialect test fixtures

@pytest.fixture
def sql_test_cases():
    """SQL test cases for dialect testing."""
    return {
        "supported": [
            "SELECT * FROM users",
            "SELECT id, name FROM users WHERE age > 25",
            "INSERT INTO users (name) VALUES ('test')",
            "UPDATE users SET name = 'new' WHERE id = 1", 
            "DELETE FROM users WHERE id = 1"
        ],
        "unsupported": [
            "SELECT COUNT(*) FROM users",  # Aggregates
            "SELECT * FROM users u JOIN posts p ON u.id = p.user_id",  # JOINs
            "SELECT * FROM users WHERE id IN (SELECT id FROM posts)",  # Subqueries
            "CREATE TABLE test (id INTEGER)",  # DDL
            "ALTER TABLE users ADD COLUMN test VARCHAR(50)",  # DDL
            "DROP TABLE users"  # DDL
        ],
        "with_params": [
            ("SELECT * FROM users WHERE name = :name", {"name": "Alice"}),
            ("SELECT * FROM users WHERE age = ? AND is_active = ?", [25, True]),
            ("INSERT INTO users (name, email) VALUES (:name, :email)", {"name": "Test", "email": "test@example.com"}),
            ("UPDATE users SET age = :age WHERE id = :id", {"age": 30, "id": 1}),
            ("DELETE FROM users WHERE name = :name", {"name": "Test"})
        ]
    }


# Integration test fixtures

@pytest.fixture
def integration_test_schema():
    """Schema for integration tests."""
    return {
        "users": """
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            username VARCHAR(50) UNIQUE NOT NULL,
            email VARCHAR(100) UNIQUE NOT NULL,
            full_name VARCHAR(150),
            age INTEGER,
            is_active BOOLEAN DEFAULT TRUE,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """,
        "posts": """
        CREATE TABLE posts (
            id INTEGER PRIMARY KEY,
            user_id INTEGER NOT NULL,
            title VARCHAR(200) NOT NULL,
            content TEXT,
            published BOOLEAN DEFAULT FALSE,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """
    }


# Performance test fixtures

@pytest.fixture
def large_dataset():
    """Large dataset for performance testing."""
    import random
    from datetime import datetime, timedelta
    
    dataset = []
    start_date = datetime.now() - timedelta(days=365)
    
    for i in range(1000):
        created_at = start_date + timedelta(
            days=random.randint(0, 365),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59)
        )
        
        dataset.append({
            "id": i + 1,
            "name": f"User {i+1:04d}",
            "email": f"user{i+1:04d}@example.com",
            "age": random.randint(18, 80),
            "is_active": random.choice([True, False]),
            "created_at": created_at.isoformat()
        })
    
    return dataset


# Utility fixtures

@pytest.fixture
def assert_sql_equivalent():
    """Helper to assert SQL equivalence."""
    def _assert_equivalent(sql1, sql2):
        """Compare SQL statements ignoring whitespace differences."""
        import re
        
        def normalize_sql(sql):
            # Remove extra whitespace and normalize
            sql = re.sub(r'\s+', ' ', sql.strip())
            return sql.upper()
        
        assert normalize_sql(sql1) == normalize_sql(sql2)
    
    return _assert_equivalent


@pytest.fixture  
def capture_queries():
    """Capture executed queries for testing."""
    queries = []
    
    def add_query(sql, params=None):
        queries.append({"sql": sql, "params": params})
    
    # Can be used to patch query execution methods
    add_query.queries = queries
    add_query.clear = lambda: queries.clear()
    
    return add_query