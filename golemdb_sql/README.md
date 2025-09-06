# golemdb-sql

A **PEP 249 compliant** Python Database API 2.0 implementation for GolemBase - the world's first **decentralized database**.

Transform GolemBase into a familiar SQL database interface that works seamlessly with Python applications, ORMs like SQLAlchemy, and any tool expecting standard database connectivity.

## 🚀 Key Features

### **Standard SQL Interface**
- **Full DDL Support**: `CREATE TABLE`, `CREATE INDEX`, `DROP TABLE`, `DROP INDEX` 
- **Complete DML Operations**: `SELECT`, `INSERT`, `UPDATE`, `DELETE` with complex WHERE clauses
- **PEP 249 Compliant**: Drop-in replacement for any Python database driver
- **Transaction Management**: Full commit/rollback support with context managers

### **GolemBase Integration** 
- **Multi-tenant Architecture**: Project-based schema isolation (`relation="project.table"`)
- **Advanced Type System**: Proper encoding for signed integers, DECIMAL precision, datetime handling
- **Query Translation**: SQL automatically converted to GolemDB annotation queries
- **Schema Persistence**: Automatic TOML-based schema management with platform-specific storage

### **Developer Experience**
- **Environment Variables**: Secure `.env` file configuration support
- **Comprehensive Error Handling**: Detailed error messages with context
- **Iterator Protocol**: Pythonic cursor iteration support
- **Connection Pooling**: Thread-safe connection sharing (level 1)
- **SQLAlchemy Ready**: Works out-of-the-box with ORMs

## 📦 Installation

```bash
# From PyPI (when published)
pip install golemdb-sql

# From source
git clone <repository-url>
cd golemdb-sqlalchemy/golemdb_sql
poetry install
```

**Requirements**: Python 3.10+ • golem-base-sdk==0.1.0

## ⚡ Quick Start

### 🔧 Setup

Create a `.env` file for secure configuration:

```bash
# .env
PRIVATE_KEY=0x1234...your-private-key
RPC_URL=https://ethwarsaw.holesky.golemdb.io/rpc
WS_URL=wss://ethwarsaw.holesky.golemdb.io/rpc/ws
APP_ID=myapp
SCHEMA_ID=production
```

### 🏗️ Create Tables & Indexes

```python
import golemdb_sql
import os

# Connect using environment variables
conn = golemdb_sql.connect(
    rpc_url=os.getenv('RPC_URL'),
    ws_url=os.getenv('WS_URL'),
    private_key=os.getenv('PRIVATE_KEY'),
    app_id=os.getenv('APP_ID'),
    schema_id=os.getenv('SCHEMA_ID')
)

cursor = conn.cursor()

# CREATE TABLE with full SQL syntax
cursor.execute("""
    CREATE TABLE users (
        id INTEGER PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        email VARCHAR(200) NOT NULL UNIQUE,
        balance DECIMAL(10,2) DEFAULT 0.00,
        active BOOLEAN DEFAULT TRUE,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
""")

# CREATE INDEX for query optimization
cursor.execute("CREATE INDEX idx_users_active ON users(active)")
cursor.execute("CREATE INDEX idx_users_created_at ON users(created_at)")

print("✅ Tables and indexes created!")
```

### 📝 Insert & Query Data

```python
# Insert data
users = [
    {'name': 'Alice Smith', 'email': 'alice@example.com', 'balance': 1250.50},
    {'name': 'Bob Johnson', 'email': 'bob@example.com', 'balance': 750.25},
    {'name': 'Carol White', 'email': 'carol@example.com', 'balance': 2100.00}
]

cursor.executemany(
    "INSERT INTO users (name, email, balance) VALUES (%(name)s, %(email)s, %(balance)s)",
    users
)

# Query with WHERE clause
cursor.execute(
    "SELECT id, name, balance FROM users WHERE balance > %(min_balance)s ORDER BY balance DESC",
    {'min_balance': 1000.00}
)

# Fetch and display results
for user_id, name, balance in cursor:
    print(f"User {user_id}: {name} - ${balance:,.2f}")

# Commit and close
conn.commit()
cursor.close()
conn.close()
```

### 🔄 Transaction Management

```python
# Context manager ensures automatic cleanup
with golemdb_sql.connect(
    rpc_url=os.getenv('RPC_URL'),
    ws_url=os.getenv('WS_URL'), 
    private_key=os.getenv('PRIVATE_KEY'),
    app_id='myapp'
) as conn:
    cursor = conn.cursor()
    
    try:
        # Multiple operations in single transaction
        cursor.execute("CREATE TABLE products (id INTEGER PRIMARY KEY, name VARCHAR(100), price DECIMAL(10,2))")
        cursor.execute("INSERT INTO products (name, price) VALUES (%(name)s, %(price)s)", 
                      {'name': 'Widget', 'price': 29.99})
        cursor.execute("UPDATE products SET price = %(price)s WHERE name = %(name)s",
                      {'name': 'Widget', 'price': 24.99})
        
        # Auto-commit on success, auto-rollback on exception
        
    except Exception as e:
        print(f"Transaction failed: {e}")
        # Rollback automatic via context manager
```

### DDL Support - Create Tables and Indexes

GolemDB-SQL supports standard DDL operations through cursor.execute():

```python
import golemdb_sql

conn = golemdb_sql.connect(
    rpc_url='https://ethwarsaw.holesky.golemdb.io/rpc',
    ws_url='wss://ethwarsaw.holesky.golemdb.io/rpc/ws', 
    private_key='0x0000000000000000000000000000000000000000000000000000000000000001',
    app_id='blog_app',
    schema_id='production'
)

cursor = conn.cursor()

# CREATE TABLE with full SQL syntax support
cursor.execute("""
    CREATE TABLE posts (
        id INTEGER PRIMARY KEY,
        title VARCHAR(200) NOT NULL,
        content TEXT,
        author_id INTEGER NOT NULL,
        is_published BOOLEAN DEFAULT FALSE,
        price DECIMAL(10,2),
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
""")

# CREATE INDEX for query optimization
cursor.execute("CREATE INDEX idx_posts_author_id ON posts(author_id)")
cursor.execute("CREATE INDEX idx_posts_is_published ON posts(is_published)")
cursor.execute("CREATE INDEX idx_posts_created_at ON posts(created_at)")

# DROP operations with IF EXISTS support
cursor.execute("DROP INDEX IF EXISTS idx_old_index")
cursor.execute("DROP TABLE IF EXISTS old_table")

# Schema is automatically saved to platform-specific directories:
# Linux: ~/.local/share/golembase/schemas/production.toml
# macOS: ~/Library/Application Support/golembase/schemas/production.toml  
# Windows: %APPDATA%/golembase/schemas/production.toml
```

## API Reference

### Module Attributes

- `apilevel`: "2.0" - DB-API version
- `threadsafety`: 1 - Thread safety level  
- `paramstyle`: "named" - Parameter style (supports %(name)s)

### Connection Class

#### Methods

- `cursor()` → Cursor: Create new cursor
- `commit()`: Commit current transaction
- `rollback()`: Rollback current transaction  
- `close()`: Close connection
- `execute(sql, params=None)` → Cursor: Execute SQL directly
- `executemany(sql, seq_params)` → Cursor: Execute SQL multiple times

#### Properties

- `closed`: bool - True if connection is closed
- `autocommit`: bool - Autocommit mode setting

### Cursor Class

#### Methods

- `execute(sql, params=None)`: Execute SQL statement
- `executemany(sql, seq_params)`: Execute SQL multiple times
- `fetchone()` → tuple | None: Fetch next row
- `fetchmany(size=None)` → List[tuple]: Fetch multiple rows
- `fetchall()` → List[tuple]: Fetch all remaining rows
- `close()`: Close cursor

#### Properties  

- `description`: Column descriptions
- `rowcount`: Number of affected/returned rows
- `arraysize`: Default fetch size for fetchmany()
- `rownumber`: Current row number in result set

### Type Constructors

```python
from golemdb_sql import Date, Time, Timestamp, Binary

# Date/time constructors
date_val = Date(2023, 12, 25)
time_val = Time(14, 30, 0) 
timestamp_val = Timestamp(2023, 12, 25, 14, 30, 0)

# From Unix timestamps
date_from_ts = DateFromTicks(1703509800)
time_from_ts = TimeFromTicks(1703509800)
timestamp_from_ts = TimestampFromTicks(1703509800)

# Binary data
binary_val = Binary(b'binary data')
```

### Exception Hierarchy

```
Exception
 └── Warning
 └── Error
     ├── InterfaceError  
     └── DatabaseError
         ├── DataError
         ├── OperationalError
         ├── IntegrityError
         ├── InternalError
         ├── ProgrammingError
         └── NotSupportedError
```

### Usage with SQLAlchemy

This package is designed to work seamlessly with the SQLAlchemy GolemBase dialect:

```python
from sqlalchemy import create_engine

# The SQLAlchemy dialect will automatically use this DB-API package
engine = create_engine(
    "golembase://0x0000000000000000000000000000000000000000000000000000000000000001@ethwarsaw.holesky.golemdb.io/myapp"
    "?ws_url=wss://ethwarsaw.holesky.golemdb.io/rpc/ws&schema_id=production"
)

# Use with SQLAlchemy ORM
from sqlalchemy.orm import sessionmaker
Session = sessionmaker(bind=engine)
session = Session()
```

## Configuration

### Connection Parameters

The `connect()` function accepts these GolemBase parameters:

- `rpc_url`: HTTPS RPC endpoint URL (required)
- `ws_url`: WebSocket URL for real-time events (required) 
- `private_key`: Hex private key for authentication (required)
- `app_id`: Application/Project identifier (default: 'default')
- `schema_id`: Schema configuration identifier (default: 'default')
- Additional parameters supported by golem-base-sdk

### Connection String Format

You can also use connection strings in multiple formats:

```python
# Key-value format
conn = golemdb_sql.connect(connection_string=
    "rpc_url=https://ethwarsaw.holesky.golemdb.io/rpc "
    "ws_url=wss://ethwarsaw.holesky.golemdb.io/rpc/ws "
    "private_key=0x0000000000000000000000000000000000000000000000000000000000000001 "
    "app_id=myapp schema_id=production"
)

# URL format
conn = golemdb_sql.connect(connection_string=
    "golembase://0x0000000000000000000000000000000000000000000000000000000000000001@ethwarsaw.holesky.golemdb.io/myapp"
    "?ws_url=wss://ethwarsaw.holesky.golemdb.io/rpc/ws&schema_id=production"
)

# Environment variables support
import os
conn = golemdb_sql.connect(
    rpc_url=os.getenv('RPC_URL'),
    ws_url=os.getenv('WS_URL'),
    private_key=os.getenv('PRIVATE_KEY'),
    app_id=os.getenv('APP_ID', 'default'),
    schema_id=os.getenv('SCHEMA_ID', 'production')
)

# Using .env files for configuration
# Create a .env file in your project directory:
# PRIVATE_KEY=0x1234...
# RPC_URL=https://ethwarsaw.holesky.golemdb.io/rpc
# WS_URL=wss://ethwarsaw.holesky.golemdb.io/rpc/ws
# APP_ID=myapp
# SCHEMA_ID=production
```

For a complete example with .env file support, see [`example_usage.py`](example_usage.py).

## Error Handling

```python
import golemdb_sql
from golemdb_sql import DatabaseError, IntegrityError

try:
    with golemdb_sql.connect(
        rpc_url='https://ethwarsaw.holesky.golemdb.io/rpc',
        ws_url='wss://ethwarsaw.holesky.golemdb.io/rpc/ws',
        private_key='0x0000000000000000000000000000000000000000000000000000000000000001',
        app_id='myapp'
    ) as conn:
        cursor = conn.cursor()
        cursor.execute("INSERT INTO users (id, name, email) VALUES (%(id)s, %(name)s, %(email)s)",
                      {'id': 1, 'name': 'Alice', 'email': 'alice@example.com'})
        
except IntegrityError as e:
    print(f"Constraint violation: {e}")
except DatabaseError as e:
    print(f"Database error: {e}")
except Exception as e:
    print(f"Unexpected error: {e}")
```

## Development

### Development Setup

```bash
# Clone and navigate to project
git clone <repo-url>
cd golemdb-sqlalchemy/golemdb_sql

# Set up development environment
poetry install --with dev

# Install pre-commit hooks (if available)
pre-commit install

# Run type checking
poetry run mypy src/

# Run linting
poetry run ruff check src/ tests/

# Format code
poetry run ruff format src/ tests/
```

### Running Tests

The golemdb_sql subproject has its own comprehensive test suite:

```bash
# Run all tests
poetry run pytest

# Run tests with coverage
poetry run pytest --cov=golemdb_sql --cov-report=html

# Run specific test files
poetry run pytest tests/test_signed_integer_encoding.py -v
poetry run pytest tests/test_decimal_precision_scale.py -v

# Run tests matching a pattern
poetry run pytest -k "test_decimal" -v
poetry run pytest -k "test_signed" -v
```

### Test Structure

- `tests/test_types.py` - Type conversion and encoding tests
- `tests/test_signed_integer_encoding.py` - Comprehensive signed integer encoding tests
- `tests/test_decimal_precision_scale.py` - DECIMAL precision/scale and string encoding tests
- `tests/test_schema_manager.py` - Schema management and TOML persistence tests  
- `tests/test_query_translator.py` - SQL to GolemDB query translation tests
- `tests/test_row_serializer.py` - Entity serialization/deserialization tests
- `tests/test_connection.py` - DB-API connection and cursor tests
- `tests/conftest.py` - Shared test fixtures and utilities

### Code Quality

```bash
# Format code
poetry run ruff format .

# Check linting
poetry run ruff check .

# Type checking  
poetry run mypy src/
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Run the test suite and linting
6. Submit a pull request

## License

MIT License - see LICENSE file for details.

## Changelog

### 0.1.0
- Initial release
- Complete PEP 249 implementation
- Transaction support
- Context manager support
- Type constructors and mapping
- Full exception hierarchy