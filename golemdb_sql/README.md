# golemdb-sql

A PEP 249 compliant Python Database API 2.0 implementation for GolemBase database.

This package provides a standard Python database interface that allows GolemBase databases to work with Python applications, ORMs like SQLAlchemy, and other database tools that expect a DB-API 2.0 compliant driver.

## Features

- **PEP 249 Compliant**: Full Python Database API 2.0 specification compliance
- **Transaction Support**: Complete transaction management with commit/rollback
- **Context Manager**: Connection and transaction context manager support  
- **Type Safety**: Proper type mapping between GolemBase and Python types
- **Error Handling**: Complete DB-API exception hierarchy
- **Iterator Protocol**: Cursors support Python iteration
- **Thread Safe**: Thread-safe connection sharing (threadsafety level 1)
- **Schema Management**: Automatic parsing of CREATE TABLE statements with SQLglot
- **Precision/Scale Metadata**: DECIMAL precision/scale extraction from DECIMAL(p,s) definitions  
- **TOML Schema Persistence**: Schema storage in XDG user data directories
- **Advanced Type System**: All signed integers encoded for uint64 storage with ordering preservation
- **DECIMAL String Encoding**: DECIMAL/NUMERIC values stored as lexicographically-ordered strings
- **Query Translation**: SQL WHERE clauses converted to GolemDB annotation queries with automatic value encoding

## Installation

### From PyPI
```bash
pip install golemdb-sql
```

### From Source
```bash
git clone <repository-url>
cd golemdb_sql
poetry install
```

## Requirements

- Python 3.10+
- golem-base-sdk==0.1.0

## Quick Start

### Basic Usage

```python
import golemdb_sql

# Connect to database
conn = golemdb_sql.connect(
    host='localhost',
    port=5432,
    database='mydb',
    user='user',
    password='password'
)

# Execute queries
cursor = conn.cursor()
cursor.execute("SELECT id, name FROM users WHERE active = %(active)s", {'active': True})

# Fetch results
for row in cursor:
    print(f"User {row[0]}: {row[1]}")

# Close connection
cursor.close()
conn.close()
```

### Context Manager Usage

```python
import golemdb_sql

# Automatic connection and transaction management
with golemdb_sql.connect(host='localhost', database='mydb') as conn:
    cursor = conn.cursor()
    
    # Insert data
    cursor.execute(
        "INSERT INTO users (name, email) VALUES (%(name)s, %(email)s)",
        {'name': 'John Doe', 'email': 'john@example.com'}
    )
    
    # Changes are automatically committed if no exceptions occur
    # Connection is automatically closed
```

### Batch Operations

```python
import golemdb_sql

conn = golemdb_sql.connect(**connection_params)
cursor = conn.cursor()

# Execute multiple operations
users = [
    {'name': 'Alice', 'email': 'alice@example.com'},
    {'name': 'Bob', 'email': 'bob@example.com'},
    {'name': 'Carol', 'email': 'carol@example.com'}
]

cursor.executemany(
    "INSERT INTO users (name, email) VALUES (%(name)s, %(email)s)",
    users
)

conn.commit()
cursor.close()
conn.close()
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
engine = create_engine("golembase://user:pass@localhost:5432/mydb")

# Use with SQLAlchemy ORM
from sqlalchemy.orm import sessionmaker
Session = sessionmaker(bind=engine)
session = Session()
```

## Configuration

### Connection Parameters

The `connect()` function accepts these parameters:

- `host`: Database server hostname
- `port`: Database server port  
- `database`: Database name
- `user`: Username
- `password`: Password
- Additional parameters supported by golem-base-sdk

### Connection String Format

You can also use connection strings:

```python
conn = golemdb_sql.quick_connect(
    "host=localhost port=5432 database=mydb user=user password=pass"
)
```

## Error Handling

```python
import golemdb_sql
from golemdb_sql import DatabaseError, IntegrityError

try:
    with golemdb_sql.connect(**params) as conn:
        cursor = conn.cursor()
        cursor.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')")
        
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