# SQLAlchemy GolemBase Dialect

A SQLAlchemy dialect for GolemBase database, providing seamless integration between SQLAlchemy ORM and GolemBase.

## Project Structure

This project uses a multi-project structure:

```
golemdb-sqlalchemy/
├── sqlalchemy_golembase/          # Main dialect package
│   ├── src/
│   │   └── sqlalchemy_dialects_golembase/
│   │       ├── __init__.py
│   │       ├── dialect.py         # Core dialect implementation
│   │       ├── compiler.py        # SQL compilation logic  
│   │       └── types.py           # Type mapping and custom types
│   └── tests/                     # Dialect unit tests
├── testapp/                       # Test application
│   ├── src/
│   │   └── testapp/
│   │       ├── __init__.py
│   │       ├── main.py            # FastAPI test application
│   │       ├── models.py          # SQLAlchemy models for testing
│   │       ├── database.py        # Database connection management
│   │       └── crud.py            # CRUD operations for testing
│   └── tests/                     # Integration tests
├── docs/                          # Documentation
├── pyproject.toml                 # Root project configuration with Poetry
├── Makefile                       # Development commands
├── README.md
└── .gitignore
```

## Installation

### Development Setup

1. Clone the repository:
```bash
git clone <repository-url>
cd golemdb-sqlalchemy
```

2. Ensure you have Python 3.10 or later installed:
```bash
python --version  # Should show 3.10+
```

3. Install Poetry (if not already installed):
```bash
curl -sSL https://install.python-poetry.org | python3 -
```

4. Install dependencies:
```bash
poetry install --with=dev
```

5. Activate the virtual environment:
```bash
poetry shell
```

### Production Installation

```bash
pip install sqlalchemy-dialects-golembase
```

Or with Poetry:
```bash
poetry add sqlalchemy-dialects-golembase
```

**Note**: Requires Python 3.10 or later and golem-base-sdk==0.1.0

## Usage

### Basic Connection

```python
from sqlalchemy import create_engine

# Connect to GolemBase
engine = create_engine("golembase://username:password@host:port/database")
```

### With SQLAlchemy ORM

```python
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

engine = create_engine("golembase://username:password@host:port/database")
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Use session
session = SessionLocal()
try:
    # Your ORM operations here
    pass
finally:
    session.close()
```

## Testing

### Running the Test Application

The test application is a separate FastAPI project that demonstrates the dialect functionality:

1. Install testapp dependencies:
```bash
cd testapp
poetry install
```

2. Set up the database connection (optional):
```bash
export GOLEMBASE_DATABASE_URL="golembase://user:password@localhost:5432/testdb"
```

3. Start the test application:
```bash
# From testapp directory
poetry run python -m testapp.main

# Or from root directory
make run-app
```

3. Visit `http://localhost:8000/docs` for the interactive API documentation.

### Available Test Endpoints

- `GET /health` - Health check
- `POST /users/` - Create user
- `GET /users/` - List users
- `GET /users/{user_id}` - Get user by ID
- `DELETE /users/{user_id}` - Delete user
- `POST /posts/` - Create post
- `GET /posts/` - List posts
- `GET /users/{user_id}/posts` - Get posts by user
- `GET /test/connection` - Test database connection
- `GET /test/schema` - Test schema introspection

### Running Unit Tests

```bash
# Run all tests
poetry run pytest
# or
make test

# Test the dialect only
make test-dialect

# Test the application only
make test-app
```

## Development

### Project Layout

- **sqlalchemy_golembase/**: Contains the core dialect implementation
  - `dialect.py`: Main dialect class with connection and introspection logic
  - `compiler.py`: SQL statement and type compilation
  - `types.py`: GolemBase-specific type mapping and custom types

- **testapp/**: Test application for validating dialect functionality
  - `main.py`: FastAPI application with CRUD endpoints
  - `models.py`: SQLAlchemy models for testing
  - `database.py`: Database connection management
  - `crud.py`: CRUD operations

### Key Features

- **Poetry-based** dependency management with proper groups
- **Multi-project structure** with dialect and test app
- **Complete dialect implementation** with introspection, compilation, and type mapping
- **FastAPI test application** with CRUD endpoints to validate functionality
- **Development tools** configured (Ruff, mypy, pytest)

### Development Commands

```bash
# Setup development environment (installs both dialect and testapp)
make setup-dev

# Install just the main dialect
make install-dev

# Install just the testapp
make install-app

# Format code
make format

# Run linting
make lint

# Run all tests
make test

# Run dialect tests only
make test-dialect

# Run testapp tests only
make test-app

# Start test application
make run-app

# Clean build artifacts
make clean
```

### Customizing for Your GolemBase Setup

1. **Update the DB-API module**: Modify `dialect.py` to import your actual GolemBase client library instead of the placeholder.

2. **Implement connection parameters**: Adjust `create_connect_args()` method to handle GolemBase-specific connection options.

3. **Schema introspection**: Implement the schema introspection methods (`get_table_names()`, `get_columns()`, etc.) based on your GolemBase system's metadata queries.

4. **Type mapping**: Update `types.py` to handle GolemBase-specific data types.

5. **SQL compilation**: Modify `compiler.py` if GolemBase uses non-standard SQL syntax.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Run the test suite (`make test`)
6. Format code (`make format`)
7. Submit a pull request

## License

MIT License - see LICENSE file for details.