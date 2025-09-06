# GolemBase SQLAlchemy Dialect Test Application

A FastAPI test application to validate the functionality of the SQLAlchemy GolemBase dialect.

## Setup

1. Ensure you have Python 3.10+ installed
2. Install Poetry if not already installed
3. Install dependencies:

```bash
cd testapp
poetry install
```

## Running the Test Application

1. Set up your GolemBase connection (optional):
```bash
export GOLEMBASE_DATABASE_URL="golembase://user:password@localhost:5432/testdb"
```

2. Start the application:
```bash
poetry run python -m testapp.main
```

Or using uvicorn directly:
```bash
poetry run uvicorn testapp.main:app --host 0.0.0.0 --port 8000
```

## Usage

- Application: http://localhost:8000
- Interactive API docs: http://localhost:8000/docs
- Alternative docs: http://localhost:8000/redoc

## Available Endpoints

### Health & Testing
- `GET /health` - Health check
- `GET /test/connection` - Test database connection
- `GET /test/schema` - Test schema introspection

### User Management
- `POST /users/` - Create user
- `GET /users/` - List users
- `GET /users/{user_id}` - Get user by ID
- `DELETE /users/{user_id}` - Delete user

### Post Management
- `POST /posts/` - Create post
- `GET /posts/` - List posts
- `GET /posts/{post_id}` - Get post by ID
- `GET /users/{user_id}/posts` - Get posts by user

## Development

### Running Tests
```bash
poetry run pytest
```

### Code Formatting
```bash
poetry run ruff format .
poetry run ruff check --fix .
```

### Type Checking
```bash
poetry run mypy src/
```

## Project Structure

```
testapp/
├── src/
│   └── testapp/
│       ├── __init__.py
│       ├── main.py        # FastAPI application
│       ├── models.py      # SQLAlchemy models
│       ├── database.py    # Database management
│       └── crud.py        # CRUD operations
├── tests/                 # Test files
├── pyproject.toml         # Project configuration
└── README.md             # This file
```