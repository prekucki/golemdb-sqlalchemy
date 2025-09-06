"""PEP 249 compliant DB-API 2.0 implementation for GolemBase.

This module provides a Python Database API 2.0 compliant interface for 
GolemBase database connections, allowing standard Python database operations
and compatibility with ORMs like SQLAlchemy.

Example:
    import golemdb_sql
    
    # Connect to database
    conn = golemdb_sql.connect(
        host='localhost',
        port=5432,
        database='mydb',
        user='user', 
        password='pass'
    )
    
    # Execute queries
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM users WHERE id = %(id)s", {'id': 1})
    rows = cursor.fetchall()
    
    # Use with context manager
    with golemdb_sql.connect(**params) as conn:
        cursor = conn.cursor()
        cursor.execute("INSERT INTO users (name) VALUES (%s)", ['John'])
        conn.commit()
"""

# PEP 249 required module attributes
apilevel = "2.0"
threadsafety = 1  # Threads may share the module, but not connections
paramstyle = "named"  # We support named parameters (:name, %(name)s)

# Version information
__version__ = "0.1.0"
__author__ = "Your Name"
__email__ = "your.email@example.com"

# Import and export all DB-API 2.0 components
from .connection import Connection, connect
from .cursor import Cursor
from .exceptions import (
    Warning,
    Error,
    InterfaceError, 
    DatabaseError,
    DataError,
    OperationalError,
    IntegrityError,
    InternalError,
    ProgrammingError,
    NotSupportedError
)
from .types import (
    # Type objects
    STRING,
    BINARY, 
    NUMBER,
    DATETIME,
    ROWID,
    
    # Type constructors
    Date,
    Time, 
    Timestamp,
    DateFromTicks,
    TimeFromTicks,
    TimestampFromTicks,
    Binary,
    
    # Constants
    NULL,
    
    # GolemBase specific
    GOLEMBASE_TYPE_MAP,
    get_type_object,
    convert_golembase_value
)

# PEP 249 exports - these are the standard DB-API 2.0 interface
__all__ = [
    # Module attributes
    'apilevel',
    'threadsafety', 
    'paramstyle',
    
    # Connection
    'connect',
    'Connection',
    'Cursor',
    
    # Exceptions
    'Warning',
    'Error',
    'InterfaceError',
    'DatabaseError', 
    'DataError',
    'OperationalError',
    'IntegrityError',
    'InternalError',
    'ProgrammingError',
    'NotSupportedError',
    
    # Type objects
    'STRING',
    'BINARY',
    'NUMBER', 
    'DATETIME',
    'ROWID',
    
    # Type constructors
    'Date',
    'Time',
    'Timestamp',
    'DateFromTicks',
    'TimeFromTicks', 
    'TimestampFromTicks',
    'Binary',
    
    # Constants
    'NULL',
    
    # Extended interface (GolemBase specific)
    'GOLEMBASE_TYPE_MAP',
    'get_type_object',
    'convert_golembase_value',
]


def get_version() -> str:
    """Get version string.
    
    Returns:
        Version string in format 'major.minor.patch'
    """
    return __version__


def get_client_info() -> dict:
    """Get client library information.
    
    Returns:
        Dictionary with client information
    """
    return {
        'name': 'golemdb-sql',
        'version': __version__,
        'apilevel': apilevel,
        'threadsafety': threadsafety,
        'paramstyle': paramstyle,
        'author': __author__,
        'email': __email__,
    }


# Convenience function for quick connections
def quick_connect(connection_string: str) -> Connection:
    """Create connection from connection string.
    
    Args:
        connection_string: Connection string in format:
            "host=localhost port=5432 database=mydb user=user password=pass"
            
    Returns:
        Connection object
        
    Example:
        conn = quick_connect("host=localhost port=5432 database=test user=admin password=secret")
    """
    params = {}
    for part in connection_string.split():
        if '=' in part:
            key, value = part.split('=', 1)
            params[key.strip()] = value.strip()
    
    return connect(**params)