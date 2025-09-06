"""GolemBase SQLAlchemy dialect implementation."""

from sqlalchemy import pool
from sqlalchemy.engine import default
from sqlalchemy.sql import compiler
from sqlalchemy.types import Integer, String, Text, DateTime, Boolean, Float

from .compiler import GolemBaseCompiler, GolemBaseTypeCompiler
from .types import GolemBaseTypeMap


class GolemBaseDialect(default.DefaultDialect):
    """SQLAlchemy dialect for GolemBase database."""
    
    name = "golembase"
    driver = "golembase"
    
    # Basic dialect configuration
    supports_alter = True
    supports_pk_autoincrement = True
    supports_default_values = True
    supports_empty_insert = True
    supports_unicode_statements = True
    supports_unicode_binds = True
    supports_native_decimal = True
    supports_native_boolean = True
    
    # Connection configuration
    default_paramstyle = "named"
    supports_sane_rowcount = True
    supports_sane_multi_rowcount = True
    
    # Compiler classes
    statement_compiler = GolemBaseCompiler
    type_compiler = GolemBaseTypeCompiler
    
    # Connection pooling
    poolclass = pool.QueuePool
    
    def __init__(self, **kwargs):
        """Initialize the GolemBase dialect."""
        super().__init__(**kwargs)
        self.type_map = GolemBaseTypeMap()
    
    @classmethod
    def dbapi(cls):
        """Return the DB-API module for GolemBase.
        
        Uses the golemdb_sql package which provides PEP 249 compliance.
        """
        try:
            import golemdb_sql
            return golemdb_sql
        except ImportError:
            raise ImportError(
                "golemdb_sql is required to use the GolemBase dialect. "
                "Install it with: pip install golemdb-sql or install the "
                "local package: cd golemdb_sql && pip install -e ."
            )
    
    def create_connect_args(self, url):
        """Create connection arguments from URL."""
        opts = url.translate_connect_args(
            username='user',
            password='password', 
            hostname='host',
            port='port',
            database='database'
        )
        
        # Handle GolemBase specific connection options
        opts.update(url.query)
        
        return ([], opts)
    
    def do_rollback(self, dbapi_connection):
        """Rollback a transaction."""
        dbapi_connection.rollback()
    
    def do_commit(self, dbapi_connection):
        """Commit a transaction."""  
        dbapi_connection.commit()
    
    def do_close(self, dbapi_connection):
        """Close a connection."""
        dbapi_connection.close()
    
    def get_schema_names(self, connection, **kw):
        """Return a list of schema names available in the database."""
        # Implement based on GolemBase schema introspection
        return ["public"]  # Placeholder
    
    def get_table_names(self, connection, schema=None, **kw):
        """Return a list of table names in the given schema."""
        # Implement based on GolemBase table introspection
        query = "SHOW TABLES"
        if schema:
            query += f" FROM {schema}"
        
        result = connection.execute(query)
        return [row[0] for row in result.fetchall()]
    
    def get_view_names(self, connection, schema=None, **kw):
        """Return a list of view names in the given schema."""
        # Implement based on GolemBase view introspection
        return []  # Placeholder
    
    def get_columns(self, connection, table_name, schema=None, **kw):
        """Return column information for the given table."""
        # Implement based on GolemBase column introspection
        query = f"DESCRIBE {table_name}"
        if schema:
            query = f"DESCRIBE {schema}.{table_name}"
        
        result = connection.execute(query)
        columns = []
        
        for row in result.fetchall():
            # Adjust based on actual GolemBase DESCRIBE output format
            col_name = row[0]  # Column name
            col_type = row[1]  # Column type
            nullable = row[2] == 'YES'  # Nullable
            default = row[3]  # Default value
            
            columns.append({
                'name': col_name,
                'type': self._map_column_type(col_type),
                'nullable': nullable,
                'default': default,
                'autoincrement': False,  # Detect based on GolemBase specifics
            })
        
        return columns
    
    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        """Return primary key constraint information."""
        # Implement based on GolemBase constraint introspection
        return {
            'constrained_columns': [],
            'name': None
        }
    
    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        """Return foreign key constraint information."""
        # Implement based on GolemBase foreign key introspection
        return []
    
    def get_indexes(self, connection, table_name, schema=None, **kw):
        """Return index information for the given table."""
        # Implement based on GolemBase index introspection
        return []
    
    def _map_column_type(self, golembase_type):
        """Map GolemBase column types to SQLAlchemy types."""
        type_mapping = {
            'INT': Integer,
            'INTEGER': Integer,
            'VARCHAR': String,
            'TEXT': Text,
            'DATETIME': DateTime,
            'BOOLEAN': Boolean,
            'BOOL': Boolean,
            'FLOAT': Float,
            'DOUBLE': Float,
        }
        
        # Extract base type name (handle types like VARCHAR(255))
        base_type = golembase_type.split('(')[0].upper()
        
        return type_mapping.get(base_type, String)()