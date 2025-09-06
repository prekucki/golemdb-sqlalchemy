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
    
    # GolemBase-specific features
    supports_statement_cache = True
    supports_schemas = True
    max_identifier_length = 128
    
    # Reflection capabilities  
    supports_views = False
    supports_sequences = False
    sequences_optional = True
    preexecute_autoincrement_sequences = False
    
    # Compiler classes
    statement_compiler = GolemBaseCompiler
    type_compiler = GolemBaseTypeCompiler
    
    # Connection pooling
    poolclass = pool.QueuePool
    
    def __init__(self, **kwargs):
        """Initialize the GolemBase dialect."""
        super().__init__(**kwargs)
        self.type_map = GolemBaseTypeMap()
    
    def do_execute(self, cursor, statement, parameters, context=None):
        """Execute a statement with proper parameter handling."""
        # GolemBase expects %(name)s style parameters
        # SQLAlchemy uses :name style, so we need to convert
        if parameters and isinstance(parameters, dict):
            # Convert :name to %(name)s in the statement if needed
            converted_statement = statement
            for param_name in parameters.keys():
                # Replace :param with %(param)s
                converted_statement = converted_statement.replace(
                    f":{param_name}", f"%({param_name})s"
                )
            statement = converted_statement
            
        cursor.execute(statement, parameters or {})
    
    def do_executemany(self, cursor, statement, parameters, context=None):
        """Execute a statement multiple times with parameter sets."""
        # Convert parameter style for each parameter set
        if parameters:
            for param_set in parameters:
                if isinstance(param_set, dict):
                    converted_statement = statement
                    for param_name in param_set.keys():
                        converted_statement = converted_statement.replace(
                            f":{param_name}", f"%({param_name})s"
                        )
                    cursor.execute(converted_statement, param_set)
        else:
            cursor.executemany(statement, parameters)
    
    @classmethod
    def import_dbapi(cls):
        """Import and return the DB-API module for GolemBase.
        
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
    
    @classmethod
    def dbapi(cls):
        """Return the DB-API module for GolemBase (deprecated).
        
        This method is deprecated in favor of import_dbapi().
        """
        return cls.import_dbapi()
    
    def create_connect_args(self, url):
        """Create connection arguments from URL.
        
        GolemBase URLs should be in the format:
        golembase://private_key@hostname/schema_id?rpc_url=xxx&ws_url=xxx&app_id=xxx
        
        Or with all parameters in query string:
        golembase:///schema_id?rpc_url=xxx&ws_url=xxx&private_key=xxx&app_id=xxx
        """
        # Start with basic URL components
        opts = {}
        
        # Extract schema_id from database part
        if url.database:
            opts['schema_id'] = url.database
            
        # Extract private_key from username if provided
        if url.username:
            opts['private_key'] = url.username
            
        # Handle query parameters (these take precedence)
        query_params = dict(url.query)
        
        # Map required GolemBase connection parameters
        golembase_params = [
            'rpc_url', 'ws_url', 'private_key', 'app_id', 'schema_id'
        ]
        
        # Update opts with query parameters
        opts.update(query_params)
        
        # Validate that we have required parameters
        missing_params = []
        for param in ['rpc_url', 'private_key']:  # Minimum required
            if param not in opts or not opts[param]:
                missing_params.append(param)
                
        if missing_params:
            raise ValueError(
                f"Missing required GolemBase connection parameters: {missing_params}. "
                "URL should include rpc_url, private_key, and optionally ws_url, app_id, schema_id."
            )
            
        # Set defaults for optional parameters
        if 'ws_url' not in opts or not opts['ws_url']:
            # Try to derive ws_url from rpc_url if not provided
            rpc_url = opts['rpc_url']
            if rpc_url.startswith('https://'):
                opts['ws_url'] = rpc_url.replace('https://', 'wss://') + '/ws'
            elif rpc_url.startswith('http://'):
                opts['ws_url'] = rpc_url.replace('http://', 'ws://') + '/ws'
                
        if 'app_id' not in opts or not opts['app_id']:
            opts['app_id'] = 'sqlalchemy_app'
            
        if 'schema_id' not in opts or not opts['schema_id']:
            opts['schema_id'] = 'default_schema'
            
        return ([], opts)
    
    def do_rollback(self, dbapi_connection):
        """Rollback a transaction."""
        try:
            dbapi_connection.rollback()
        except AttributeError:
            # GolemBase might not support traditional transactions
            pass
    
    def do_commit(self, dbapi_connection):
        """Commit a transaction."""  
        try:
            dbapi_connection.commit()
        except AttributeError:
            # GolemBase might not support traditional transactions
            pass
    
    def do_close(self, dbapi_connection):
        """Close a connection."""
        try:
            dbapi_connection.close()
        except Exception:
            # Handle any connection closure errors gracefully
            pass
    
    def is_disconnect(self, e, connection, cursor):
        """Check if an exception indicates a disconnected state."""
        if connection is None:
            return True
            
        # Check for common disconnection error patterns
        error_msg = str(e).lower()
        disconnect_patterns = [
            'connection closed',
            'connection lost',
            'connection refused', 
            'network is unreachable',
            'connection reset',
            'connection timeout',
            'websocket connection closed',
            'rpc connection failed'
        ]
        
        return any(pattern in error_msg for pattern in disconnect_patterns)
    
    def do_ping(self, dbapi_connection):
        """Check if the connection is still alive."""
        try:
            # Use a simple constant query that doesn't require tables
            cursor = dbapi_connection.cursor()
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            cursor.close()
            # Verify we got the expected result
            return result is not None and result[0] == 1
        except Exception:
            return False
    
    def get_schema_names(self, connection, **kw):
        """Return a list of schema names available in the database."""
        # GolemBase uses schema_id as the schema concept
        # For now, return the current schema as the only available schema
        # This could be extended if GolemBase supports schema introspection
        return ["public"]
    
    def get_table_names(self, connection, schema=None, **kw):
        """Return a list of table names in the given schema."""
        # GolemBase might not support SHOW TABLES command
        # Return empty list for now - users can manually specify tables
        # This could be enhanced if GolemBase adds table introspection support
        try:
            # Attempt to use SHOW TABLES if supported
            query = "SHOW TABLES"
            if schema:
                query += f" FROM {schema}"
            
            result = connection.execute(query)
            return [row[0] for row in result.fetchall()]
        except Exception:
            # If SHOW TABLES is not supported, return empty list
            # SQLAlchemy can still work with explicitly defined tables
            return []
    
    def get_view_names(self, connection, schema=None, **kw):
        """Return a list of view names in the given schema."""
        # GolemBase likely doesn't support views yet
        return []
    
    def get_columns(self, connection, table_name, schema=None, **kw):
        """Return column information for the given table."""
        from sqlalchemy.sql import text
        
        try:
            # Use DESCRIBE command to get column information
            table_ref = f"{schema}.{table_name}" if schema else table_name
            query = f"DESCRIBE {table_ref}"
            
            result = connection.execute(text(query))
            columns = []
            
            for row in result.fetchall():
                # DESCRIBE format: Field, Type, Null, Key, Default, Extra
                if len(row) < 3:
                    continue
                    
                col_name = row[0]
                col_type = row[1] 
                nullable = row[2] == 'YES'
                key_type = row[3] if len(row) > 3 else None
                default = row[4] if len(row) > 4 and row[4] is not None else None
                extra = row[5] if len(row) > 5 else None
                
                # Determine autoincrement
                autoincrement = (
                    key_type == 'PRI' and 
                    'INTEGER' in col_type.upper() and 
                    extra == 'auto_increment'
                )
                
                columns.append({
                    'name': col_name,
                    'type': self._map_column_type(col_type),
                    'nullable': nullable,
                    'default': default,
                    'autoincrement': autoincrement,
                    'primary_key': key_type == 'PRI',
                })
            
            return columns
        except Exception:
            # If DESCRIBE is not supported, return empty list
            return []
    
    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        """Return primary key constraint information."""
        from sqlalchemy.sql import text
        
        try:
            # Use DESCRIBE to find primary key columns
            table_ref = f"{schema}.{table_name}" if schema else table_name
            query = f"DESCRIBE {table_ref}"
            
            result = connection.execute(text(query))
            pk_columns = []
            
            for row in result.fetchall():
                if len(row) > 3 and row[3] == 'PRI':  # Key column is 'PRI'
                    pk_columns.append(row[0])  # Column name
            
            return {
                'constrained_columns': pk_columns,
                'name': f"pk_{table_name}" if pk_columns else None
            }
        except Exception:
            return {
                'constrained_columns': [],
                'name': None
            }
    
    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        """Return foreign key constraint information."""
        # GolemBase might not have traditional foreign keys
        # For now, return empty list - could be enhanced if FK support is added
        try:
            # Could potentially query schema metadata for foreign key relationships
            # This would require additional introspection commands in golemdb-sql
            return []
        except Exception:
            return []
    
    def get_indexes(self, connection, table_name, schema=None, **kw):
        """Return index information for the given table."""
        from sqlalchemy.sql import text
        
        try:
            # Use DESCRIBE to find indexed columns
            table_ref = f"{schema}.{table_name}" if schema else table_name
            query = f"DESCRIBE {table_ref}"
            
            result = connection.execute(text(query))
            indexes = []
            
            for row in result.fetchall():
                if len(row) > 3 and row[3] in ('MUL', 'UNI'):  # Indexed columns
                    col_name = row[0]
                    is_unique = row[3] == 'UNI'
                    
                    # Create index entry
                    index_name = f"idx_{table_name}_{col_name}"
                    indexes.append({
                        'name': index_name,
                        'column_names': [col_name],
                        'unique': is_unique,
                        'type': 'btree',  # Default index type
                        'dialect_options': {}
                    })
            
            return indexes
        except Exception:
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