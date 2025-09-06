"""PEP 249 DB-API 2.0 compliant Cursor class for GolemBase."""

from typing import Any, Dict, List, Optional, Sequence, Tuple, Union, TYPE_CHECKING

if TYPE_CHECKING:
    from .connection import Connection
from .exceptions import (
    DatabaseError,
    DataError,
    InterfaceError,
    OperationalError,
    ProgrammingError,
)
from .filters import apply_post_filter, has_post_filter_conditions


class Cursor:
    """DB-API 2.0 compliant cursor for GolemBase database operations.
    
    Cursors represent a database cursor, which is used to manage the context 
    of a fetch operation.
    """
    
    def __init__(self, connection: 'Connection'):
        """Initialize cursor with connection.
        
        Args:
            connection: The Connection object that created this cursor
        """
        self._connection = connection
        self._closed = False
        self._results: List[Tuple[Any, ...]] = []
        self._description: Optional[Sequence[Sequence[Any]]] = None
        self._rowcount: int = -1
        self._arraysize: int = 1
        self._rownumber: Optional[int] = None
    
    @property
    def description(self) -> Optional[Sequence[Sequence[Any]]]:
        """Sequence of 7-item sequences describing result columns.
        
        Each sequence contains: (name, type_code, display_size, internal_size, 
        precision, scale, null_ok). Only name and type_code are required.
        """
        return self._description
    
    @property  
    def rowcount(self) -> int:
        """Number of rows that the last .execute*() produced or affected.
        
        The attribute is -1 in case no .execute*() has been performed on the 
        cursor or the rowcount cannot be determined.
        """
        return self._rowcount
    
    @property
    def arraysize(self) -> int:
        """Read/write attribute specifying the number of rows to fetch at a time.
        
        Default value is 1 meaning to fetch one row at a time.
        """
        return self._arraysize
    
    @arraysize.setter
    def arraysize(self, size: int) -> None:
        """Set the arraysize for fetchmany operations."""
        if size < 1:
            raise ValueError("arraysize must be positive")
        self._arraysize = size
    
    @property
    def rownumber(self) -> Optional[int]:
        """0-based index of the cursor in the result set or None."""
        return self._rownumber
    
    @property
    def connection(self) -> 'Connection':
        """The Connection object that created this cursor."""
        return self._connection
    
    def close(self) -> None:
        """Close the cursor now.
        
        The cursor will be unusable from this point forward; an Error exception 
        will be raised if any operation is attempted.
        """
        self._closed = True
        self._results.clear()
        self._description = None
        self._rowcount = -1
        self._rownumber = None
    
    def execute(self, operation: str, parameters: Optional[Union[Dict[str, Any], Sequence[Any]]] = None) -> None:
        """Execute a database operation (query or command).
        
        Args:
            operation: SQL statement to execute
            parameters: Parameters for the SQL statement (dict for named, sequence for positional)
        """
        self._check_cursor()
        
        # Ensure transaction is active for non-autocommit connections
        if hasattr(self._connection, '_ensure_transaction'):
            self._connection._ensure_transaction()
        
        try:
            # Execute query using golem-base-sdk through connection
            result = self._execute_with_sdk(operation, parameters)
            self._process_result(result)
            
        except Exception as e:
            raise DatabaseError(f"Error executing query: {e}")
    
    def executemany(self, operation: str, seq_of_parameters: Sequence[Union[Dict[str, Any], Sequence[Any]]]) -> None:
        """Execute a database operation multiple times.
        
        Args:
            operation: SQL statement to execute
            seq_of_parameters: Sequence of parameter sets
        """
        self._check_cursor()
        
        total_rowcount = 0
        
        for parameters in seq_of_parameters:
            self.execute(operation, parameters)
            if self._rowcount > 0:
                total_rowcount += self._rowcount
        
        # Update rowcount to total affected rows
        self._rowcount = total_rowcount
    
    def fetchone(self) -> Optional[Tuple[Any, ...]]:
        """Fetch the next row of a query result set.
        
        Returns:
            A sequence representing the next row, or None when no more data is available
        """
        self._check_cursor()
        
        if not self._results:
            return None
            
        row = self._results.pop(0)
        self._update_rownumber()
        return row
    
    def fetchmany(self, size: Optional[int] = None) -> List[Tuple[Any, ...]]:
        """Fetch the next set of rows of a query result set.
        
        Args:
            size: Number of rows to fetch. If not given, cursor.arraysize is used
            
        Returns:
            List of sequences representing the rows
        """
        self._check_cursor()
        
        if size is None:
            size = self._arraysize
            
        if size < 0:
            raise ValueError("fetch size must be non-negative")
        
        # Fetch up to 'size' rows
        result = self._results[:size]
        self._results = self._results[size:]
        
        self._update_rownumber()
        return result
    
    def fetchall(self) -> List[Tuple[Any, ...]]:
        """Fetch all remaining rows of a query result set.
        
        Returns:
            List of sequences representing all remaining rows
        """
        self._check_cursor()
        
        result = self._results.copy()
        self._results.clear()
        
        self._update_rownumber()
        return result
    
    def setinputsizes(self, sizes: Sequence[Optional[Any]]) -> None:
        """Set input sizes for parameters (optional method).
        
        This method is optional and may be a no-op for some databases.
        
        Args:
            sizes: Sequence of type objects or sizes for each input parameter
        """
        # This is typically a no-op for most databases
        pass
    
    def setoutputsize(self, size: int, column: Optional[int] = None) -> None:
        """Set a column buffer size for fetches (optional method).
        
        This method is optional and may be a no-op for some databases.
        
        Args:
            size: Maximum size of the column buffer
            column: Column index (0-based) or None for all columns
        """
        # This is typically a no-op for most databases  
        pass
    
    def _check_cursor(self) -> None:
        """Check if cursor is still valid.
        
        Raises InterfaceError if cursor or connection is closed.
        """
        if self._closed:
            raise InterfaceError("Cursor is closed")
            
        if hasattr(self._connection, '_check_connection'):
            self._connection._check_connection()
    
    def _execute_with_sdk(self, operation: str, parameters: Optional[Union[Dict[str, Any], Sequence[Any]]]) -> Any:
        """Execute operation using GolemBase entity operations.
        
        Args:
            operation: SQL statement
            parameters: Query parameters
            
        Returns:
            Result data
        """
        # Get the underlying SDK client, ensuring it's initialized
        if not self._connection._client:
            # Initialize client lazily on first use
            self._connection._init_async_client()
        sdk_client = self._connection._client
        
        # Convert parameters to dict format
        params_dict = self._convert_parameters(parameters)
        
        # Get query translator and schema manager from connection
        from .query_translator import QueryTranslator
        from .schema_manager import SchemaManager
        
        schema_manager = SchemaManager(
            schema_id=self._connection._params.schema_id,
            project_id=self._connection._params.app_id
        )
        translator = QueryTranslator(schema_manager)
        
        # Parse and translate SQL to GolemBase operations
        operation = operation.strip()
        operation_upper = operation.upper()
        
        # DDL Operations (Data Definition Language)
        if operation_upper.startswith('CREATE TABLE'):
            return self._execute_create_table(operation)
        elif operation_upper.startswith('CREATE INDEX'):
            return self._execute_create_index(operation)
        elif operation_upper.startswith('DROP TABLE'):
            return self._execute_drop_table(operation)
        elif operation_upper.startswith('DROP INDEX'):
            return self._execute_drop_index(operation)
        
        # Schema Introspection Operations
        elif operation_upper.startswith('SHOW TABLES'):
            return self._execute_show_tables(operation)
        elif operation_upper.startswith('DESCRIBE') or operation_upper.startswith('DESC '):
            return self._execute_describe_table(operation)
        
        # Simple constant queries (for connection testing, etc.)
        elif self._is_simple_constant_query(operation):
            return self._execute_simple_constant_query(operation, params_dict)
        
        # DML Operations (Data Manipulation Language) 
        elif operation_upper.startswith('SELECT'):
            query_result = translator.translate_select(operation, params_dict)
            return self._execute_select(sdk_client, query_result)
        elif operation_upper.startswith('INSERT'):
            query_result = translator.translate_insert(operation, params_dict)
            return self._execute_insert(sdk_client, query_result)
        elif operation_upper.startswith('UPDATE'):
            query_result = translator.translate_update(operation, params_dict)
            return self._execute_update(sdk_client, query_result)
        elif operation_upper.startswith('DELETE'):
            query_result = translator.translate_delete(operation, params_dict)
            return self._execute_delete(sdk_client, query_result)
        else:
            raise ProgrammingError(f"Unsupported SQL operation: {operation}")
    
    def _execute_select(self, sdk_client, query_result):
        """Execute SELECT operation using GolemBase query_entities."""
        import logging
        logger = logging.getLogger(__name__)
        
        logger.debug(f"SELECT operation - GolemBase query: '{query_result.golem_query}'")
        
        # Use the golem_query to query entities
        entities = self._connection._run_async(
            sdk_client.query_entities(query_result.golem_query)
        )
        
        # Convert entities to table rows
        from .row_serializer import RowSerializer
        from .schema_manager import SchemaManager
        
        schema_manager = SchemaManager(
            schema_id=self._connection._params.schema_id,
            project_id=self._connection._params.app_id
        )
        serializer = RowSerializer(schema_manager)
        
        rows = []
        for entity in entities:
            # Deserialize entity back to row data
            row_data = serializer.deserialize_entity(entity.storage_value, query_result.table_name)
            
            # Apply post-filter conditions for non-indexed columns
            if query_result.post_filter_conditions:
                if not apply_post_filter(row_data, query_result.post_filter_conditions):
                    continue  # Skip this row if it doesn't match post-filter conditions
            
            # Extract only requested columns
            if query_result.columns:
                row_tuple = tuple(row_data.get(col) for col in query_result.columns)
            else:
                # SELECT * - return all columns
                table_def = schema_manager.get_table(query_result.table_name)
                if table_def:
                    column_names = [col.name for col in table_def.columns]
                    row_tuple = tuple(row_data.get(col) for col in column_names)
                else:
                    row_tuple = tuple(row_data.values())
            
            rows.append(row_tuple)
        
        return rows
    
    def _execute_insert(self, sdk_client, query_result):
        """Execute INSERT operation using GolemBase create_entities."""
        import logging
        logger = logging.getLogger(__name__)
        
        from .row_serializer import RowSerializer
        from .schema_manager import SchemaManager
        
        schema_manager = SchemaManager(
            schema_id=self._connection._params.schema_id,
            project_id=self._connection._params.app_id
        )
        serializer = RowSerializer(schema_manager)
        
        logger.debug(f"INSERT operation - Table: {query_result.table_name}")
        logger.debug(f"INSERT operation - Raw data: {query_result.insert_data}")
        
        # Serialize row data to entity format
        json_data, annotations = serializer.serialize_row(query_result.table_name, query_result.insert_data)
        
        logger.debug(f"INSERT operation - Serialized JSON data: {json_data}")
        logger.debug(f"INSERT operation - String annotations: {annotations['string_annotations']}")
        logger.debug(f"INSERT operation - Numeric annotations: {annotations['numeric_annotations']}")
        
        # Import GolemBase types
        from golem_base_sdk.types import GolemBaseCreate, Annotation
        
        # Convert annotations to proper Annotation objects
        string_annotations = [
            Annotation[str](key=key, value=value) 
            for key, value in annotations['string_annotations'].items()
        ]
        numeric_annotations = [
            Annotation[int](key=key, value=value)
            for key, value in annotations['numeric_annotations'].items() 
        ]
        
        # Create GolemBaseCreate object
        entity_create = GolemBaseCreate(
            data=json_data,
            btl=schema_manager.get_ttl_for_table(query_result.table_name),
            string_annotations=string_annotations,
            numeric_annotations=numeric_annotations
        )
        
        logger.debug(f"INSERT operation - GolemBaseCreate object: {entity_create}")
        logger.debug(f"INSERT operation - Calling sdk_client.create_entities([entity_create])")
        
        entity_ids = self._connection._run_async(
            sdk_client.create_entities([entity_create])
        )
        
        logger.debug(f"INSERT operation - Created entity IDs: {entity_ids}")
        
        return len(entity_ids)
    
    def _execute_update(self, sdk_client, query_result):
        """Execute UPDATE operation using GolemBase update_entities."""
        # First find entities to update
        entities = self._connection._run_async(
            sdk_client.query_entities(query_result.golem_query)
        )
        
        from .row_serializer import RowSerializer
        from .schema_manager import SchemaManager
        
        schema_manager = SchemaManager(
            schema_id=self._connection._params.schema_id,
            project_id=self._connection._params.app_id
        )
        serializer = RowSerializer(schema_manager)
        
        # Import GolemBase types
        from golem_base_sdk.types import GolemBaseUpdate, Annotation, EntityKey, GenericBytes
        
        updated_entities = []
        for entity in entities:
            # Deserialize current data
            row_data = serializer.deserialize_entity(entity.storage_value, query_result.table_name)
            
            # Apply updates
            row_data.update(query_result.update_data)
            
            # Serialize back to entity format
            json_data, annotations = serializer.serialize_row(query_result.table_name, row_data)
            
            # Convert annotations to proper Annotation objects
            string_annotations = [
                Annotation[str](key=key, value=value) 
                for key, value in annotations['string_annotations'].items()
            ]
            numeric_annotations = [
                Annotation[int](key=key, value=value)
                for key, value in annotations['numeric_annotations'].items() 
            ]
            
            # Convert entity key to proper GenericBytes format
            if isinstance(entity.entity_key, str) and entity.entity_key.startswith('0x'):
                entity_key_bytes = bytes.fromhex(entity.entity_key[2:])
                entity_key_obj = EntityKey(GenericBytes(entity_key_bytes))
            else:
                # If it's already in the right format, use as-is
                entity_key_obj = entity.entity_key
            
            # Ensure data is bytes
            if isinstance(json_data, str):
                json_data_bytes = json_data.encode('utf-8')
            else:
                json_data_bytes = json_data
            
            # Create GolemBaseUpdate object
            entity_update = GolemBaseUpdate(
                entity_key=entity_key_obj,
                data=json_data_bytes,
                btl=schema_manager.get_ttl_for_table(query_result.table_name),
                string_annotations=string_annotations,
                numeric_annotations=numeric_annotations
            )
            
            updated_entities.append(entity_update)
        
        if updated_entities:
            self._connection._run_async(
                sdk_client.update_entities(updated_entities)
            )
        
        return len(updated_entities)
    
    def _execute_delete(self, sdk_client, query_result):
        """Execute DELETE operation using GolemBase delete_entities."""
        # Import GolemBase types
        from golem_base_sdk.types import GolemBaseDelete, GenericBytes, EntityKey
        
        # First find entities to delete
        entities = self._connection._run_async(
            sdk_client.query_entities(query_result.golem_query)
        )
        
        # Create GolemBaseDelete objects
        delete_objects = []
        for entity in entities:
            # Convert entity key to proper GenericBytes format
            if isinstance(entity.entity_key, str) and entity.entity_key.startswith('0x'):
                entity_key_bytes = bytes.fromhex(entity.entity_key[2:])
                entity_key_obj = EntityKey(GenericBytes(entity_key_bytes))
            else:
                # If it's already in the right format, use as-is
                entity_key_obj = entity.entity_key
            
            # Create GolemBaseDelete object
            delete_obj = GolemBaseDelete(entity_key=entity_key_obj)
            delete_objects.append(delete_obj)
        
        if delete_objects:
            self._connection._run_async(
                sdk_client.delete_entities(delete_objects)
            )
        
        return len(delete_objects)
    
    def _execute_create_table(self, operation: str) -> dict:
        """Execute CREATE TABLE DDL operation.
        
        Args:
            operation: CREATE TABLE SQL statement
            
        Returns:
            Result dictionary with rowcount=0 for DDL operations
        """
        try:
            # Get schema manager from connection
            schema_manager = self._get_schema_manager()
            
            # Parse SQL and create table definition
            table_def = schema_manager.create_table_from_sql(operation)
            
            # Check if table already exists
            if schema_manager.table_exists(table_def.name):
                raise ProgrammingError(f"Table '{table_def.name}' already exists")
            
            # Add to schema (automatically saves to TOML)
            schema_manager.add_table(table_def)
            
            # Return success result (DDL operations have rowcount=0)
            return {'rowcount': 0, 'description': None, 'rows': []}
            
        except Exception as e:
            if isinstance(e, ProgrammingError):
                raise
            else:
                raise ProgrammingError(f"Error creating table: {e}")
    
    def _execute_create_index(self, operation: str) -> dict:
        """Execute CREATE INDEX DDL operation.
        
        Args:
            operation: CREATE INDEX SQL statement
            
        Returns:
            Result dictionary with rowcount=0 for DDL operations
        """
        try:
            import sqlglot
            from sqlglot import expressions as exp
            from .schema_manager import IndexDefinition
            
            # Parse CREATE INDEX statement
            parsed = sqlglot.parse_one(operation, read="sqlite")
            
            if not isinstance(parsed, exp.Create):
                raise ValueError("Not a CREATE INDEX statement")
                
            # Extract index information from parsed.this (Index object)
            index_obj = parsed.this
            if not hasattr(index_obj, 'args') or 'this' not in index_obj.args or 'table' not in index_obj.args:
                raise ValueError("Invalid CREATE INDEX statement format")
                
            # Get index name from args
            index_name = index_obj.args['this'].name if hasattr(index_obj.args['this'], 'name') else str(index_obj.args['this'])
            
            # Get table name from args
            table_obj = index_obj.args['table']
            table_name = table_obj.this.name if hasattr(table_obj.this, 'name') else str(table_obj.this)
            
            # Get columns from args
            columns = []
            if 'columns' in index_obj.args and index_obj.args['columns']:
                for col_expr in index_obj.args['columns']:
                    # Handle Ordered columns (Column wrapped in Ordered)
                    if hasattr(col_expr, 'this') and hasattr(col_expr.this, 'this'):
                        col_name = col_expr.this.this.name if hasattr(col_expr.this.this, 'name') else str(col_expr.this.this)
                    else:
                        col_name = str(col_expr)
                    columns.append(col_name)
            
            # Check for unique constraint
            unique = bool(index_obj.args.get('unique', False))
                
            if not index_name or not table_name or not columns:
                raise ValueError("Invalid CREATE INDEX statement format")
            
            # Get schema manager and check if table exists
            schema_manager = self._get_schema_manager()
            table_def = schema_manager.get_table(table_name)
            
            if not table_def:
                raise ProgrammingError(f"Table '{table_name}' does not exist")
            
            # Check if index already exists
            existing_index_names = [idx.name for idx in table_def.indexes]
            if index_name in existing_index_names:
                raise ProgrammingError(f"Index '{index_name}' already exists")
            
            # Create new index definition
            new_index = IndexDefinition(
                name=index_name,
                columns=columns,
                unique=unique
            )
            
            # Add index to table definition
            table_def.indexes.append(new_index)
            
            # Mark columns as indexed
            for col_name in columns:
                col_def = table_def.get_column(col_name)
                if col_def:
                    col_def.indexed = True
            
            # Save updated table definition
            schema_manager.add_table(table_def)
            
            return {'rowcount': 0, 'description': None, 'rows': []}
            
        except Exception as e:
            if isinstance(e, ProgrammingError):
                raise
            else:
                raise ProgrammingError(f"Error creating index: {e}")
    
    def _execute_drop_table(self, operation: str) -> dict:
        """Execute DROP TABLE DDL operation.
        
        Args:
            operation: DROP TABLE SQL statement
            
        Returns:
            Result dictionary with rowcount=0 for DDL operations
        """
        try:
            import sqlglot
            from sqlglot import expressions as exp
            
            # Parse DROP TABLE statement
            parsed = sqlglot.parse_one(operation, read="sqlite")
            
            if not isinstance(parsed, exp.Drop) or not hasattr(parsed, 'this'):
                raise ValueError("Not a DROP TABLE statement")
            
            # Extract table name
            table_name = parsed.this.name if hasattr(parsed.this, 'name') else str(parsed.this)
            
            # Get schema manager and check if table exists
            schema_manager = self._get_schema_manager()
            
            if not schema_manager.table_exists(table_name):
                # Handle IF EXISTS clause
                if 'IF EXISTS' in operation.upper():
                    return {'rowcount': 0, 'description': None, 'rows': []}
                else:
                    raise ProgrammingError(f"Table '{table_name}' does not exist")
            
            # Remove table from schema
            schema_manager.remove_table(table_name)
            
            return {'rowcount': 0, 'description': None, 'rows': []}
            
        except Exception as e:
            if isinstance(e, ProgrammingError):
                raise
            else:
                raise ProgrammingError(f"Error dropping table: {e}")
    
    def _execute_drop_index(self, operation: str) -> dict:
        """Execute DROP INDEX DDL operation.
        
        Args:
            operation: DROP INDEX SQL statement
            
        Returns:
            Result dictionary with rowcount=0 for DDL operations
        """
        try:
            import sqlglot
            from sqlglot import expressions as exp
            
            # Parse DROP INDEX statement  
            parsed = sqlglot.parse_one(operation, read="sqlite")
            
            if not isinstance(parsed, exp.Drop):
                raise ValueError("Not a DROP INDEX statement")
            
            # Extract index name
            index_name = parsed.this.name if hasattr(parsed.this, 'name') else str(parsed.this)
            
            # Get schema manager
            schema_manager = self._get_schema_manager()
            
            # Find which table contains this index
            target_table = None
            target_index = None
            
            for table_name, table_def in schema_manager.tables.items():
                for idx in table_def.indexes:
                    if idx.name == index_name:
                        target_table = table_def
                        target_index = idx
                        break
                if target_table:
                    break
            
            if not target_table:
                # Handle IF EXISTS clause
                if 'IF EXISTS' in operation.upper():
                    return {'rowcount': 0, 'description': None, 'rows': []}
                else:
                    raise ProgrammingError(f"Index '{index_name}' does not exist")
            
            # Remove index from table definition
            target_table.indexes.remove(target_index)
            
            # Update column indexed flags if no other indexes reference them
            for col_name in target_index.columns:
                col_def = target_table.get_column(col_name)
                if col_def and not col_def.primary_key and not col_def.unique:
                    # Check if any other indexes reference this column
                    still_indexed = any(
                        col_name in idx.columns 
                        for idx in target_table.indexes 
                        if idx != target_index
                    )
                    if not still_indexed:
                        col_def.indexed = False
            
            # Save updated table definition
            schema_manager.add_table(target_table)
            
            return {'rowcount': 0, 'description': None, 'rows': []}
            
        except Exception as e:
            if isinstance(e, ProgrammingError):
                raise
            else:
                raise ProgrammingError(f"Error dropping index: {e}")
    
    def _execute_show_tables(self, operation: str) -> dict:
        """Execute SHOW TABLES introspection command.
        
        Args:
            operation: SHOW TABLES SQL statement
            
        Returns:
            Result dictionary with table names
        """
        try:
            # Get schema manager from connection
            schema_manager = self._get_schema_manager()
            
            # Get all table names from schema
            table_names = schema_manager.get_table_names()
            
            # Format as rows for result
            rows = [(table_name,) for table_name in sorted(table_names)]
            
            # Create description for single column result
            description = [('Table', 'STRING', None, None, None, None, True)]
            
            return {
                'rowcount': len(rows),
                'description': description,
                'rows': rows
            }
            
        except Exception as e:
            raise ProgrammingError(f"Error executing SHOW TABLES: {e}")
    
    def _execute_describe_table(self, operation: str) -> dict:
        """Execute DESCRIBE table introspection command.
        
        Args:
            operation: DESCRIBE [schema.]table SQL statement
            
        Returns:
            Result dictionary with column information
        """
        try:
            import re
            
            # Parse table name from DESCRIBE statement
            # Handle both DESCRIBE table and DESC table formats
            match = re.match(r'(?:DESCRIBE|DESC)\s+([^\s;]+)', operation.strip(), re.IGNORECASE)
            if not match:
                raise ValueError("Invalid DESCRIBE statement format")
            
            table_name = match.group(1).strip()
            
            # Remove quotes if present
            if table_name.startswith('"') and table_name.endswith('"'):
                table_name = table_name[1:-1]
            elif table_name.startswith("'") and table_name.endswith("'"):
                table_name = table_name[1:-1]
            
            # Get schema manager from connection
            schema_manager = self._get_schema_manager()
            
            # Get table definition
            table_def = schema_manager.get_table(table_name)
            if not table_def:
                raise ProgrammingError(f"Table '{table_name}' does not exist")
            
            # Format columns as rows (Field, Type, Null, Key, Default, Extra)
            rows = []
            for column in table_def.columns:
                # Determine column type string
                type_str = column.type
                if column.length and f"({column.length})" not in type_str:
                    type_str += f"({column.length})"
                elif column.precision and column.scale and f"({column.precision},{column.scale})" not in type_str:
                    type_str += f"({column.precision},{column.scale})"
                elif column.precision and f"({column.precision})" not in type_str:
                    type_str += f"({column.precision})"
                
                # Determine nullable
                nullable = "YES" if column.nullable else "NO"
                
                # Determine key type
                key = ""
                if column.primary_key:
                    key = "PRI"
                elif column.unique:
                    key = "UNI"
                elif column.indexed:
                    key = "MUL"
                
                # Get default value
                default = column.default
                
                # Extra information
                extra = ""
                if column.primary_key and column.type.upper() == 'INTEGER':
                    extra = "auto_increment"
                
                rows.append((
                    column.name,      # Field
                    type_str,         # Type
                    nullable,         # Null
                    key,             # Key
                    default,         # Default
                    extra            # Extra
                ))
            
            # Create description for DESCRIBE result columns
            description = [
                ('Field', 'STRING', None, None, None, None, False),
                ('Type', 'STRING', None, None, None, None, False),
                ('Null', 'STRING', None, None, None, None, False),
                ('Key', 'STRING', None, None, None, None, True),
                ('Default', 'STRING', None, None, None, None, True),
                ('Extra', 'STRING', None, None, None, None, True)
            ]
            
            return {
                'rowcount': len(rows),
                'description': description,
                'rows': rows
            }
            
        except Exception as e:
            if isinstance(e, ProgrammingError):
                raise
            else:
                raise ProgrammingError(f"Error executing DESCRIBE: {e}")
    
    def _is_simple_constant_query(self, operation: str) -> bool:
        """Check if the query is a simple constant query like SELECT 1."""
        import re
        
        # Clean up the query
        operation = operation.strip().upper()
        
        # Pattern to match simple constant SELECT queries:
        # SELECT constant [AS alias] [FROM DUAL] [WHERE condition]
        patterns = [
            r'^SELECT\s+\d+\s*$',  # SELECT 1
            r'^SELECT\s+\d+\s+AS\s+\w+\s*$',  # SELECT 1 AS test
            r"^SELECT\s+'[^']*'\s*$",  # SELECT 'test'
            r"^SELECT\s+'[^']*'\s+AS\s+\w+\s*$",  # SELECT 'test' AS value
            r'^SELECT\s+\d+\s+FROM\s+DUAL\s*$',  # SELECT 1 FROM DUAL (Oracle style)
            r'^SELECT\s+NULL\s*$',  # SELECT NULL
            r'^SELECT\s+TRUE\s*$',  # SELECT TRUE
            r'^SELECT\s+FALSE\s*$',  # SELECT FALSE
            r'^SELECT\s+CURRENT_TIMESTAMP\s*$',  # SELECT CURRENT_TIMESTAMP
            r'^SELECT\s+NOW\(\)\s*$',  # SELECT NOW()
        ]
        
        return any(re.match(pattern, operation) for pattern in patterns)
    
    def _execute_simple_constant_query(self, operation: str, parameters: Dict[str, Any]) -> dict:
        """Execute simple constant queries without involving GolemBase entities."""
        import re
        from datetime import datetime
        
        operation_upper = operation.strip().upper()
        
        try:
            # Parse the constant value from the query
            if 'SELECT 1' in operation_upper:
                rows = [(1,)]
                description = [('1', 'INTEGER', None, None, None, None, False)]
            elif re.search(r'SELECT\s+(\d+)', operation_upper):
                # Extract the number
                match = re.search(r'SELECT\s+(\d+)', operation_upper)
                value = int(match.group(1))
                rows = [(value,)]
                description = [(str(value), 'INTEGER', None, None, None, None, False)]
            elif re.search(r"SELECT\s+'([^']*)'", operation_upper):
                # Extract the string
                match = re.search(r"SELECT\s+'([^']*)'", operation_upper)
                value = match.group(1)
                rows = [(value,)]
                description = [(f"'{value}'", 'STRING', None, None, None, None, False)]
            elif 'SELECT NULL' in operation_upper:
                rows = [(None,)]
                description = [('NULL', 'NULL', None, None, None, None, True)]
            elif 'SELECT TRUE' in operation_upper:
                rows = [(True,)]
                description = [('TRUE', 'BOOLEAN', None, None, None, None, False)]
            elif 'SELECT FALSE' in operation_upper:
                rows = [(False,)]
                description = [('FALSE', 'BOOLEAN', None, None, None, None, False)]
            elif 'CURRENT_TIMESTAMP' in operation_upper or 'NOW()' in operation_upper:
                current_time = datetime.now()
                rows = [(current_time,)]
                description = [('CURRENT_TIMESTAMP', 'DATETIME', None, None, None, None, False)]
            else:
                # Generic fallback
                rows = [(1,)]
                description = [('result', 'INTEGER', None, None, None, None, False)]
            
            return {
                'rowcount': len(rows),
                'description': description,
                'rows': rows
            }
            
        except Exception as e:
            raise ProgrammingError(f"Error executing simple constant query: {e}")
    
    def _convert_parameters(self, parameters: Optional[Union[Dict[str, Any], Sequence[Any]]]) -> Any:
        """Convert parameters to format expected by golem-base-sdk.
        
        Args:
            parameters: DB-API parameters
            
        Returns:
            Parameters in SDK format
        """
        if parameters is None:
            return {}
            
        if isinstance(parameters, dict):
            # Named parameters - most databases support this directly
            return parameters
        elif isinstance(parameters, (list, tuple)):
            # Positional parameters - may need conversion depending on SDK
            # For now, assume SDK accepts positional parameters
            return parameters
        else:
            return parameters
    
    def _process_result(self, result: Any) -> None:
        """Process result from golem-base-sdk execution.
        
        Args:
            result: Result object from SDK
        """
        # Reset state
        self._results.clear()
        self._description = None
        self._rowcount = -1
        self._rownumber = None
        
        try:
            # Handle different result types based on SDK response format
            if hasattr(result, 'fetchall'):
                # SDK cursor-like object
                rows = result.fetchall()
                self._results = [tuple(row) if not isinstance(row, tuple) else row for row in rows]
                self._rowcount = len(self._results)
                
                # Get column descriptions if available
                if hasattr(result, 'description'):
                    self._description = self._convert_description(result.description)
                    
            elif isinstance(result, dict) and 'rows' in result:
                # Our introspection command result format
                rows = result.get('rows', [])
                self._results = [tuple(row) if not isinstance(row, tuple) else row for row in rows]
                self._rowcount = result.get('rowcount', len(self._results))
                
                # Get description if available
                if 'description' in result:
                    self._description = result['description']
                    
            elif hasattr(result, 'rows') or hasattr(result, 'data'):
                # SDK result object with rows/data attribute
                rows = getattr(result, 'rows', getattr(result, 'data', []))
                self._results = [tuple(row) if not isinstance(row, tuple) else row for row in rows]
                self._rowcount = len(self._results)
                
                # Get column info if available
                if hasattr(result, 'columns'):
                    self._description = self._build_description_from_columns(result.columns)
                elif hasattr(result, 'fields'):
                    self._description = self._build_description_from_columns(result.fields)
                    
            elif isinstance(result, (list, tuple)):
                # Direct list of rows
                self._results = [tuple(row) if not isinstance(row, tuple) else row for row in result]
                self._rowcount = len(self._results)
                
            elif hasattr(result, 'rowcount'):
                # Non-SELECT query result (INSERT, UPDATE, DELETE)
                self._rowcount = result.rowcount
                
            else:
                # Default handling - assume it's iterable
                try:
                    rows = list(result)
                    self._results = [tuple(row) if not isinstance(row, tuple) else row for row in rows]
                    self._rowcount = len(self._results)
                except (TypeError, ValueError):
                    # Not iterable, assume it's a command result
                    self._rowcount = 0
                    
        except Exception as e:
            raise DatabaseError(f"Error processing result: {e}")
    
    def _convert_description(self, sdk_description: Any) -> Sequence[Sequence[Any]]:
        """Convert SDK column description to DB-API format.
        
        Args:
            sdk_description: Column description from SDK
            
        Returns:
            DB-API compatible description
        """
        if not sdk_description:
            return None
            
        description = []
        for col in sdk_description:
            if isinstance(col, (list, tuple)) and len(col) >= 2:
                # Already in compatible format
                description.append(col)
            elif hasattr(col, 'name'):
                # Column object with name attribute
                name = col.name
                type_code = getattr(col, 'type', getattr(col, 'type_code', None))
                # Build 7-item sequence: (name, type_code, display_size, internal_size, precision, scale, null_ok)
                description.append((
                    name,
                    type_code,
                    getattr(col, 'display_size', None),
                    getattr(col, 'internal_size', None), 
                    getattr(col, 'precision', None),
                    getattr(col, 'scale', None),
                    getattr(col, 'null_ok', None)
                ))
            else:
                # Simple name or unknown format
                description.append((str(col), None, None, None, None, None, None))
                
        return description
    
    def _build_description_from_columns(self, columns: Any) -> Sequence[Sequence[Any]]:
        """Build DB-API description from column information.
        
        Args:
            columns: Column information from SDK
            
        Returns:
            DB-API compatible description
        """
        if not columns:
            return None
            
        description = []
        for col in columns:
            if isinstance(col, str):
                # Just column name
                description.append((col, None, None, None, None, None, None))
            elif isinstance(col, dict):
                # Column info as dictionary
                name = col.get('name', str(col))
                type_code = col.get('type', col.get('type_code'))
                description.append((
                    name,
                    type_code,
                    col.get('display_size'),
                    col.get('internal_size'),
                    col.get('precision'),
                    col.get('scale'),
                    col.get('null_ok')
                ))
            else:
                description.append((str(col), None, None, None, None, None, None))
                
        return description
    
    def _update_rownumber(self) -> None:
        """Update the current row number based on remaining results."""
        if self._rowcount >= 0:
            remaining = len(self._results)
            if remaining < self._rowcount:
                self._rownumber = self._rowcount - remaining - 1
            else:
                self._rownumber = None
    
    def _get_schema_manager(self):
        """Get schema manager instance from connection parameters.
        
        Returns:
            SchemaManager instance for DDL operations
        """
        from .schema_manager import SchemaManager
        
        return SchemaManager(
            schema_id=self._connection._params.schema_id,
            project_id=self._connection._params.app_id
        )
    
    def __iter__(self) -> 'Cursor':
        """Make cursor iterable."""
        return self
    
    def __next__(self) -> Tuple[Any, ...]:
        """Iterator protocol implementation."""
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row