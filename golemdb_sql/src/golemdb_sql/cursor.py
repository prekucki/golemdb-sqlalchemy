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
        # Get the underlying SDK client
        sdk_client = self._connection._client
        
        # Convert parameters to dict format
        params_dict = self._convert_parameters(parameters)
        
        # Get query translator and schema manager from connection
        from .query_translator import QueryTranslator
        from .schema_manager import SchemaManager
        
        schema_manager = SchemaManager(self._connection._params.schema_id)
        translator = QueryTranslator(schema_manager)
        
        # Parse and translate SQL to GolemBase operations
        operation = operation.strip()
        if operation.upper().startswith('SELECT'):
            query_result = translator.translate_select(operation, params_dict)
            return self._execute_select(sdk_client, query_result)
        elif operation.upper().startswith('INSERT'):
            query_result = translator.translate_insert(operation, params_dict)
            return self._execute_insert(sdk_client, query_result)
        elif operation.upper().startswith('UPDATE'):
            query_result = translator.translate_update(operation, params_dict)
            return self._execute_update(sdk_client, query_result)
        elif operation.upper().startswith('DELETE'):
            query_result = translator.translate_delete(operation, params_dict)
            return self._execute_delete(sdk_client, query_result)
        else:
            raise ProgrammingError(f"Unsupported SQL operation: {operation}")
    
    def _execute_select(self, sdk_client, query_result):
        """Execute SELECT operation using GolemBase query_entities."""
        # Use the golem_query to query entities
        entities = self._connection._run_async(
            sdk_client.query_entities(query_result.golem_query)
        )
        
        # Convert entities to table rows
        from .row_serializer import RowSerializer
        from .schema_manager import SchemaManager
        
        schema_manager = SchemaManager(self._connection._params.schema_id)
        serializer = RowSerializer(schema_manager)
        
        rows = []
        for entity in entities:
            # Deserialize entity back to row data
            row_data = serializer.deserialize_entity(entity.data, query_result.table_name)
            
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
        from .row_serializer import RowSerializer
        from .schema_manager import SchemaManager
        
        schema_manager = SchemaManager(self._connection._params.schema_id)
        serializer = RowSerializer(schema_manager)
        
        # Serialize row data to entity format
        json_data, annotations = serializer.serialize_row(query_result.table_name, query_result.insert_data)
        
        # Create entity
        entity_data = {
            'data': json_data,
            'string_annotations': annotations['string_annotations'],
            'numeric_annotations': annotations['numeric_annotations'],
            'ttl': schema_manager.get_ttl_for_table(query_result.table_name)
        }
        
        entity_ids = self._connection._run_async(
            sdk_client.create_entities([entity_data])
        )
        
        return len(entity_ids)
    
    def _execute_update(self, sdk_client, query_result):
        """Execute UPDATE operation using GolemBase update_entities."""
        # First find entities to update
        entities = self._connection._run_async(
            sdk_client.query_entities(query_result.golem_query)
        )
        
        from .row_serializer import RowSerializer
        from .schema_manager import SchemaManager
        
        schema_manager = SchemaManager(self._connection._params.schema_id)
        serializer = RowSerializer(schema_manager)
        
        updated_entities = []
        for entity in entities:
            # Deserialize current data
            row_data = serializer.deserialize_entity(entity.data, query_result.table_name)
            
            # Apply updates
            row_data.update(query_result.update_data)
            
            # Serialize back to entity format
            json_data, annotations = serializer.serialize_row(query_result.table_name, row_data)
            
            updated_entities.append({
                'id': entity.id,
                'data': json_data,
                'string_annotations': annotations['string_annotations'],
                'numeric_annotations': annotations['numeric_annotations']
            })
        
        if updated_entities:
            self._connection._run_async(
                sdk_client.update_entities(updated_entities)
            )
        
        return len(updated_entities)
    
    def _execute_delete(self, sdk_client, query_result):
        """Execute DELETE operation using GolemBase delete_entities."""
        # First find entities to delete
        entities = self._connection._run_async(
            sdk_client.query_entities(query_result.golem_query)
        )
        
        entity_ids = [entity.id for entity in entities]
        
        if entity_ids:
            self._connection._run_async(
                sdk_client.delete_entities(entity_ids)
            )
        
        return len(entity_ids)
    
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
    
    def __iter__(self) -> 'Cursor':
        """Make cursor iterable."""
        return self
    
    def __next__(self) -> Tuple[Any, ...]:
        """Iterator protocol implementation."""
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row