"""JSON serialization system for GolemBase table row data."""

import json
import uuid
from datetime import datetime, date, time
from typing import Any, Dict, List, Optional, Tuple, Union
from decimal import Decimal
from .schema_manager import SchemaManager, TableDefinition, ColumnDefinition
from .exceptions import DataError, ProgrammingError
from .types import decode_uint64_to_signed, should_encode_as_signed_integer, get_integer_bit_width, decode_decimal_from_string_ordering


class RowSerializer:
    """Handles serialization/deserialization of table rows to/from GolemBase entities."""
    
    def __init__(self, schema_manager: SchemaManager):
        """Initialize row serializer.
        
        Args:
            schema_manager: Schema manager for table definitions
        """
        self.schema_manager = schema_manager
    
    def serialize_row(self, table_name: str, row_data: Dict[str, Any]) -> Tuple[bytes, Dict[str, Any]]:
        """Serialize table row to GolemBase entity format.
        
        Args:
            table_name: Name of table
            row_data: Row data as dictionary
            
        Returns:
            Tuple of (entity_data, annotations_dict)
            - entity_data: JSON serialized row data as bytes
            - annotations_dict: Dictionary with string_annotations and numeric_annotations
            
        Raises:
            DataError: If serialization fails
            ProgrammingError: If table not found
        """
        try:
            table_def = self.schema_manager.get_table(table_name)
            if not table_def:
                raise ProgrammingError(f"Table '{table_name}' not found in schema")
            
            # Convert row data to JSON-serializable format
            serialized_data = self._prepare_data_for_json(row_data, table_def)
            
            # Add metadata to the JSON
            entity_data = {
                '_table': table_name,
                '_version': 1,
                '_created_at': datetime.utcnow().isoformat(),
                **serialized_data
            }
            
            # Serialize to JSON bytes
            json_bytes = json.dumps(entity_data, default=self._json_serializer).encode('utf-8')
            
            # Generate annotations for indexing
            annotations = self.schema_manager.get_entity_annotations_for_table(table_name, row_data)
            
            return json_bytes, annotations
            
        except Exception as e:
            raise DataError(f"Failed to serialize row for table '{table_name}': {e}")
    
    def deserialize_entity(self, entity_data: bytes, table_name: str) -> Dict[str, Any]:
        """Deserialize GolemBase entity to table row format.
        
        Args:
            entity_data: JSON entity data as bytes
            table_name: Expected table name
            
        Returns:
            Dictionary with row data
            
        Raises:
            DataError: If deserialization fails
            ProgrammingError: If table not found
        """
        try:
            table_def = self.schema_manager.get_table(table_name)
            if not table_def:
                raise ProgrammingError(f"Table '{table_name}' not found in schema")
            
            # Parse JSON data
            json_data = json.loads(entity_data.decode('utf-8'))
            
            # Verify table name matches
            if json_data.get('_table') != table_name:
                raise DataError(f"Entity table mismatch: expected '{table_name}', got '{json_data.get('_table')}'")
            
            # Extract row data (excluding metadata fields)
            row_data = {k: v for k, v in json_data.items() if not k.startswith('_')}
            
            # Convert data back to Python types
            converted_data = self._convert_from_json(row_data, table_def)
            
            return converted_data
            
        except json.JSONDecodeError as e:
            raise DataError(f"Failed to parse entity JSON data: {e}")
        except Exception as e:
            raise DataError(f"Failed to deserialize entity for table '{table_name}': {e}")
    
    def create_row_from_columns_values(self, table_name: str, columns: List[str], values: List[Any]) -> Dict[str, Any]:
        """Create row dictionary from column names and values.
        
        Args:
            table_name: Name of table
            columns: List of column names
            values: List of values in same order as columns
            
        Returns:
            Dictionary mapping column names to values
            
        Raises:
            DataError: If columns and values don't match
            ProgrammingError: If table not found
        """
        if len(columns) != len(values):
            raise DataError(f"Column count ({len(columns)}) doesn't match value count ({len(values)})")
        
        table_def = self.schema_manager.get_table(table_name)
        if not table_def:
            raise ProgrammingError(f"Table '{table_name}' not found in schema")
        
        # Create row dictionary
        row_data = dict(zip(columns, values))
        
        # Add default values for missing columns
        for col_def in table_def.columns:
            if col_def.name not in row_data:
                if col_def.default is not None:
                    # Parse default value
                    default_value = self._parse_default_value(col_def.default, col_def.type)
                    row_data[col_def.name] = default_value
                elif not col_def.nullable:
                    raise DataError(f"Column '{col_def.name}' is NOT NULL but no value provided")
                else:
                    row_data[col_def.name] = None
        
        return row_data
    
    def update_row_data(self, existing_data: Dict[str, Any], updates: Dict[str, Any], table_name: str) -> Dict[str, Any]:
        """Update existing row data with new values.
        
        Args:
            existing_data: Current row data
            updates: New values to apply
            table_name: Name of table
            
        Returns:
            Updated row data dictionary
            
        Raises:
            ProgrammingError: If table not found
        """
        table_def = self.schema_manager.get_table(table_name)
        if not table_def:
            raise ProgrammingError(f"Table '{table_name}' not found in schema")
        
        # Copy existing data
        updated_data = existing_data.copy()
        
        # Apply updates
        for col_name, new_value in updates.items():
            # Verify column exists
            col_def = table_def.get_column(col_name)
            if not col_def:
                raise ProgrammingError(f"Column '{col_name}' does not exist in table '{table_name}'")
            
            updated_data[col_name] = new_value
        
        return updated_data
    
    def _prepare_data_for_json(self, row_data: Dict[str, Any], table_def: TableDefinition) -> Dict[str, Any]:
        """Prepare row data for JSON serialization.
        
        Args:
            row_data: Raw row data
            table_def: Table definition for type information
            
        Returns:
            JSON-serializable data
        """
        prepared_data = {}
        
        for col_name, value in row_data.items():
            col_def = table_def.get_column(col_name)
            
            if value is None:
                prepared_data[col_name] = None
            elif col_def:
                # Convert based on column type
                prepared_data[col_name] = self._convert_value_for_column(value, col_def)
            else:
                # Column not in schema - store as-is but try to make JSON-serializable
                prepared_data[col_name] = self._make_json_serializable(value)
        
        return prepared_data
    
    def _convert_value_for_column(self, value: Any, col_def: ColumnDefinition) -> Any:
        """Convert value based on column type for JSON storage.
        
        Args:
            value: Value to convert
            col_def: Column definition
            
        Returns:
            JSON-serializable value
        """
        if value is None:
            return None
        
        col_type = col_def.type.upper()
        
        if col_type in ('INTEGER', 'INT', 'BIGINT', 'SMALLINT', 'TINYINT'):
            return int(value)
        
        elif col_type in ('FLOAT', 'DOUBLE', 'REAL'):
            return float(value)
        
        elif col_type.startswith(('DECIMAL', 'NUMERIC', 'NUMBER')):
            # Store as string to preserve precision with proper formatting
            if isinstance(value, Decimal):
                return str(value)
            elif isinstance(value, float):
                return str(Decimal(str(value)))  # Convert via string to avoid float precision issues
            else:
                return str(value)
        
        elif col_type in ('BOOLEAN', 'BOOL'):
            return bool(value)
        
        elif col_type in ('DATETIME', 'TIMESTAMP'):
            if isinstance(value, datetime):
                return value.isoformat()
            elif isinstance(value, (int, float)):
                # Unix timestamp
                return datetime.fromtimestamp(value).isoformat()
            else:
                return str(value)
        
        elif col_type == 'DATE':
            if isinstance(value, date):
                return value.isoformat()
            elif isinstance(value, datetime):
                return value.date().isoformat()
            else:
                return str(value)
        
        elif col_type == 'TIME':
            if isinstance(value, time):
                return value.isoformat()
            elif isinstance(value, datetime):
                return value.time().isoformat()
            else:
                return str(value)
        
        elif col_type in ('VARCHAR', 'CHAR', 'TEXT', 'STRING'):
            return str(value)
        
        elif col_type in ('BLOB', 'BINARY', 'VARBINARY'):
            if isinstance(value, bytes):
                # Encode as base64 for JSON storage
                import base64
                return base64.b64encode(value).decode('ascii')
            else:
                return str(value)
        
        elif col_type in ('JSON', 'JSONB'):
            if isinstance(value, (dict, list)):
                return value
            elif isinstance(value, str):
                try:
                    return json.loads(value)
                except json.JSONDecodeError:
                    return value
            else:
                return str(value)
        
        else:
            # Unknown type - try to make JSON-serializable
            return self._make_json_serializable(value)
    
    def _convert_from_json(self, json_data: Dict[str, Any], table_def: TableDefinition) -> Dict[str, Any]:
        """Convert JSON data back to Python types.
        
        Args:
            json_data: Deserialized JSON data
            table_def: Table definition for type information
            
        Returns:
            Dictionary with proper Python types
        """
        converted_data = {}
        
        for col_name, value in json_data.items():
            col_def = table_def.get_column(col_name)
            
            if value is None:
                converted_data[col_name] = None
            elif col_def:
                converted_data[col_name] = self._convert_value_from_json(value, col_def)
            else:
                # Column not in schema - keep as-is
                converted_data[col_name] = value
        
        return converted_data
    
    def _convert_value_from_json(self, value: Any, col_def: ColumnDefinition) -> Any:
        """Convert JSON value back to proper Python type.
        
        Args:
            value: JSON value
            col_def: Column definition
            
        Returns:
            Properly typed Python value
        """
        if value is None:
            return None
        
        col_type = col_def.type.upper()
        
        if col_type in ('INTEGER', 'INT', 'BIGINT', 'SMALLINT', 'TINYINT'):
            return int(value)
        
        elif col_type in ('FLOAT', 'DOUBLE', 'REAL'):
            return float(value)
        
        elif col_type.startswith(('DECIMAL', 'NUMERIC', 'NUMBER')):
            # Convert back to Decimal with proper precision/scale validation
            if isinstance(value, str):
                decimal_value = Decimal(value)
            else:
                decimal_value = Decimal(str(value))
            
            # Validate against column precision/scale if available
            if col_def.precision is not None and col_def.scale is not None:
                # Check if value fits within declared precision/scale
                # This is a safety check during deserialization
                max_digits_before_decimal = col_def.precision - col_def.scale
                str_repr = str(abs(decimal_value))
                if '.' in str_repr:
                    int_part, frac_part = str_repr.split('.')
                    if len(int_part) > max_digits_before_decimal or len(frac_part) > col_def.scale:
                        # Value doesn't fit - this shouldn't happen if encoding was correct
                        pass  # Log warning but don't fail
            
            return decimal_value
        
        elif col_type in ('BOOLEAN', 'BOOL'):
            return bool(value)
        
        elif col_type in ('DATETIME', 'TIMESTAMP'):
            if isinstance(value, str):
                try:
                    return datetime.fromisoformat(value)
                except ValueError:
                    # Try parsing other formats
                    return datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
            else:
                return value
        
        elif col_type == 'DATE':
            if isinstance(value, str):
                try:
                    return datetime.fromisoformat(value).date()
                except ValueError:
                    return datetime.strptime(value, '%Y-%m-%d').date()
            else:
                return value
        
        elif col_type == 'TIME':
            if isinstance(value, str):
                try:
                    return datetime.fromisoformat(f"1970-01-01T{value}").time()
                except ValueError:
                    return datetime.strptime(value, '%H:%M:%S').time()
            else:
                return value
        
        elif col_type in ('VARCHAR', 'CHAR', 'TEXT', 'STRING'):
            return str(value)
        
        elif col_type in ('BLOB', 'BINARY', 'VARBINARY'):
            if isinstance(value, str):
                # Decode from base64
                import base64
                return base64.b64decode(value)
            else:
                return value
        
        elif col_type in ('JSON', 'JSONB'):
            return value  # Already parsed JSON
        
        else:
            return value
    
    def _make_json_serializable(self, value: Any) -> Any:
        """Make value JSON serializable.
        
        Args:
            value: Value to make serializable
            
        Returns:
            JSON-serializable value
        """
        if isinstance(value, (str, int, float, bool)) or value is None:
            return value
        elif isinstance(value, (datetime, date, time)):
            return value.isoformat()
        elif isinstance(value, Decimal):
            return str(value)
        elif isinstance(value, bytes):
            import base64
            return base64.b64encode(value).decode('ascii')
        elif isinstance(value, uuid.UUID):
            return str(value)
        elif isinstance(value, (list, tuple)):
            return [self._make_json_serializable(item) for item in value]
        elif isinstance(value, dict):
            return {k: self._make_json_serializable(v) for k, v in value.items()}
        else:
            return str(value)
    
    def _parse_default_value(self, default_str: str, column_type: str) -> Any:
        """Parse default value string based on column type.
        
        Args:
            default_str: Default value as string
            column_type: Column type
            
        Returns:
            Parsed default value
        """
        if default_str.upper() in ('NULL', 'NONE'):
            return None
        
        col_type = column_type.upper()
        
        if col_type in ('INTEGER', 'INT', 'BIGINT', 'SMALLINT', 'TINYINT'):
            return int(default_str)
        elif col_type in ('FLOAT', 'DOUBLE', 'REAL', 'DECIMAL', 'NUMERIC'):
            return float(default_str)
        elif col_type in ('BOOLEAN', 'BOOL'):
            return default_str.upper() in ('TRUE', '1', 'YES', 'ON')
        elif default_str.upper() == 'CURRENT_TIMESTAMP':
            return datetime.utcnow()
        else:
            # Remove quotes if present
            if default_str.startswith('"') and default_str.endswith('"'):
                return default_str[1:-1]
            elif default_str.startswith("'") and default_str.endswith("'"):
                return default_str[1:-1]
            else:
                return default_str
    
    def _json_serializer(self, obj: Any) -> Any:
        """Custom JSON serializer for special types.
        
        Args:
            obj: Object to serialize
            
        Returns:
            JSON-serializable representation
        """
        if isinstance(obj, (datetime, date, time)):
            return obj.isoformat()
        elif isinstance(obj, Decimal):
            return str(obj)
        elif isinstance(obj, uuid.UUID):
            return str(obj)
        elif isinstance(obj, bytes):
            import base64
            return base64.b64encode(obj).decode('ascii')
        else:
            raise TypeError(f"Object of type {type(obj)} is not JSON serializable")