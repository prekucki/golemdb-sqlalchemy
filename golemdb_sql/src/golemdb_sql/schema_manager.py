"""TOML-based schema management for GolemBase entity-table mapping using SQLglot."""

import os
import json
import toml
import sqlglot
import re
import appdirs
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, asdict
from sqlglot import expressions as exp
from .exceptions import ProgrammingError, DatabaseError
from .types import encode_signed_to_uint64, should_encode_as_signed_integer, get_integer_bit_width, encode_decimal_for_string_ordering


@dataclass
class ColumnDefinition:
    """Definition of a table column."""
    name: str
    type: str
    nullable: bool = True
    default: Optional[Any] = None
    primary_key: bool = False
    unique: bool = False
    indexed: bool = False
    precision: Optional[int] = None  # For DECIMAL/NUMERIC: total digits
    scale: Optional[int] = None      # For DECIMAL/NUMERIC: digits after decimal point
    length: Optional[int] = None     # For VARCHAR/CHAR: character length
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for TOML serialization."""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ColumnDefinition':
        """Create from dictionary."""
        return cls(**data)


def parse_column_type(type_str: str) -> Tuple[str, Optional[int], Optional[int], Optional[int]]:
    """Parse SQL column type string to extract base type, precision, scale, and length.
    
    Examples:
        DECIMAL(10,2) -> ('DECIMAL', 10, 2, None)
        VARCHAR(50) -> ('VARCHAR', None, None, 50)  
        INTEGER -> ('INTEGER', None, None, None)
        NUMBER(8,2) -> ('NUMBER', 8, 2, None)
        CHAR(10) -> ('CHAR', None, None, 10)
        
    Args:
        type_str: SQL column type string
        
    Returns:
        Tuple of (base_type, precision, scale, length)
    """
    type_str = type_str.upper().strip()
    
    # Match pattern like TYPE(precision,scale) or TYPE(precision) or TYPE
    match = re.match(r'^(\w+)(?:\((\d+)(?:,(\d+))?\))?$', type_str)
    
    if not match:
        # Fallback for complex types - just return base type
        return (type_str, None, None, None)
    
    base_type = match.group(1)
    precision = int(match.group(2)) if match.group(2) else None
    scale = int(match.group(3)) if match.group(3) else None
    
    # For string types, first parameter is length, not precision
    if base_type in ('VARCHAR', 'CHAR', 'TEXT', 'STRING'):
        length = precision
        precision = None
        scale = None
        return (base_type, precision, scale, length)
    
    # For DECIMAL/NUMERIC, both precision and scale are meaningful
    elif base_type in ('DECIMAL', 'NUMERIC', 'NUMBER'):
        return (base_type, precision, scale, None)
    
    # For other types, ignore parameters
    else:
        return (base_type, None, None, None)


@dataclass
class IndexDefinition:
    """Definition of an index."""
    name: str
    columns: List[str]
    unique: bool = False
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for TOML serialization."""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'IndexDefinition':
        """Create from dictionary."""
        return cls(**data)


@dataclass
class ForeignKeyDefinition:
    """Definition of a foreign key constraint."""
    name: str
    columns: List[str]
    referenced_table: str
    referenced_columns: List[str]
    on_delete: str = "NO ACTION"
    on_update: str = "NO ACTION"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for TOML serialization."""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ForeignKeyDefinition':
        """Create from dictionary."""
        return cls(**data)


@dataclass
class TableDefinition:
    """Definition of a table mapped to GolemBase entities."""
    name: str
    columns: List[ColumnDefinition]
    indexes: List[IndexDefinition]
    foreign_keys: List[ForeignKeyDefinition]
    entity_ttl: int = 86400  # Default TTL in seconds (24 hours)
    
    def get_primary_key_columns(self) -> List[str]:
        """Get primary key column names."""
        return [col.name for col in self.columns if col.primary_key]
    
    def get_indexed_columns(self) -> Set[str]:
        """Get all indexed column names."""
        indexed = set()
        
        # Columns marked as indexed
        for col in self.columns:
            if col.indexed or col.primary_key or col.unique:
                indexed.add(col.name)
        
        # Columns in indexes
        for idx in self.indexes:
            indexed.update(idx.columns)
        
        return indexed
    
    def get_column(self, name: str) -> Optional[ColumnDefinition]:
        """Get column definition by name."""
        for col in self.columns:
            if col.name == name:
                return col
        return None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for TOML serialization."""
        return {
            'name': self.name,
            'entity_ttl': self.entity_ttl,
            'columns': [col.to_dict() for col in self.columns],
            'indexes': [idx.to_dict() for idx in self.indexes],
            'foreign_keys': [fk.to_dict() for fk in self.foreign_keys]
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TableDefinition':
        """Create from dictionary."""
        return cls(
            name=data['name'],
            entity_ttl=data.get('entity_ttl', 86400),
            columns=[ColumnDefinition.from_dict(col) for col in data.get('columns', [])],
            indexes=[IndexDefinition.from_dict(idx) for idx in data.get('indexes', [])],
            foreign_keys=[ForeignKeyDefinition.from_dict(fk) for fk in data.get('foreign_keys', [])]
        )


class SchemaManager:
    """Manages TOML-based schemas for GolemBase entity-table mapping."""
    
    def __init__(self, schema_id: str = "default", project_id: str = "default"):
        """Initialize schema manager.
        
        Args:
            schema_id: Identifier for the schema configuration
            project_id: Project/Application identifier for multi-tenancy
        """
        self.schema_id = schema_id
        self.project_id = project_id
        self.schema_path = self._get_schema_path()
        self.tables: Dict[str, TableDefinition] = {}
        
        # Load existing schema
        self._load_schema()
    
    def _get_schema_path(self) -> Path:
        """Get path to schema TOML file.
        
        Returns:
            Path to schema file in user data directory
        """
        # Get platform-appropriate user data directory using appdirs
        user_data = Path(appdirs.user_data_dir('golembase', 'golembase'))
        
        # Create schemas directory
        schemas_dir = user_data / 'schemas'
        schemas_dir.mkdir(parents=True, exist_ok=True)
        
        return schemas_dir / f"{self.schema_id}.toml"
    
    def _load_schema(self) -> None:
        """Load schema from TOML file."""
        if not self.schema_path.exists():
            # Create empty schema file
            self._save_schema()
            return
        
        try:
            with open(self.schema_path, 'r') as f:
                schema_data = toml.load(f)
            
            # Load tables
            for table_name, table_data in schema_data.get('tables', {}).items():
                table_data['name'] = table_name
                self.tables[table_name] = TableDefinition.from_dict(table_data)
                
        except Exception as e:
            raise DatabaseError(f"Failed to load schema from {self.schema_path}: {e}")
    
    def _save_schema(self) -> None:
        """Save schema to TOML file."""
        try:
            schema_data = {
                'schema_id': self.schema_id,
                'version': '1.0',
                'tables': {}
            }
            
            # Add table definitions
            for table_name, table_def in self.tables.items():
                table_dict = table_def.to_dict()
                # Remove redundant name field
                table_dict.pop('name', None)
                schema_data['tables'][table_name] = table_dict
            
            with open(self.schema_path, 'w') as f:
                toml.dump(schema_data, f)
                
        except Exception as e:
            raise DatabaseError(f"Failed to save schema to {self.schema_path}: {e}")
    
    def add_table(self, table_def: TableDefinition) -> None:
        """Add or update table definition.
        
        Args:
            table_def: Table definition to add
        """
        self.tables[table_def.name] = table_def
        self._save_schema()
    
    def remove_table(self, table_name: str) -> None:
        """Remove table definition.
        
        Args:
            table_name: Name of table to remove
        """
        if table_name in self.tables:
            del self.tables[table_name]
            self._save_schema()
    
    def get_table(self, table_name: str) -> Optional[TableDefinition]:
        """Get table definition by name.
        
        Args:
            table_name: Name of table
            
        Returns:
            Table definition or None if not found
        """
        return self.tables.get(table_name)
    
    def get_table_names(self) -> List[str]:
        """Get all table names.
        
        Returns:
            List of table names
        """
        return list(self.tables.keys())
    
    def table_exists(self, table_name: str) -> bool:
        """Check if table exists in schema.
        
        Args:
            table_name: Name of table
            
        Returns:
            True if table exists
        """
        return table_name in self.tables
    
    def create_table_from_sql(self, sql: str) -> TableDefinition:
        """Create table definition from CREATE TABLE SQL statement using SQLglot.
        
        Args:
            sql: CREATE TABLE SQL statement
            
        Returns:
            Table definition
            
        Raises:
            ProgrammingError: If SQL cannot be parsed
        """
        try:
            # Parse SQL using SQLglot
            parsed = sqlglot.parse_one(sql, read="sqlite")  # Use SQLite dialect as base
            
            if not isinstance(parsed, exp.Create):
                raise ValueError("Not a CREATE statement")
            
            if not hasattr(parsed, 'this') or not isinstance(parsed.this, exp.Schema):
                raise ValueError("Not a CREATE TABLE statement")
            
            # Extract table name
            table_name = parsed.this.this.name if hasattr(parsed.this.this, 'name') else str(parsed.this.this)
            
            # Extract columns
            columns = []
            indexes = []
            foreign_keys = []
            
            # Process schema (column definitions)
            if parsed.this.expressions:
                for expr in parsed.this.expressions:
                    if isinstance(expr, exp.ColumnDef):
                        col_def = self._parse_column_definition(expr)
                        columns.append(col_def)
                    elif isinstance(expr, exp.PrimaryKeyColumnConstraint):
                        # Handle primary key constraints
                        for col in columns:
                            if col.name in [c.name for c in expr.expressions]:
                                col.primary_key = True
                                col.indexed = True
                    elif isinstance(expr, exp.UniqueColumnConstraint):
                        # Handle unique constraints
                        constraint_columns = [c.name for c in expr.expressions]
                        for col in columns:
                            if col.name in constraint_columns:
                                col.unique = True
                                col.indexed = True
                        
                        # Add as index
                        if constraint_columns:
                            indexes.append(IndexDefinition(
                                name=f"uk_{table_name}_{'_'.join(constraint_columns)}",
                                columns=constraint_columns,
                                unique=True
                            ))
                    elif isinstance(expr, exp.ForeignKey):
                        # Handle foreign key constraints
                        fk_def = self._parse_foreign_key(expr, table_name)
                        if fk_def:
                            foreign_keys.append(fk_def)
            
            return TableDefinition(
                name=table_name,
                columns=columns,
                indexes=indexes,
                foreign_keys=foreign_keys
            )
            
        except Exception as e:
            raise ProgrammingError(f"Failed to parse CREATE TABLE statement: {e}")
    
    def _parse_column_definition(self, col_expr: exp.ColumnDef) -> ColumnDefinition:
        """Parse SQLglot column definition.
        
        Args:
            col_expr: SQLglot ColumnDef expression
            
        Returns:
            Column definition
        """
        col_name = col_expr.this.name
        
        # Extract column type from SQLglot args
        col_type = "TEXT"  # Default
        if hasattr(col_expr, 'args') and 'kind' in col_expr.args and col_expr.args['kind']:
            col_type = str(col_expr.args['kind']).upper()
        elif hasattr(col_expr, 'kind') and col_expr.kind:
            col_type = str(col_expr.kind).upper()
        elif hasattr(col_expr, 'type') and col_expr.type:
            col_type = str(col_expr.type).upper()
        
        # Parse column type to extract precision, scale, length
        base_type, precision, scale, length = parse_column_type(col_type)
        
        # Extract constraints
        nullable = True
        default = None
        primary_key = False
        unique = False
        
        for constraint in col_expr.constraints:
            # Handle ColumnConstraint wrapper
            constraint_kind = constraint.kind if hasattr(constraint, 'kind') else constraint
            
            if isinstance(constraint_kind, exp.NotNullColumnConstraint):
                nullable = False
            elif isinstance(constraint_kind, exp.DefaultColumnConstraint):
                if hasattr(constraint_kind, 'this') and constraint_kind.this:
                    default = str(constraint_kind.this)
            elif isinstance(constraint_kind, exp.PrimaryKeyColumnConstraint):
                primary_key = True
                nullable = False
            elif isinstance(constraint_kind, exp.UniqueColumnConstraint):
                unique = True
            elif isinstance(constraint_kind, exp.AutoIncrementColumnConstraint):
                # Handle AUTOINCREMENT
                pass
        
        return ColumnDefinition(
            name=col_name,
            type=col_type,  # Keep full type string for compatibility
            nullable=nullable,
            default=default,
            primary_key=primary_key,
            unique=unique,
            indexed=primary_key or unique,
            precision=precision,
            scale=scale,
            length=length
        )
    
    def _parse_foreign_key(self, fk_expr: exp.ForeignKey, table_name: str) -> Optional[ForeignKeyDefinition]:
        """Parse SQLglot foreign key constraint.
        
        Args:
            fk_expr: SQLglot ForeignKey expression
            table_name: Name of the table containing the foreign key
            
        Returns:
            Foreign key definition or None if parsing fails
        """
        try:
            # Extract referenced table and columns
            if not fk_expr.reference:
                return None
            
            ref_table = fk_expr.reference.this.name
            
            # Extract column names
            columns = []
            if fk_expr.expressions:
                columns = [expr.name for expr in fk_expr.expressions]
            
            ref_columns = []
            if fk_expr.reference.expressions:
                ref_columns = [expr.name for expr in fk_expr.reference.expressions]
            
            # Extract ON DELETE/UPDATE actions
            on_delete = "NO ACTION"
            on_update = "NO ACTION"
            
            if hasattr(fk_expr, 'delete') and fk_expr.delete:
                on_delete = str(fk_expr.delete).upper()
            
            if hasattr(fk_expr, 'update') and fk_expr.update:
                on_update = str(fk_expr.update).upper()
            
            return ForeignKeyDefinition(
                name=f"fk_{table_name}_{'_'.join(columns)}",
                columns=columns,
                referenced_table=ref_table,
                referenced_columns=ref_columns,
                on_delete=on_delete,
                on_update=on_update
            )
            
        except Exception:
            return None
    
    def get_entity_annotations_for_table(self, table_name: str, row_data: Dict[str, Any]) -> Dict[str, Any]:
        """Get GolemBase annotations for a table row.
        
        Args:
            table_name: Name of table
            row_data: Row data as dictionary
            
        Returns:
            Dictionary of annotations (string and numeric)
        """
        table_def = self.get_table(table_name)
        if not table_def:
            raise ProgrammingError(f"Table '{table_name}' not found in schema")
        
        # Metadata annotations
        string_annotations = {
            'row_type': 'json',
            'relation': f'{self.project_id}.{table_name}'
        }
        numeric_annotations = {}
        
        # Add indexed columns as annotations with idx_ prefix
        indexed_columns = table_def.get_indexed_columns()
        
        for col_name in indexed_columns:
            if col_name in row_data:
                value = row_data[col_name]
                col_def = table_def.get_column(col_name)
                
                if col_def and value is not None:
                    # Determine annotation type based on column type
                    if col_def.type.upper() in ('INTEGER', 'INT', 'BIGINT', 'SMALLINT', 'TINYINT'):
                        int_value = int(value)
                        if should_encode_as_signed_integer(col_def.type):
                            # Apply signed integer encoding for all signed integer types to preserve ordering
                            bit_width = get_integer_bit_width(col_def.type)
                            numeric_annotations[f'idx_{col_name}'] = encode_signed_to_uint64(int_value, bit_width)
                        else:
                            # Should not reach here as all integer types need encoding
                            numeric_annotations[f'idx_{col_name}'] = int_value
                    elif col_def.type.upper().startswith(('DECIMAL', 'NUMERIC', 'NUMBER')):
                        # DECIMAL/NUMERIC are stored as string annotations with lexicographic ordering
                        precision = col_def.precision or 18  # Default precision
                        scale = col_def.scale or 0           # Default scale
                        
                        try:
                            encoded_decimal = encode_decimal_for_string_ordering(value, precision, scale)
                            string_annotations[f'idx_{col_name}'] = encoded_decimal
                        except ValueError as e:
                            raise ValueError(f"DECIMAL value {value} invalid for column {col_name} {col_def.type}: {e}")
                    # Note: FLOAT, DOUBLE, REAL are NOT indexable due to precision issues
                    elif col_def.type.upper() in ('BOOLEAN', 'BOOL'):
                        numeric_annotations[f'idx_{col_name}'] = 1 if value else 0
                    elif col_def.type.upper() in ('DATETIME', 'TIMESTAMP'):
                        # Convert datetime to Unix timestamp
                        if hasattr(value, 'timestamp'):
                            numeric_annotations[f'idx_{col_name}'] = int(value.timestamp())
                        elif isinstance(value, (int, float)):
                            numeric_annotations[f'idx_{col_name}'] = int(value)
                    else:
                        # String annotation
                        string_annotations[f'idx_{col_name}'] = str(value)
        
        return {
            'string_annotations': string_annotations,
            'numeric_annotations': numeric_annotations
        }
    
    def get_ttl_for_table(self, table_name: str) -> int:
        """Get TTL (time-to-live) for table entities.
        
        Args:
            table_name: Name of table
            
        Returns:
            TTL in seconds
        """
        table_def = self.get_table(table_name)
        return table_def.entity_ttl if table_def else 86400  # Default 24 hours