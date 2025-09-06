"""SQL to GolemBase annotation query translator using SQLglot."""

import sqlglot
import re
from typing import Any, Dict, List, Optional, Tuple, Union
from dataclasses import dataclass
from sqlglot import expressions as exp
from .exceptions import ProgrammingError, DatabaseError
from .schema_manager import SchemaManager
from .types import encode_signed_to_uint64, should_encode_as_signed_integer, get_integer_bit_width, encode_decimal_for_string_ordering


@dataclass
class QueryResult:
    """Result of SQL query translation."""
    operation_type: str
    table_name: str
    golem_query: str
    columns: Optional[List[str]] = None
    insert_data: Optional[Dict[str, Any]] = None
    update_data: Optional[Dict[str, Any]] = None
    limit: Optional[int] = None
    offset: Optional[int] = None
    sort_by: Optional[str] = None
    sort_order: str = "asc"
    post_filter_conditions: Optional[List[Dict[str, Any]]] = None  # Non-indexed column conditions


class QueryTranslator:
    """Translates SQL queries to GolemBase annotation-based queries."""
    
    def __init__(self, schema_manager: SchemaManager):
        """Initialize query translator.
        
        Args:
            schema_manager: Schema manager for table definitions
        """
        self.schema_manager = schema_manager
    
    def _preprocess_sql(self, sql: str, parameters: Optional[Dict[str, Any]] = None) -> Tuple[str, Optional[Dict[str, Any]]]:
        """Preprocess SQL to handle Python DB-API parameter styles.
        
        Converts %(name)s style parameters to named parameters that SQLglot can understand.
        
        Args:
            sql: Original SQL with %(name)s parameters
            parameters: Parameter values
            
        Returns:
            Tuple of (processed_sql, processed_parameters)
        """
        if not parameters:
            return sql, parameters
        
        # Find all %(name)s style parameters
        param_pattern = r'%\((\w+)\)s'
        param_matches = re.finditer(param_pattern, sql)
        
        processed_sql = sql
        processed_params = parameters.copy() if isinstance(parameters, dict) else {}
        
        # Replace %(name)s with :name for SQLglot
        for match in param_matches:
            param_name = match.group(1)
            old_param = match.group(0)  # %(name)s
            new_param = f':{param_name}'  # :name
            processed_sql = processed_sql.replace(old_param, new_param)
        
        return processed_sql, processed_params
    
    def translate_select(self, sql: str, parameters: Optional[Dict[str, Any]] = None) -> QueryResult:
        """Translate SELECT statement to GolemBase query.
        
        Args:
            sql: SELECT SQL statement
            parameters: Query parameters
            
        Returns:
            Dictionary with query information and annotation filters
        """
        try:
            # Preprocess SQL to handle parameter styles
            processed_sql, processed_params = self._preprocess_sql(sql, parameters)
            
            # Parse SQL
            parsed = sqlglot.parse_one(processed_sql, read="sqlite")
            
            if not isinstance(parsed, exp.Select):
                raise ValueError("Not a SELECT statement")
            
            # Extract table information
            table_info = self._extract_table_info(parsed)
            if not table_info:
                raise ValueError("No table found in SELECT statement")
            
            table_name, table_alias = table_info[0]  # Use first table for now
            
            # Verify table exists in schema
            if not self.schema_manager.table_exists(table_name):
                raise ProgrammingError(f"Table '{table_name}' does not exist")
            
            # Extract WHERE clause conditions
            where_clause = None
            if hasattr(parsed, 'where') and parsed.where is not None:
                # Handle different SQLglot versions
                if callable(parsed.where):
                    where_clause = parsed.find(exp.Where)
                    if where_clause:
                        where_clause = where_clause.this
                else:
                    where_clause = parsed.where
            annotation_query, post_filter_conditions = self._build_annotation_query(where_clause, table_name, processed_params)
            
            # Extract selected columns
            selected_columns = self._extract_selected_columns(parsed, table_name)
            
            # Extract ORDER BY
            order_by = self._extract_order_by(parsed, table_name)
            
            # Extract LIMIT/OFFSET
            limit_offset = self._extract_limit_offset(parsed)
            
            return QueryResult(
                operation_type='SELECT',
                table_name=table_name,
                golem_query=annotation_query,
                columns=selected_columns,
                limit=limit_offset.get('limit'),
                offset=limit_offset.get('offset'),
                sort_by=order_by[0]['column'] if order_by else None,
                sort_order='desc' if order_by and order_by[0].get('desc') else 'asc',
                post_filter_conditions=post_filter_conditions
            )
            
        except Exception as e:
            raise ProgrammingError(f"Failed to translate SELECT query: {e}")
    
    def translate_insert(self, sql: str, parameters: Optional[Dict[str, Any]] = None) -> QueryResult:
        """Translate INSERT statement to GolemBase entity creation.
        
        Args:
            sql: INSERT SQL statement
            parameters: Query parameters
            
        Returns:
            Dictionary with insert information
        """
        try:
            # Preprocess SQL to handle parameter styles
            processed_sql, processed_params = self._preprocess_sql(sql, parameters)
            
            # Parse SQL
            parsed = sqlglot.parse_one(processed_sql, read="sqlite")
            
            if not isinstance(parsed, exp.Insert):
                raise ValueError("Not an INSERT statement")
            
            # Extract table name using find method for better compatibility
            table_expr = parsed.find(exp.Table)
            if table_expr and hasattr(table_expr, 'name'):
                table_name = table_expr.name
            elif hasattr(parsed, 'this') and hasattr(parsed.this, 'name'):
                table_name = parsed.this.name
            else:
                raise ValueError("Cannot extract table name from INSERT statement")
            
            # Verify table exists
            if not self.schema_manager.table_exists(table_name):
                raise ProgrammingError(f"Table '{table_name}' does not exist")
            
            # Extract column names and values
            columns = []
            if parsed.this.expressions:
                columns = [col.name for col in parsed.this.expressions]
            
            # Extract values
            values = []
            if isinstance(parsed.expression, exp.Values):
                for tuple_expr in parsed.expression.expressions:
                    if isinstance(tuple_expr, exp.Tuple):
                        row_values = []
                        for val_expr in tuple_expr.expressions:
                            value = self._extract_literal_value(val_expr, processed_params)
                            row_values.append(value)
                        values.append(row_values)
            
            # Convert values to insert_data format
            insert_data = {}
            if columns and values:
                for row in values:
                    for i, col in enumerate(columns):
                        if i < len(row):
                            insert_data[col] = row[i]
                        
            return QueryResult(
                operation_type='INSERT',
                table_name=table_name,
                golem_query=f'table="{table_name}"',
                columns=columns,
                insert_data=insert_data
            )
            
        except Exception as e:
            raise ProgrammingError(f"Failed to translate INSERT query: {e}")
    
    def translate_update(self, sql: str, parameters: Optional[Dict[str, Any]] = None) -> QueryResult:
        """Translate UPDATE statement to GolemBase entity updates.
        
        Args:
            sql: UPDATE SQL statement  
            parameters: Query parameters
            
        Returns:
            Dictionary with update information
        """
        try:
            # Preprocess SQL to handle parameter styles
            processed_sql, processed_params = self._preprocess_sql(sql, parameters)
            
            # Parse SQL
            parsed = sqlglot.parse_one(processed_sql, read="sqlite")
            
            if not isinstance(parsed, exp.Update):
                raise ValueError("Not an UPDATE statement")
            
            # Extract table name using find method for better compatibility
            table_expr = parsed.find(exp.Table)
            if table_expr and hasattr(table_expr, 'name'):
                table_name = table_expr.name
            elif hasattr(parsed, 'this') and hasattr(parsed.this, 'name'):
                table_name = parsed.this.name
            else:
                raise ValueError("Cannot extract table name from UPDATE statement")
            
            # Verify table exists
            if not self.schema_manager.table_exists(table_name):
                raise ProgrammingError(f"Table '{table_name}' does not exist")
            
            # Extract SET clause (column = value pairs)
            set_values = {}
            if parsed.expressions:
                for set_expr in parsed.expressions:
                    if isinstance(set_expr, exp.EQ):
                        col_name = set_expr.this.name
                        value = self._extract_literal_value(set_expr.expression, processed_params)
                        set_values[col_name] = value
            
            # Extract WHERE clause for finding entities to update
            annotation_query, post_filter_conditions = self._build_annotation_query(parsed.where, table_name, processed_params)
            
            return QueryResult(
                operation_type='UPDATE',
                table_name=table_name,
                golem_query=annotation_query,
                update_data=set_values
            )
            
        except Exception as e:
            raise ProgrammingError(f"Failed to translate UPDATE query: {e}")
    
    def translate_delete(self, sql: str, parameters: Optional[Dict[str, Any]] = None) -> QueryResult:
        """Translate DELETE statement to GolemBase entity deletions.
        
        Args:
            sql: DELETE SQL statement
            parameters: Query parameters
            
        Returns:
            Dictionary with delete information
        """
        try:
            # Preprocess SQL to handle parameter styles
            processed_sql, processed_params = self._preprocess_sql(sql, parameters)
            
            # Parse SQL
            parsed = sqlglot.parse_one(processed_sql, read="sqlite")
            
            if not isinstance(parsed, exp.Delete):
                raise ValueError("Not a DELETE statement")
            
            # Extract table name using find method for better compatibility
            table_expr = parsed.find(exp.Table)
            if table_expr and hasattr(table_expr, 'name'):
                table_name = table_expr.name
            elif hasattr(parsed, 'this') and hasattr(parsed.this, 'name'):
                table_name = parsed.this.name
            else:
                raise ValueError("Cannot extract table name from DELETE statement")
            
            # Verify table exists
            if not self.schema_manager.table_exists(table_name):
                raise ProgrammingError(f"Table '{table_name}' does not exist")
            
            # Extract WHERE clause for finding entities to delete
            annotation_query, post_filter_conditions = self._build_annotation_query(parsed.where, table_name, processed_params)
            
            return QueryResult(
                operation_type='DELETE',
                table_name=table_name,
                golem_query=annotation_query
            )
            
        except Exception as e:
            raise ProgrammingError(f"Failed to translate DELETE query: {e}")
    
    def _extract_table_info(self, select_expr: exp.Select) -> List[Tuple[str, Optional[str]]]:
        """Extract table names and aliases from SELECT.
        
        Args:
            select_expr: SQLglot SELECT expression
            
        Returns:
            List of (table_name, alias) tuples
        """
        tables = []
        
        if hasattr(select_expr, 'from_') and select_expr.from_:
            # Handle newer SQLglot API where from_ is a property
            if hasattr(select_expr.from_, 'this'):
                from_expr = select_expr.from_.this
            elif hasattr(select_expr, 'find') and select_expr.find(exp.From):
                from_clause = select_expr.find(exp.From)
                from_expr = from_clause.this if from_clause else None
            else:
                from_expr = None
            
            if isinstance(from_expr, exp.Table):
                table_name = from_expr.name
                alias = from_expr.alias if hasattr(from_expr, 'alias') else None
                tables.append((table_name, alias))
            
            # Handle JOINs
            for join in select_expr.args.get('joins', []):
                if isinstance(join.this, exp.Table):
                    table_name = join.this.name
                    alias = join.this.alias if hasattr(join.this, 'alias') else None
                    tables.append((table_name, alias))
        
        return tables
    
    def _build_annotation_query(self, where_expr: Optional[exp.Expression], table_name: str, parameters: Optional[Dict[str, Any]]) -> Tuple[str, List[Dict[str, Any]]]:
        """Build GolemBase annotation query from WHERE clause.
        
        Args:
            where_expr: SQLglot WHERE expression
            table_name: Table name for context
            parameters: Query parameters
            
        Returns:
            Tuple of (GolemBase query string, post-filter conditions for non-indexed columns)
        """
        if not where_expr:
            return f'relation="{self.schema_manager.project_id}.{table_name}"', []
        
        # Start with relation filter (project_id.table_name)
        base_query = f'relation="{self.schema_manager.project_id}.{table_name}"'
        
        # Initialize collection for post-filter conditions
        self._post_filter_conditions = []
        
        # Convert WHERE expression to annotation query
        where_query = self._convert_expression_to_annotation(where_expr, table_name, parameters)
        
        # Collect any post-filter conditions that were found
        post_filter_conditions = getattr(self, '_post_filter_conditions', [])
        
        if where_query:
            return f'{base_query} && ({where_query})', post_filter_conditions
        else:
            return base_query, post_filter_conditions
    
    def _convert_expression_to_annotation(self, expr: exp.Expression, table_name: str, parameters: Optional[Dict[str, Any]]) -> str:
        """Convert SQLglot expression to GolemBase annotation query.
        
        Args:
            expr: SQLglot expression
            table_name: Table name for column context
            parameters: Query parameters
            
        Returns:
            Annotation query string
        """
        if isinstance(expr, exp.EQ):
            # Equality: column = value
            left = self._get_column_name(expr.this)
            right = self._extract_literal_value(expr.expression, parameters)
            return self._format_annotation_condition(left, '=', right, table_name)
        
        elif isinstance(expr, exp.NEQ):
            # Not equal: column != value (not directly supported, use NOT)
            left = self._get_column_name(expr.this)
            right = self._extract_literal_value(expr.expression, parameters)
            condition = self._format_annotation_condition(left, '=', right, table_name)
            return f'!({condition})'  # Negate the condition
        
        elif isinstance(expr, exp.GT):
            # Greater than: column > value
            left = self._get_column_name(expr.this)
            right = self._extract_literal_value(expr.expression, parameters)
            return self._format_annotation_condition(left, '>', right, table_name)
        
        elif isinstance(expr, exp.GTE):
            # Greater than or equal: column >= value
            left = self._get_column_name(expr.this)
            right = self._extract_literal_value(expr.expression, parameters)
            return self._format_annotation_condition(left, '>=', right, table_name)
        
        elif isinstance(expr, exp.LT):
            # Less than: column < value
            left = self._get_column_name(expr.this)
            right = self._extract_literal_value(expr.expression, parameters)
            return self._format_annotation_condition(left, '<', right, table_name)
        
        elif isinstance(expr, exp.LTE):
            # Less than or equal: column <= value
            left = self._get_column_name(expr.this)
            right = self._extract_literal_value(expr.expression, parameters)
            return self._format_annotation_condition(left, '<=', right, table_name)
        
        elif isinstance(expr, exp.And):
            # AND: combine with &&, filtering out None conditions (non-indexed columns)
            left = self._convert_expression_to_annotation(expr.this, table_name, parameters)
            right = self._convert_expression_to_annotation(expr.expression, table_name, parameters)
            
            # Handle None values (non-indexed conditions)
            if left is None and right is None:
                return None  # Both conditions are post-filters
            elif left is None:
                return right  # Only right condition can be used in GolemBase query
            elif right is None:
                return left   # Only left condition can be used in GolemBase query
            else:
                return f'({left}) && ({right})'
        
        elif isinstance(expr, exp.Or):
            # OR: combine with ||, but if any condition is non-indexed, we can't optimize
            left = self._convert_expression_to_annotation(expr.this, table_name, parameters)
            right = self._convert_expression_to_annotation(expr.expression, table_name, parameters)
            
            # For OR conditions, if any side is non-indexed, we need to fetch all and post-filter
            if left is None or right is None:
                return None  # Can't optimize OR with mixed indexed/non-indexed
            else:
                return f'({left}) || ({right})'
        
        elif isinstance(expr, exp.Not):
            # NOT: negate expression
            inner = self._convert_expression_to_annotation(expr.this, table_name, parameters)
            return f'!({inner})'
        
        elif isinstance(expr, exp.Like):
            # LIKE: convert to string equality or contains (limited support)
            left = self._get_column_name(expr.this)
            right = self._extract_literal_value(expr.expression, parameters)
            
            # Simple case: exact match (no wildcards)
            if isinstance(right, str) and '%' not in right and '_' not in right:
                return self._format_annotation_condition(left, '=', right, table_name)
            else:
                # GolemBase doesn't support LIKE - would need to filter results after retrieval
                raise ProgrammingError("LIKE with wildcards not supported in GolemBase annotations")
        
        elif isinstance(expr, exp.In):
            # IN: convert to multiple OR conditions
            left = self._get_column_name(expr.this)
            
            if isinstance(expr.expressions[0], exp.Tuple):
                values = []
                for val_expr in expr.expressions[0].expressions:
                    value = self._extract_literal_value(val_expr, parameters)
                    values.append(value)
                
                # Build OR conditions
                conditions = []
                for value in values:
                    condition = self._format_annotation_condition(left, '=', value, table_name)
                    conditions.append(condition)
                
                return '(' + ' || '.join(conditions) + ')'
        
        else:
            raise ProgrammingError(f"Unsupported WHERE expression type: {type(expr)}")
    
    def _get_column_name(self, expr: exp.Expression) -> str:
        """Extract column name from expression.
        
        Args:
            expr: SQLglot expression
            
        Returns:
            Column name
        """
        if isinstance(expr, exp.Column):
            return expr.name
        elif hasattr(expr, 'name'):
            return expr.name
        else:
            return str(expr)
    
    def _extract_literal_value(self, expr: exp.Expression, parameters: Optional[Dict[str, Any]]) -> Any:
        """Extract literal value from expression.
        
        Args:
            expr: SQLglot expression
            parameters: Query parameters for substitution
            
        Returns:
            Literal value
        """
        if isinstance(expr, exp.Literal):
            if expr.is_string:
                return expr.this  # String literal
            elif expr.is_int:
                return int(expr.this)
            elif expr.is_number:
                return float(expr.this)
            else:
                return expr.this
        
        elif isinstance(expr, exp.Placeholder):
            # Handle both named and positional parameters
            param_name = expr.name if hasattr(expr, 'name') else None
            
            # Handle :name style parameters (from our preprocessing)
            if param_name and param_name.startswith(':'):
                param_name = param_name[1:]  # Remove the ':' prefix
            
            if param_name and parameters and isinstance(parameters, dict) and param_name in parameters:
                # Named parameter: :name, %(name)s
                return parameters[param_name]
            elif parameters and isinstance(parameters, (list, tuple)):
                # Positional parameter: ? - use first parameter for now
                # This is a simplification; proper implementation would track parameter index
                if len(parameters) > 0:
                    return parameters[0]  # Return first parameter for single value queries
                else:
                    raise ProgrammingError("No parameters provided for positional placeholder")
            else:
                raise ProgrammingError(f"Parameter '{param_name or '?'}' not provided")
        
        elif isinstance(expr, exp.Null):
            return None
        
        elif isinstance(expr, exp.Boolean):
            return expr.this
        
        else:
            # Try to convert to string as fallback
            return str(expr)
    
    def _format_annotation_condition(self, column: str, operator: str, value: Any, table_name: str) -> str:
        """Format annotation condition based on column type.
        
        Args:
            column: Column name
            operator: Comparison operator
            value: Value to compare
            table_name: Table name for column type lookup
            
        Returns:
            Formatted annotation condition
        """
        table_def = self.schema_manager.get_table(table_name)
        if not table_def:
            # Fallback to string annotation
            if isinstance(value, str):
                return f'{column}="{value}"'
            else:
                return f'{column}={value}'
        
        col_def = table_def.get_column(column)
        if not col_def:
            # Column not in schema, treat as string
            if isinstance(value, str):
                return f'{column}="{value}"'
            else:
                return f'{column}={value}'
        
        # Check if column is indexed - only indexed columns can use idx_ prefix
        indexed_columns = table_def.get_indexed_columns()
        is_indexed = column in indexed_columns
        
        if not is_indexed:
            # Store this condition for post-filtering and return None to exclude from GolemBase query
            if not hasattr(self, '_post_filter_conditions'):
                self._post_filter_conditions = []
            
            self._post_filter_conditions.append({
                'column': column,
                'operator': operator, 
                'value': value,
                'column_type': col_def.type if col_def else 'STRING'
            })
            return None
        
        # Format based on column type for indexed columns
        if col_def.type.upper() in ('INTEGER', 'INT', 'BIGINT', 'SMALLINT', 'TINYINT'):
            int_value = int(value)
            if should_encode_as_signed_integer(col_def.type):
                # Apply signed integer encoding for all signed integer types to preserve ordering
                bit_width = get_integer_bit_width(col_def.type)
                encoded_value = encode_signed_to_uint64(int_value, bit_width)
                return f'idx_{column}{operator}{encoded_value}'
            else:
                # Should not reach here as all integer types need encoding
                return f'idx_{column}{operator}{int_value}'
        elif col_def.type.upper().startswith(('DECIMAL', 'NUMERIC', 'NUMBER')):
            # DECIMAL/NUMERIC use string annotations with lexicographic ordering
            precision = col_def.precision or 18  # Default precision
            scale = col_def.scale or 0           # Default scale
            
            try:
                encoded_value = encode_decimal_for_string_ordering(value, precision, scale)
                return f'idx_{column}{operator}"{encoded_value}"'  # String comparison
            except ValueError as e:
                raise ProgrammingError(f"DECIMAL query value {value} invalid for column {column} {col_def.type}: {e}")
        elif col_def.type.upper() in ('FLOAT', 'DOUBLE', 'REAL'):
            # Floating point types are not indexable - cannot query by annotation
            raise ProgrammingError(f"Column '{column}' has type {col_def.type} which is not indexable. FLOAT/DOUBLE/REAL types cannot be used in WHERE clauses.")
        elif col_def.type.upper() in ('BOOLEAN', 'BOOL'):
            bool_value = 1 if value else 0
            return f'idx_{column}{operator}{bool_value}'
        elif col_def.type.upper() in ('DATETIME', 'TIMESTAMP'):
            # Convert to Unix timestamp if needed
            if hasattr(value, 'timestamp'):
                timestamp = int(value.timestamp())
            elif isinstance(value, (int, float)):
                timestamp = int(value)
            else:
                timestamp = value
            return f'idx_{column}{operator}{timestamp}'
        else:
            # String annotation
            return f'idx_{column}{operator}"{value}"'
    
    def _extract_selected_columns(self, select_expr: exp.Select, table_name: str) -> List[str]:
        """Extract selected columns from SELECT.
        
        Args:
            select_expr: SQLglot SELECT expression
            table_name: Table name for context
            
        Returns:
            List of column names (empty list means SELECT *)
        """
        columns = []
        
        if select_expr.expressions:
            for expr in select_expr.expressions:
                if isinstance(expr, exp.Star):
                    # SELECT * - return empty list to indicate all columns
                    return []
                elif isinstance(expr, exp.Column):
                    columns.append(expr.name)
                elif isinstance(expr, exp.Alias):
                    # Handle aliased columns
                    if isinstance(expr.this, exp.Column):
                        columns.append(expr.this.name)
                    else:
                        # Complex expression - use alias name
                        columns.append(expr.alias)
                else:
                    # Other expressions - use string representation
                    columns.append(str(expr))
        
        return columns
    
    def _extract_order_by(self, select_expr: exp.Select, table_name: str) -> List[Dict[str, Any]]:
        """Extract ORDER BY clause.
        
        Args:
            select_expr: SQLglot SELECT expression
            table_name: Table name for context
            
        Returns:
            List of order specifications
        """
        order_by = []
        
        # Find ORDER BY clauses using find method
        order_by_clause = select_expr.find(exp.Order) 
        if order_by_clause and order_by_clause.expressions:
            for order_expr in order_by_clause.expressions:
                if isinstance(order_expr, exp.Ordered):
                    column = self._get_column_name(order_expr.this)
                    desc = getattr(order_expr, 'desc', False)
                    order_by.append({
                        'column': column,
                        'desc': desc
                    })
        
        return order_by
    
    def _extract_limit_offset(self, select_expr: exp.Select) -> Dict[str, Optional[int]]:
        """Extract LIMIT and OFFSET values.
        
        Args:
            select_expr: SQLglot SELECT expression
            
        Returns:
            Dictionary with limit and offset values
        """
        limit = None
        offset = None
        
        # Find LIMIT clause using find method
        limit_clause = select_expr.find(exp.Limit)
        if limit_clause:
            if limit_clause.expression:
                limit = int(limit_clause.expression.this)
            if hasattr(limit_clause, 'offset') and limit_clause.offset:
                offset = int(limit_clause.offset.this)
        
        return {'limit': limit, 'offset': offset}