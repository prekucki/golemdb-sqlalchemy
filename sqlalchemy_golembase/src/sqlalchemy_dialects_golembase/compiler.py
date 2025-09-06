"""SQLAlchemy compiler classes for GolemBase dialect."""

from sqlalchemy.sql import compiler
from sqlalchemy.sql.expression import Select, Insert, Update, Delete


class GolemBaseCompiler(compiler.SQLCompiler):
    """SQL statement compiler for GolemBase."""
    
    def visit_select(self, select, **kwargs):
        """Compile SELECT statements for GolemBase."""
        # Start with the default compilation
        text = super().visit_select(select, **kwargs)
        
        # Add any GolemBase-specific modifications here
        # For example, if GolemBase has different LIMIT/OFFSET syntax
        
        return text
    
    def visit_insert(self, insert_stmt, **kwargs):
        """Compile INSERT statements for GolemBase."""
        text = super().visit_insert(insert_stmt, **kwargs)
        
        # Handle GolemBase-specific INSERT syntax if needed
        
        return text
    
    def visit_update(self, update_stmt, **kwargs):
        """Compile UPDATE statements for GolemBase."""
        text = super().visit_update(update_stmt, **kwargs)
        
        # Handle GolemBase-specific UPDATE syntax if needed
        
        return text
    
    def visit_delete(self, delete_stmt, **kwargs):
        """Compile DELETE statements for GolemBase."""
        text = super().visit_delete(delete_stmt, **kwargs)
        
        # Handle GolemBase-specific DELETE syntax if needed
        
        return text
    
    def limit_clause(self, select, **kwargs):
        """Generate LIMIT clause for GolemBase."""
        # Customize based on GolemBase LIMIT syntax
        text = ""
        if select._limit_clause is not None:
            text += f"\n LIMIT {self.process(select._limit_clause, **kwargs)}"
        if select._offset_clause is not None:
            text += f" OFFSET {self.process(select._offset_clause, **kwargs)}"
        return text
    
    def for_update_clause(self, select, **kwargs):
        """Generate FOR UPDATE clause for GolemBase."""
        if select._for_update_arg is not None:
            if select._for_update_arg.read:
                return " FOR SHARE"
            else:
                return " FOR UPDATE"
        return ""


class GolemBaseTypeCompiler(compiler.GenericTypeCompiler):
    """Type compiler for GolemBase data types."""
    
    def visit_INTEGER(self, type_, **kwargs):
        """Compile INTEGER type."""
        return "INTEGER"
    
    def visit_BIGINT(self, type_, **kwargs):
        """Compile BIGINT type."""
        return "BIGINT"
    
    def visit_SMALLINT(self, type_, **kwargs):
        """Compile SMALLINT type."""
        return "SMALLINT"
    
    def visit_VARCHAR(self, type_, **kwargs):
        """Compile VARCHAR type."""
        if type_.length:
            return f"VARCHAR({type_.length})"
        return "VARCHAR"
    
    def visit_CHAR(self, type_, **kwargs):
        """Compile CHAR type."""
        if type_.length:
            return f"CHAR({type_.length})"
        return "CHAR"
    
    def visit_TEXT(self, type_, **kwargs):
        """Compile TEXT type."""
        return "TEXT"
    
    def visit_FLOAT(self, type_, **kwargs):
        """Compile FLOAT type."""
        if type_.precision:
            return f"FLOAT({type_.precision})"
        return "FLOAT"
    
    def visit_DOUBLE(self, type_, **kwargs):
        """Compile DOUBLE type."""
        return "DOUBLE"
    
    def visit_DECIMAL(self, type_, **kwargs):
        """Compile DECIMAL type."""
        if type_.precision and type_.scale:
            return f"DECIMAL({type_.precision}, {type_.scale})"
        elif type_.precision:
            return f"DECIMAL({type_.precision})"
        return "DECIMAL"
    
    def visit_BOOLEAN(self, type_, **kwargs):
        """Compile BOOLEAN type."""
        return "BOOLEAN"
    
    def visit_DATE(self, type_, **kwargs):
        """Compile DATE type."""
        return "DATE"
    
    def visit_TIME(self, type_, **kwargs):
        """Compile TIME type."""
        return "TIME"
    
    def visit_DATETIME(self, type_, **kwargs):
        """Compile DATETIME type."""
        return "DATETIME"
    
    def visit_TIMESTAMP(self, type_, **kwargs):
        """Compile TIMESTAMP type."""
        return "TIMESTAMP"
    
    def visit_BLOB(self, type_, **kwargs):
        """Compile BLOB type."""
        return "BLOB"
    
    def visit_JSON(self, type_, **kwargs):
        """Compile JSON type if supported by GolemBase."""
        return "JSON"