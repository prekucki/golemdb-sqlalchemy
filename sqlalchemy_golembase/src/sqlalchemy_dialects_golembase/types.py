"""GolemBase-specific SQLAlchemy types and type mapping."""

from sqlalchemy.types import TypeDecorator, String, Integer, DateTime, Boolean, Float, Text


class GolemBaseString(TypeDecorator):
    """GolemBase-specific string type."""
    
    impl = String
    cache_ok = True
    
    def process_bind_param(self, value, dialect):
        """Process values before sending to database."""
        if value is not None:
            return str(value)
        return value
    
    def process_result_value(self, value, dialect):
        """Process values coming from database."""
        if value is not None:
            return str(value)
        return value


class GolemBaseInteger(TypeDecorator):
    """GolemBase-specific integer type."""
    
    impl = Integer
    cache_ok = True


class GolemBaseFloat(TypeDecorator):
    """GolemBase-specific float type."""
    
    impl = Float
    cache_ok = True


class GolemBaseBoolean(TypeDecorator):
    """GolemBase-specific boolean type."""
    
    impl = Boolean
    cache_ok = True
    
    def process_bind_param(self, value, dialect):
        """Convert Python boolean to GolemBase boolean representation."""
        if value is None:
            return None
        return bool(value)
    
    def process_result_value(self, value, dialect):
        """Convert GolemBase boolean to Python boolean."""
        if value is None:
            return None
        # Handle different boolean representations GolemBase might use
        if isinstance(value, str):
            return value.lower() in ('true', '1', 'yes', 'on')
        return bool(value)


class GolemBaseDateTime(TypeDecorator):
    """GolemBase-specific datetime type."""
    
    impl = DateTime
    cache_ok = True


class GolemBaseText(TypeDecorator):
    """GolemBase-specific text type."""
    
    impl = Text
    cache_ok = True


class GolemBaseTypeMap:
    """Type mapping between GolemBase and SQLAlchemy types."""
    
    def __init__(self):
        """Initialize type mappings."""
        self.type_map = {
            # String types
            'VARCHAR': GolemBaseString,
            'CHAR': GolemBaseString,
            'STRING': GolemBaseString,
            'TEXT': GolemBaseText,
            
            # Numeric types
            'INT': GolemBaseInteger,
            'INTEGER': GolemBaseInteger,
            'BIGINT': GolemBaseInteger,
            'SMALLINT': GolemBaseInteger,
            'FLOAT': GolemBaseFloat,
            'DOUBLE': GolemBaseFloat,
            'REAL': GolemBaseFloat,
            
            # Boolean types
            'BOOLEAN': GolemBaseBoolean,
            'BOOL': GolemBaseBoolean,
            
            # Date/Time types
            'DATETIME': GolemBaseDateTime,
            'TIMESTAMP': GolemBaseDateTime,
            'DATE': GolemBaseDateTime,
            'TIME': GolemBaseDateTime,
        }
    
    def get_sqlalchemy_type(self, golembase_type_name):
        """Get SQLAlchemy type for GolemBase type name."""
        base_type = golembase_type_name.split('(')[0].upper()
        type_class = self.type_map.get(base_type, GolemBaseString)
        
        # Handle parameterized types (e.g., VARCHAR(255))
        if '(' in golembase_type_name and ')' in golembase_type_name:
            params_str = golembase_type_name.split('(')[1].rstrip(')')
            if ',' in params_str:
                # Handle types with multiple parameters (e.g., DECIMAL(10,2))
                params = [int(p.strip()) for p in params_str.split(',')]
                return type_class(*params)
            else:
                # Handle types with single parameter (e.g., VARCHAR(255))
                param = int(params_str)
                return type_class(param)
        
        return type_class()
    
    def get_golembase_type(self, sqlalchemy_type):
        """Get GolemBase type name for SQLAlchemy type."""
        type_name = type(sqlalchemy_type).__name__
        
        type_reverse_map = {
            'String': 'VARCHAR',
            'Text': 'TEXT',
            'Integer': 'INTEGER',
            'BigInteger': 'BIGINT',
            'SmallInteger': 'SMALLINT',
            'Float': 'FLOAT',
            'Numeric': 'DECIMAL',
            'Boolean': 'BOOLEAN',
            'DateTime': 'DATETIME',
            'Date': 'DATE',
            'Time': 'TIME',
        }
        
        return type_reverse_map.get(type_name, 'VARCHAR')