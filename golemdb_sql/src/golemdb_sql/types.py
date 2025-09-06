"""PEP 249 DB-API 2.0 type constructors and constants for GolemBase."""

import time
from datetime import date, datetime, time as time_obj
from typing import Any, Union, Tuple


# PEP 249 Type Objects and Constructors
# These are used to describe the types of columns in the database

class DBAPITypeObject:
    """Base class for DB-API type objects."""
    
    def __init__(self, *values: Any):
        """Initialize type object with possible values."""
        self.values = frozenset(values)
    
    def __eq__(self, other: Any) -> bool:
        """Check if other value is of this type."""
        return other in self.values
    
    def __hash__(self) -> int:
        """Make type object hashable."""
        return hash(self.values)
    
    def __repr__(self) -> str:
        """String representation."""
        return f"{self.__class__.__name__}({', '.join(map(repr, self.values))})"


# Standard DB-API 2.0 Type Objects
STRING = DBAPITypeObject("STRING", "VARCHAR", "CHAR", "TEXT")
BINARY = DBAPITypeObject("BINARY", "VARBINARY", "BLOB")  
NUMBER = DBAPITypeObject("NUMBER", "NUMERIC", "DECIMAL", "INT", "INTEGER", "FLOAT", "REAL", "DOUBLE")
DATETIME = DBAPITypeObject("DATETIME", "TIMESTAMP", "DATE", "TIME")
ROWID = DBAPITypeObject("ROWID")


def Date(year: int, month: int, day: int) -> date:
    """Construct a date value.
    
    Args:
        year: Year (e.g., 2023)
        month: Month (1-12)
        day: Day of month (1-31)
        
    Returns:
        datetime.date object
    """
    return date(year, month, day)


def Time(hour: int, minute: int, second: int) -> time_obj:
    """Construct a time value.
    
    Args:
        hour: Hour (0-23)
        minute: Minute (0-59) 
        second: Second (0-59)
        
    Returns:
        datetime.time object (precision up to seconds only)
    """
    return time_obj(hour, minute, second)


def Timestamp(year: int, month: int, day: int, hour: int, minute: int, second: int) -> datetime:
    """Construct a timestamp value.
    
    Args:
        year: Year (e.g., 2023)
        month: Month (1-12)
        day: Day of month (1-31)
        hour: Hour (0-23)
        minute: Minute (0-59)
        second: Second (0-59)
        
    Returns:
        datetime.datetime object (precision up to seconds only)
    """
    return datetime(year, month, day, hour, minute, second)


def DateFromTicks(ticks: float) -> date:
    """Construct a date value from Unix timestamp.
    
    Args:
        ticks: Unix timestamp (seconds since epoch)
        
    Returns:
        datetime.date object
    """
    return datetime.fromtimestamp(ticks).date()


def TimeFromTicks(ticks: float) -> time_obj:
    """Construct a time value from Unix timestamp.
    
    Args:
        ticks: Unix timestamp (seconds since epoch)
        
    Returns:
        datetime.time object
    """
    return datetime.fromtimestamp(ticks).time()


def TimestampFromTicks(ticks: float) -> datetime:
    """Construct a timestamp value from Unix timestamp.
    
    Args:
        ticks: Unix timestamp (seconds since epoch)
        
    Returns:
        datetime.datetime object
    """
    return datetime.fromtimestamp(ticks)


def Binary(data: Union[str, bytes]) -> bytes:
    """Construct a binary value.
    
    Args:
        data: Binary data as string or bytes
        
    Returns:
        bytes object
    """
    if isinstance(data, str):
        return data.encode('utf-8')
    elif isinstance(data, bytes):
        return data
    else:
        return bytes(data)


class _NULL:
    """Singleton class to represent SQL NULL values."""
    
    _instance = None
    
    def __new__(cls) -> '_NULL':
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __repr__(self) -> str:
        return 'NULL'
    
    def __str__(self) -> str:
        return 'NULL'
    
    def __bool__(self) -> bool:
        return False


# SQL NULL singleton
NULL = _NULL()


# GolemBase specific type mappings
# These map GolemBase column types to DB-API type objects
GOLEMBASE_TYPE_MAP = {
    # String types
    'VARCHAR': STRING,
    'CHAR': STRING, 
    'TEXT': STRING,
    'STRING': STRING,
    'CLOB': STRING,
    
    # Numeric types
    'INT': NUMBER,
    'INTEGER': NUMBER,
    'BIGINT': NUMBER,
    'SMALLINT': NUMBER,
    'TINYINT': NUMBER,
    'FLOAT': NUMBER,
    'DOUBLE': NUMBER,
    'REAL': NUMBER,
    'DECIMAL': NUMBER,
    'NUMERIC': NUMBER,
    
    # Date/time types
    'DATE': DATETIME,
    'TIME': DATETIME,
    'DATETIME': DATETIME,
    'TIMESTAMP': DATETIME,
    
    # Binary types
    'BLOB': BINARY,
    'BINARY': BINARY,
    'VARBINARY': BINARY,
    'BYTEA': BINARY,
    
    # Boolean (treated as number)
    'BOOLEAN': NUMBER,
    'BOOL': NUMBER,
    
    # Other types
    'ROWID': ROWID,
    'UUID': STRING,
    'JSON': STRING,
    'JSONB': STRING,
}


def get_type_object(golembase_type: str) -> DBAPITypeObject:
    """Get DB-API type object for GolemBase column type.
    
    Args:
        golembase_type: GolemBase column type name
        
    Returns:
        Appropriate DB-API type object
    """
    # Extract base type (remove size/precision info)
    base_type = golembase_type.split('(')[0].upper().strip()
    
    return GOLEMBASE_TYPE_MAP.get(base_type, STRING)


# Signed integer encoding for GolemBase uint64 numeric annotations
def encode_signed_to_uint64(value: int, bits: int = 64) -> int:
    """Encode signed integer to uint64 preserving ordering without high bit.
    
    Modified encoding strategy to avoid GolemBase "high bit set not supported" limitation:
    - Maps signed integers to range [0, 2^62] to avoid setting the high bit (bit 63)
    - Preserves ordering: negative < 0 < positive
    - Uses simple offset encoding without bit flipping
    
    Encoding formula: value + offset (where offset centers the range at 2^61)
    
    Args:
        value: Signed integer to encode
        bits: Number of bits (8, 16, 32, or 64)
        
    Returns:
        Encoded value as uint64 (guaranteed < 2^63 to avoid high bit)
        
    Raises:
        OverflowError: If value doesn't fit in specified bit width
    """
    if bits == 8:
        # TINYINT: -2^7 to 2^7-1 (-128 to 127) -> range of 256 values
        if not (-2**7 <= value < 2**7):
            raise OverflowError(f"Value {value} doesn't fit in 8-bit signed integer")
        # Map to range [0, 255] with center at 128
        return value + 2**7
    elif bits == 16:
        # SMALLINT: -2^15 to 2^15-1 (-32,768 to 32,767) -> range of 65536 values  
        if not (-2**15 <= value < 2**15):
            raise OverflowError(f"Value {value} doesn't fit in 16-bit signed integer")
        # Map to range [0, 65535] with center at 32768
        return value + 2**15
    elif bits == 32:
        # INTEGER: -2^31 to 2^31-1 -> range of ~4.3B values
        if not (-2**31 <= value < 2**31):
            raise OverflowError(f"Value {value} doesn't fit in 32-bit signed integer")
        # Map to range [0, 4294967295] with center at 2^31
        return value + 2**31
    elif bits == 64:
        # BIGINT: -2^63 to 2^63-1 -> full signed 64-bit range
        if not (-2**63 <= value < 2**63):
            raise OverflowError(f"Value {value} doesn't fit in 64-bit signed integer")
        # Special handling for 64-bit to avoid high bit:
        # We cannot use the full range due to GolemBase limitations
        # For now, limit to a safe range that doesn't set the high bit
        # This may not handle the full signed 64-bit range, but preserves ordering
        max_safe_input = 2**62 - 1  # Largest input that won't cause high bit when offset
        if value < -2**62 or value > max_safe_input:
            raise OverflowError(f"Value {value} exceeds safe range for GolemBase encoding")
        return value + 2**62
    else:
        raise ValueError(f"Unsupported bit width: {bits}")


def decode_uint64_to_signed(encoded_value: int, bits: int = 64) -> int:
    """Decode uint64 back to signed integer.
    
    Reverses the encoding applied by encode_signed_to_uint64.
    Uses simple offset subtraction to reverse the encoding.
    
    Args:
        encoded_value: Encoded value as uint64
        bits: Number of bits (8, 16, 32, or 64)
        
    Returns:
        Original signed integer value
    """
    if bits == 8:
        # Reverse 8-bit encoding: value + 2^7 -> value
        return encoded_value - 2**7
    elif bits == 16:
        # Reverse 16-bit encoding: value + 2^15 -> value  
        return encoded_value - 2**15
    elif bits == 32:
        # Reverse 32-bit encoding: value + 2^31 -> value
        return encoded_value - 2**31
    elif bits == 64:
        # Reverse 64-bit encoding: value + 2^62 -> value
        return encoded_value - 2**62
    else:
        raise ValueError(f"Unsupported bit width: {bits}")


def should_encode_as_signed_integer(golembase_type: str) -> bool:
    """Check if a column type should be encoded as signed integer.
    
    Args:
        golembase_type: GolemBase column type name
        
    Returns:
        True if should be encoded, False otherwise
    """
    base_type = golembase_type.split('(')[0].upper().strip()
    return base_type in ('INTEGER', 'INT', 'BIGINT', 'SMALLINT', 'TINYINT')


def get_integer_bit_width(golembase_type: str) -> int:
    """Get bit width for integer type.
    
    Args:
        golembase_type: GolemBase column type name
        
    Returns:
        Bit width (8, 16, 32, or 64)
    """
    base_type = golembase_type.split('(')[0].upper().strip()
    if base_type == 'BIGINT':
        return 64
    elif base_type in ('INTEGER', 'INT'):
        return 32
    elif base_type == 'SMALLINT':
        return 16
    elif base_type == 'TINYINT':
        return 8
    else:
        return 64  # Default


def encode_decimal_for_string_ordering(value, precision: int = 18, scale: int = 6) -> str:
    """Encode decimal value for lexicographic string ordering based on SQL92 DECIMAL(precision, scale).
    
    Positive numbers: prefix with '.' and pad with leading zeros
    Negative numbers: prefix with '-' and invert digits (0→9, 1→8, etc.)
    
    This ensures string ordering matches numeric ordering.
    
    Args:
        value: Decimal value (can be Decimal, float, or string)
        precision: Total number of significant digits (SQL92 DECIMAL precision)
        scale: Number of digits after decimal point (SQL92 DECIMAL scale)
        
    Returns:
        String encoded for lexicographic ordering
        
    Raises:
        ValueError: If value doesn't fit within specified precision/scale
    """
    from decimal import Decimal
    
    # Convert to Decimal for precise handling
    if isinstance(value, Decimal):
        dec_value = value
    elif isinstance(value, str):
        dec_value = Decimal(value)
    else:
        dec_value = Decimal(str(value))
    
    # Handle sign
    is_negative = dec_value < 0
    abs_value = abs(dec_value)
    
    # Calculate max value for given precision/scale
    integer_digits = precision - scale
    max_integer_part = 10 ** integer_digits - 1
    
    # Validate precision constraints
    if abs_value >= 10 ** precision:
        raise ValueError(f"Value {value} exceeds DECIMAL({precision},{scale}) precision")
    
    # Convert to string with exact scale
    abs_str = format(abs_value, f'.{scale}f')
    
    # Split on decimal point
    if '.' in abs_str:
        integer_part, fractional_part = abs_str.split('.')
    else:
        integer_part, fractional_part = abs_str, '0' * scale
    
    # Ensure fractional part has exactly 'scale' digits
    fractional_part = fractional_part.ljust(scale, '0')[:scale]
    
    # Pad integer part to exactly 'integer_digits' digits
    integer_part = integer_part.zfill(integer_digits)
    
    # Validate that integer part fits
    if len(integer_part) > integer_digits:
        raise ValueError(f"Value {value} integer part too large for DECIMAL({precision},{scale})")
    
    # Combine parts
    if scale > 0:
        formatted = f"{integer_part}.{fractional_part}"
    else:
        formatted = integer_part  # No decimal point for scale=0
    
    if is_negative:
        # For negative numbers: invert digits and prefix with '-'
        inverted = ''.join('9876543210'[int(c)] if c.isdigit() else c for c in formatted)
        return f"-{inverted}"
    else:
        # For positive numbers: prefix with '.'
        return f".{formatted}"


def decode_decimal_from_string_ordering(encoded_str: str):
    """Decode decimal value from lexicographic string encoding.
    
    Reverses the encoding applied by encode_decimal_for_string_ordering.
    
    Args:
        encoded_str: String encoded for lexicographic ordering
        
    Returns:
        Decimal value
    """
    from decimal import Decimal
    
    if encoded_str.startswith('-'):
        # Negative number: remove prefix and invert digits
        inverted_str = encoded_str[1:]  # Remove '-' prefix
        # Invert digits back: 9→0, 8→1, etc.
        original_str = ''.join('0123456789'[9-int(c)] if c.isdigit() else c for c in inverted_str)
        return -Decimal(original_str)
    elif encoded_str.startswith('.'):
        # Positive number: remove prefix
        return Decimal(encoded_str[1:])
    else:
        raise ValueError(f"Invalid encoded decimal string: {encoded_str}")


def get_decimal_precision_scale(column_type: str) -> Tuple[int, int]:
    """Extract precision and scale from DECIMAL column type string.
    
    Args:
        column_type: Column type string like 'DECIMAL(10,2)'
        
    Returns:
        Tuple of (precision, scale) with defaults if not specified
    """
    from .schema_manager import parse_column_type
    
    base_type, precision, scale, _ = parse_column_type(column_type)
    
    # Default values per SQL92 standard
    if base_type in ('DECIMAL', 'NUMERIC', 'NUMBER'):
        precision = precision or 18  # Default precision
        scale = scale or 0          # Default scale
        return (precision, scale)
    else:
        raise ValueError(f"Not a decimal type: {column_type}")


def convert_golembase_value(value: Any, golembase_type: str) -> Any:
    """Convert a value from GolemBase to appropriate Python type.
    
    Args:
        value: Raw value from GolemBase
        golembase_type: GolemBase column type
        
    Returns:
        Value converted to appropriate Python type
    """
    if value is None:
        return None
        
    base_type = golembase_type.split('(')[0].upper().strip()
    
    # Date/time conversions
    if base_type in ('DATE', 'TIME', 'DATETIME', 'TIMESTAMP'):
        if isinstance(value, str):
            # Parse string representation
            try:
                if base_type == 'DATE':
                    return datetime.strptime(value, '%Y-%m-%d').date()
                elif base_type == 'TIME':
                    return datetime.strptime(value, '%H:%M:%S').time()
                else:  # DATETIME, TIMESTAMP
                    # Try different timestamp formats
                    for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%d %H:%M:%S.%f'):
                        try:
                            return datetime.strptime(value, fmt)
                        except ValueError:
                            continue
                    return value  # Return as-is if parsing fails
            except ValueError:
                return value
        else:
            return value  # Assume already in correct format
    
    # Numeric conversions
    elif base_type in ('INT', 'INTEGER', 'BIGINT', 'SMALLINT', 'TINYINT'):
        if isinstance(value, str):
            try:
                return int(value)
            except ValueError:
                return value
        else:
            return value
            
    elif base_type in ('FLOAT', 'DOUBLE', 'REAL', 'DECIMAL', 'NUMERIC'):
        if isinstance(value, str):
            try:
                return float(value)
            except ValueError:
                return value
        else:
            return value
    
    # Boolean conversions
    elif base_type in ('BOOLEAN', 'BOOL'):
        if isinstance(value, str):
            return value.lower() in ('true', '1', 'yes', 'on', 't', 'y')
        else:
            return bool(value)
    
    # Binary conversions
    elif base_type in ('BLOB', 'BINARY', 'VARBINARY', 'BYTEA'):
        if isinstance(value, str):
            return value.encode('utf-8')
        else:
            return value
    
    # Default: return as-is
    else:
        return value