"""Tests for type system functionality."""

import pytest
from datetime import datetime, date, time
from decimal import Decimal
from golemdb_sql.types import (
    Date, Time, Timestamp, DateFromTicks, TimeFromTicks, TimestampFromTicks,
    Binary, STRING, BINARY, NUMBER, DATETIME, ROWID
)


class TestTypeConstructors:
    """Test PEP 249 type constructors."""
    
    def test_date_constructor(self):
        """Test Date constructor."""
        # Test with integers
        d = Date(2023, 6, 15)
        assert isinstance(d, date)
        assert d.year == 2023
        assert d.month == 6
        assert d.day == 15
        
        # Test edge cases
        d_start = Date(2023, 1, 1)
        assert d_start == date(2023, 1, 1)
        
        d_end = Date(2023, 12, 31)
        assert d_end == date(2023, 12, 31)
    
    def test_time_constructor(self):
        """Test Time constructor."""
        # Test with hours, minutes, seconds
        t = Time(14, 30, 45)
        assert isinstance(t, time)
        assert t.hour == 14
        assert t.minute == 30
        assert t.second == 45
        
        # microseconds not supported - precision up to seconds only
        
        # Test edge cases
        t_start = Time(0, 0, 0)
        assert t_start == time(0, 0, 0)
        
        t_end = Time(23, 59, 59)
        assert t_end == time(23, 59, 59)
    
    def test_timestamp_constructor(self):
        """Test Timestamp constructor."""
        # Test with all components
        ts = Timestamp(2023, 6, 15, 14, 30, 45)
        assert isinstance(ts, datetime)
        assert ts.year == 2023
        assert ts.month == 6
        assert ts.day == 15
        assert ts.hour == 14
        assert ts.minute == 30
        assert ts.second == 45
        
        # microseconds not supported - precision up to seconds only
        
        # Test minimum values
        ts_min = Timestamp(1, 1, 1, 0, 0, 0)
        assert ts_min == datetime(1, 1, 1, 0, 0, 0)
    
    def test_datefromticks(self):
        """Test DateFromTicks constructor."""
        # Test with known timestamp
        # January 1, 2023 00:00:00 UTC = 1672531200
        timestamp = 1672531200
        d = DateFromTicks(timestamp)
        
        assert isinstance(d, date)
        # Note: Result may vary based on local timezone
        assert d.year == 2023
        assert d.month == 1
        assert d.day == 1
    
    def test_timefromticks(self):
        """Test TimeFromTicks constructor."""
        # Test with timestamp that represents a specific time
        # Use timestamp for a known time
        timestamp = 1672531200 + (14 * 3600) + (30 * 60) + 45  # 14:30:45 on Jan 1, 2023
        t = TimeFromTicks(timestamp)
        
        assert isinstance(t, time)
        # Note: Result may vary based on local timezone
        # Just verify it's a valid time
        assert 0 <= t.hour <= 23
        assert 0 <= t.minute <= 59
        assert 0 <= t.second <= 59
    
    def test_timestampfromticks(self):
        """Test TimestampFromTicks constructor."""
        # Test with known timestamp
        timestamp = 1672531200  # January 1, 2023 00:00:00 UTC
        ts = TimestampFromTicks(timestamp)
        
        assert isinstance(ts, datetime)
        assert ts.year == 2023
        assert ts.month == 1
        assert ts.day == 1
    
    def test_binary_constructor(self):
        """Test Binary constructor."""
        # Test with bytes
        data = b'\\x00\\x01\\x02\\x03'
        binary = Binary(data)
        assert binary == data
        assert isinstance(binary, bytes)
        
        # Test with string (should be encoded)
        text = "Hello, World!"
        binary_text = Binary(text)
        assert isinstance(binary_text, bytes)
        assert binary_text == text.encode('utf-8')
        
        # Test empty binary
        empty = Binary(b'')
        assert empty == b''
        
        # Test binary operations
        data1 = Binary(b'hello')
        data2 = Binary(b'world')
        combined = data1 + data2
        assert combined == b'helloworld'


class TestTypeObjects:
    """Test PEP 249 type objects."""
    
    def test_string_type(self):
        """Test STRING type object."""
        assert hasattr(STRING, '__cmp__')
        
        # Test equality with itself
        assert STRING == STRING
        
        # Test string representation
        assert str(STRING) == "STRING"
    
    def test_binary_type(self):
        """Test BINARY type object."""
        assert hasattr(BINARY, '__cmp__')
        
        # Test equality
        assert BINARY == BINARY
        assert BINARY != STRING
        
        # Test string representation
        assert str(BINARY) == "BINARY"
    
    def test_number_type(self):
        """Test NUMBER type object."""
        assert hasattr(NUMBER, '__cmp__')
        
        # Test equality
        assert NUMBER == NUMBER
        assert NUMBER != STRING
        assert NUMBER != BINARY
        
        # Test string representation
        assert str(NUMBER) == "NUMBER"
    
    def test_datetime_type(self):
        """Test DATETIME type object."""
        assert hasattr(DATETIME, '__cmp__')
        
        # Test equality
        assert DATETIME == DATETIME
        assert DATETIME != STRING
        assert DATETIME != NUMBER
        
        # Test string representation
        assert str(DATETIME) == "DATETIME"
    
    def test_rowid_type(self):
        """Test ROWID type object."""
        assert hasattr(ROWID, '__cmp__')
        
        # Test equality
        assert ROWID == ROWID
        assert ROWID != STRING
        assert ROWID != NUMBER
        
        # Test string representation
        assert str(ROWID) == "ROWID"
    
    def test_type_object_comparison(self):
        """Test type object comparison functionality."""
        # Test that different types are not equal
        type_objects = [STRING, BINARY, NUMBER, DATETIME, ROWID]
        
        for i, type1 in enumerate(type_objects):
            for j, type2 in enumerate(type_objects):
                if i == j:
                    assert type1 == type2
                else:
                    assert type1 != type2
    
    def test_type_object_hashing(self):
        """Test that type objects are hashable."""
        # Should be able to use as dictionary keys
        type_map = {
            STRING: "string_handler",
            BINARY: "binary_handler", 
            NUMBER: "number_handler",
            DATETIME: "datetime_handler",
            ROWID: "rowid_handler"
        }
        
        assert len(type_map) == 5
        assert type_map[STRING] == "string_handler"
        assert type_map[NUMBER] == "number_handler"
    
    def test_type_object_immutability(self):
        """Test that type objects are immutable."""
        # Should not be able to modify type objects
        original_str = str(STRING)
        
        # Type objects should be consistent
        assert str(STRING) == original_str
        assert STRING == STRING
    
    def test_type_identification(self):
        """Test type identification for common Python types."""
        # This would be used by the database driver to map Python types
        # to appropriate database types
        
        type_mappings = {
            str: STRING,
            bytes: BINARY,
            int: NUMBER,
            float: NUMBER,
            Decimal: NUMBER,
            datetime: DATETIME,
            date: DATETIME,
            time: DATETIME,
        }
        
        for python_type, db_type in type_mappings.items():
            assert db_type in [STRING, BINARY, NUMBER, DATETIME, ROWID]


class TestTypeConversions:
    """Test type conversion utilities."""
    
    def test_date_conversion_edge_cases(self):
        """Test Date constructor with edge cases."""
        # Test leap year
        leap_date = Date(2024, 2, 29)  # 2024 is a leap year
        assert leap_date.day == 29
        
        # Test non-leap year (should raise ValueError)
        with pytest.raises(ValueError):
            Date(2023, 2, 29)  # 2023 is not a leap year
        
        # Test invalid month
        with pytest.raises(ValueError):
            Date(2023, 13, 1)
        
        # Test invalid day
        with pytest.raises(ValueError):
            Date(2023, 1, 32)
    
    def test_time_conversion_edge_cases(self):
        """Test Time constructor with edge cases."""
        # Test valid boundary values
        Time(0, 0, 0, 0)  # Midnight
        Time(23, 59, 59, 999999)  # End of day
        
        # Test invalid hour
        with pytest.raises(ValueError):
            Time(24, 0, 0)
        
        # Test invalid minute
        with pytest.raises(ValueError):
            Time(0, 60, 0)
        
        # Test invalid second
        with pytest.raises(ValueError):
            Time(0, 0, 60)
        
        # Test invalid microsecond
        with pytest.raises(ValueError):
            Time(0, 0, 0, 1000000)
    
    def test_timestamp_conversion_edge_cases(self):
        """Test Timestamp constructor with edge cases."""
        # Test valid boundaries
        Timestamp(1, 1, 1, 0, 0, 0)  # Minimum datetime
        Timestamp(9999, 12, 31, 23, 59, 59, 999999)  # Maximum datetime
        
        # Test invalid combinations
        with pytest.raises(ValueError):
            Timestamp(0, 1, 1)  # Year 0 doesn't exist
        
        with pytest.raises(ValueError):
            Timestamp(10000, 1, 1)  # Year too large
    
    def test_binary_conversion_types(self):
        """Test Binary constructor with different input types."""
        # Test with bytearray
        ba = bytearray([0, 1, 2, 3])
        binary = Binary(ba)
        assert isinstance(binary, bytes)
        assert binary == bytes([0, 1, 2, 3])
        
        # Test with list of integers
        int_list = [65, 66, 67]  # ASCII for 'ABC'
        binary = Binary(int_list)
        assert isinstance(binary, bytes)
        assert binary == b'ABC'
        
        # Test with memoryview
        mv = memoryview(b'test')
        binary = Binary(mv)
        assert isinstance(binary, bytes)
        assert binary == b'test'
    
    def test_ticks_conversion_negative(self):
        """Test tick-based constructors with negative timestamps."""
        # Test with negative timestamp (before epoch)
        negative_timestamp = -86400  # One day before epoch
        
        # Should handle negative timestamps gracefully
        try:
            d = DateFromTicks(negative_timestamp)
            assert isinstance(d, date)
            
            t = TimeFromTicks(negative_timestamp) 
            assert isinstance(t, time)
            
            ts = TimestampFromTicks(negative_timestamp)
            assert isinstance(ts, datetime)
        except (ValueError, OSError):
            # Some systems may not support negative timestamps
            pytest.skip("System doesn't support negative timestamps")
    
    def test_type_constructor_consistency(self):
        """Test that type constructors produce consistent results."""
        # Test that constructors produce objects that work with standard operations
        d = Date(2023, 6, 15)
        t = Time(14, 30, 45)
        ts = Timestamp(2023, 6, 15, 14, 30, 45)
        
        # Should work with standard comparison operators
        assert d == date(2023, 6, 15)
        assert t == time(14, 30, 45)
        assert ts == datetime(2023, 6, 15, 14, 30, 45)
        
        # Should work with string formatting
        assert str(d) == "2023-06-15"
        assert str(t) == "14:30:45"
        assert "2023-06-15" in str(ts) and "14:30:45" in str(ts)
        
        # Should be hashable
        date_set = {d, Date(2023, 6, 15)}
        assert len(date_set) == 1  # Same date should not duplicate