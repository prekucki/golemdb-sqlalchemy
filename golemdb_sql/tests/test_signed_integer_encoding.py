"""Test signed integer encoding for GolemDB uint64 numeric annotations."""

import pytest
from golemdb_sql.types import (
    encode_signed_to_uint64,
    decode_uint64_to_signed,
    should_encode_as_signed_integer,
    get_integer_bit_width
)


class TestSignedIntegerEncoding:
    """Test signed integer encoding utilities."""
    
    def test_encode_signed_32bit_zero(self):
        """Test encoding zero for 32-bit integers."""
        result = encode_signed_to_uint64(0, 32)
        # New GolemBase-compatible encoding: 0 + 2^31 = 2^31
        expected = 2**31
        assert result == expected
    
    def test_encode_signed_32bit_positive(self):
        """Test encoding positive values for 32-bit integers."""
        # Test 1
        result = encode_signed_to_uint64(1, 32)
        expected = 1 + 2**31
        assert result == expected
        
        # Test max positive 32-bit value
        max_32_pos = 2**31 - 1
        result = encode_signed_to_uint64(max_32_pos, 32)
        expected = max_32_pos + 2**31
        assert result == expected
    
    def test_encode_signed_32bit_negative(self):
        """Test encoding negative values for 32-bit integers."""
        # Test -1
        result = encode_signed_to_uint64(-1, 32)
        expected = -1 + 2**31
        assert result == expected
        
        # Test min negative 32-bit value
        min_32_neg = -2**31
        result = encode_signed_to_uint64(min_32_neg, 32)
        expected = min_32_neg + 2**31
        assert result == expected
    
    def test_encode_signed_64bit_zero(self):
        """Test encoding zero for 64-bit integers."""
        result = encode_signed_to_uint64(0, 64)
        # New GolemBase-compatible encoding: 0 + 2^62 = 2^62
        expected = 2**62
        assert result == expected
    
    def test_encode_signed_64bit_positive(self):
        """Test encoding positive values for 64-bit integers."""
        # Test 1
        result = encode_signed_to_uint64(1, 64)
        expected = 1 + 2**62
        assert result == expected
        
        # Test large positive value (within safe range)
        large_pos = 2**61
        result = encode_signed_to_uint64(large_pos, 64)
        expected = large_pos + 2**62
        assert result == expected
    
    def test_encode_signed_64bit_negative(self):
        """Test encoding negative values for 64-bit integers."""
        # Test -1
        result = encode_signed_to_uint64(-1, 64)
        expected = -1 + 2**62
        assert result == expected
        
        # Test -2
        result = encode_signed_to_uint64(-2, 64)
        expected = -2 + 2**62
        assert result == expected
    
    def test_decode_roundtrip_32bit(self):
        """Test that encode/decode roundtrip works for 32-bit values."""
        test_values = [-2**31, -100, -1, 0, 1, 100, 2**31 - 1]
        
        for value in test_values:
            encoded = encode_signed_to_uint64(value, 32)
            decoded = decode_uint64_to_signed(encoded, 32)
            assert decoded == value, f"Failed roundtrip for {value}: encoded={encoded}, decoded={decoded}"
    
    def test_decode_roundtrip_64bit(self):
        """Test that encode/decode roundtrip works for 64-bit values."""
        # Use safe range values that work with GolemBase constraints
        test_values = [-2**62, -2**32, -1, 0, 1, 2**32, 2**62 - 1]
        
        for value in test_values:
            encoded = encode_signed_to_uint64(value, 64)
            decoded = decode_uint64_to_signed(encoded, 64)
            assert decoded == value, f"Failed roundtrip for {value}: encoded={encoded}, decoded={decoded}"
    
    def test_ordering_preservation_32bit(self):
        """Test that ordering is preserved for 32-bit values."""
        values = [-100, -10, -1, 0, 1, 10, 100]
        encoded_values = [encode_signed_to_uint64(v, 32) for v in values]
        
        # Check that encoded values are in ascending order
        for i in range(len(encoded_values) - 1):
            assert encoded_values[i] < encoded_values[i + 1], \
                f"Ordering not preserved: {values[i]} ({encoded_values[i]}) >= {values[i+1]} ({encoded_values[i+1]})"
    
    def test_ordering_preservation_64bit(self):
        """Test that ordering is preserved for 64-bit values."""
        values = [-1000, -100, -10, -1, 0, 1, 10, 100, 1000]
        encoded_values = [encode_signed_to_uint64(v, 64) for v in values]
        
        # Check that encoded values are in ascending order
        for i in range(len(encoded_values) - 1):
            assert encoded_values[i] < encoded_values[i + 1], \
                f"Ordering not preserved: {values[i]} ({encoded_values[i]}) >= {values[i+1]} ({encoded_values[i+1]})"
    
    def test_range_boundaries_32bit(self):
        """Test encoding at 32-bit range boundaries."""
        # Test min value
        min_val = -2**31
        encoded = encode_signed_to_uint64(min_val, 32)
        decoded = decode_uint64_to_signed(encoded, 32)
        assert decoded == min_val
        
        # Test max value
        max_val = 2**31 - 1
        encoded = encode_signed_to_uint64(max_val, 32)
        decoded = decode_uint64_to_signed(encoded, 32)
        assert decoded == max_val
        
        # Test overflow
        with pytest.raises(OverflowError):
            encode_signed_to_uint64(2**31, 32)
        
        with pytest.raises(OverflowError):
            encode_signed_to_uint64(-2**31 - 1, 32)
    
    def test_range_boundaries_64bit(self):
        """Test encoding at 64-bit range boundaries (GolemBase safe range)."""
        # Test min safe value
        min_val = -2**62
        encoded = encode_signed_to_uint64(min_val, 64)
        decoded = decode_uint64_to_signed(encoded, 64)
        assert decoded == min_val
        
        # Test max safe value
        max_val = 2**62 - 1
        encoded = encode_signed_to_uint64(max_val, 64)
        decoded = decode_uint64_to_signed(encoded, 64)
        assert decoded == max_val
        
        # Test overflow beyond safe range
        with pytest.raises(OverflowError):
            encode_signed_to_uint64(2**62, 64)
        
        with pytest.raises(OverflowError):
            encode_signed_to_uint64(-2**62 - 1, 64)
    
    def test_encode_signed_8bit(self):
        """Test encoding 8-bit (TINYINT) values."""
        # Test zero
        result = encode_signed_to_uint64(0, 8)
        expected = 2**7  # 0 + 128
        assert result == expected
        
        # Test positive
        result = encode_signed_to_uint64(127, 8)  # Max TINYINT
        expected = 127 + 2**7
        assert result == expected
        
        # Test negative
        result = encode_signed_to_uint64(-128, 8)  # Min TINYINT
        expected = -128 + 2**7
        assert result == expected
        
        # Test roundtrip
        for val in [-128, -1, 0, 1, 127]:
            encoded = encode_signed_to_uint64(val, 8)
            decoded = decode_uint64_to_signed(encoded, 8)
            assert decoded == val
    
    def test_encode_signed_16bit(self):
        """Test encoding 16-bit (SMALLINT) values."""
        # Test zero
        result = encode_signed_to_uint64(0, 16)
        expected = 2**15  # 0 + 32768
        assert result == expected
        
        # Test positive
        result = encode_signed_to_uint64(32767, 16)  # Max SMALLINT
        expected = 32767 + 2**15
        assert result == expected
        
        # Test negative
        result = encode_signed_to_uint64(-32768, 16)  # Min SMALLINT
        expected = -32768 + 2**15
        assert result == expected
        
        # Test roundtrip
        for val in [-32768, -1, 0, 1, 32767]:
            encoded = encode_signed_to_uint64(val, 16)
            decoded = decode_uint64_to_signed(encoded, 16)
            assert decoded == val
    
    def test_ordering_preservation_8bit(self):
        """Test that ordering is preserved for 8-bit values."""
        values = [-128, -10, -1, 0, 1, 10, 127]
        encoded_values = [encode_signed_to_uint64(v, 8) for v in values]
        
        # Check that encoded values are in ascending order
        for i in range(len(encoded_values) - 1):
            assert encoded_values[i] < encoded_values[i + 1], \
                f"Ordering not preserved: {values[i]} ({encoded_values[i]}) >= {values[i+1]} ({encoded_values[i+1]})"
    
    def test_ordering_preservation_16bit(self):
        """Test that ordering is preserved for 16-bit values."""
        values = [-32768, -1000, -1, 0, 1, 1000, 32767]
        encoded_values = [encode_signed_to_uint64(v, 16) for v in values]
        
        # Check that encoded values are in ascending order
        for i in range(len(encoded_values) - 1):
            assert encoded_values[i] < encoded_values[i + 1], \
                f"Ordering not preserved: {values[i]} ({encoded_values[i]}) >= {values[i+1]} ({encoded_values[i+1]})"

    def test_unsupported_bit_width(self):
        """Test error handling for unsupported bit widths."""
        with pytest.raises(ValueError, match="Unsupported bit width"):
            encode_signed_to_uint64(0, 12)  # Unsupported width
        
        with pytest.raises(ValueError, match="Unsupported bit width"):
            decode_uint64_to_signed(0, 12)  # Unsupported width
    
    def test_should_encode_as_signed_integer(self):
        """Test should_encode_as_signed_integer function."""
        assert should_encode_as_signed_integer('INTEGER') is True
        assert should_encode_as_signed_integer('INT') is True
        assert should_encode_as_signed_integer('BIGINT') is True
        assert should_encode_as_signed_integer('SMALLINT') is True
        assert should_encode_as_signed_integer('TINYINT') is True
        assert should_encode_as_signed_integer('INTEGER(10)') is True
        assert should_encode_as_signed_integer('bigint') is True  # Case insensitive
        
        assert should_encode_as_signed_integer('VARCHAR') is False
        assert should_encode_as_signed_integer('FLOAT') is False
    
    def test_get_integer_bit_width(self):
        """Test get_integer_bit_width function."""
        assert get_integer_bit_width('TINYINT') == 8
        assert get_integer_bit_width('tinyint') == 8  # Case insensitive
        
        assert get_integer_bit_width('SMALLINT') == 16
        assert get_integer_bit_width('smallint') == 16
        
        assert get_integer_bit_width('INTEGER') == 32
        assert get_integer_bit_width('INT') == 32
        assert get_integer_bit_width('INTEGER(10)') == 32
        assert get_integer_bit_width('int') == 32  # Case insensitive
        
        assert get_integer_bit_width('BIGINT') == 64
        assert get_integer_bit_width('bigint') == 64
        
        # Default for unknown types
        assert get_integer_bit_width('UNKNOWN') == 64
        assert get_integer_bit_width('VARCHAR') == 64


class TestSignedIntegerIntegration:
    """Integration tests for signed integer encoding with schema manager and query translator."""
    
    @pytest.fixture
    def schema_manager(self):
        """Create schema manager for testing."""
        from golemdb_sql.schema_manager import SchemaManager, TableDefinition, ColumnDefinition
        
        sm = SchemaManager("test_signed_encoding")
        
        # Create test table with signed integer columns
        table_def = TableDefinition(
            name="test_table",
            columns=[
                ColumnDefinition(name="id", type="INTEGER", primary_key=True),
                ColumnDefinition(name="big_id", type="BIGINT", indexed=True),  # Make indexed so it goes to annotations
                ColumnDefinition(name="small_id", type="SMALLINT", indexed=True),  # Make indexed so it goes to annotations
                ColumnDefinition(name="tiny_id", type="TINYINT", indexed=True),  # Make indexed so it goes to annotations
                ColumnDefinition(name="name", type="VARCHAR(50)")
            ],
            indexes=[],
            foreign_keys=[]
        )
        sm.add_table(table_def)
        return sm
    
    def test_schema_manager_annotation_encoding(self, schema_manager):
        """Test that schema manager applies signed integer encoding to annotations."""
        row_data = {
            "id": -123,
            "big_id": -4611686018427387904,  # Safe 64-bit value (min for our encoding)
            "small_id": -100,
            "tiny_id": -50,
            "name": "test"
        }
        
        annotations = schema_manager.get_entity_annotations_for_table("test_table", row_data)
        
        # Check that INTEGER column is encoded (with idx_ prefix)
        encoded_id = encode_signed_to_uint64(-123, 32)
        assert annotations["numeric_annotations"]["idx_id"] == encoded_id
        
        # Check that BIGINT column is encoded (with idx_ prefix)
        encoded_big_id = encode_signed_to_uint64(-4611686018427387904, 64)
        assert annotations["numeric_annotations"]["idx_big_id"] == encoded_big_id
        
        # Check that SMALLINT column is encoded (with idx_ prefix)
        encoded_small_id = encode_signed_to_uint64(-100, 16)
        assert annotations["numeric_annotations"]["idx_small_id"] == encoded_small_id
        
        # Check that TINYINT column is encoded (with idx_ prefix)
        encoded_tiny_id = encode_signed_to_uint64(-50, 8)
        assert annotations["numeric_annotations"]["idx_tiny_id"] == encoded_tiny_id
        
        # Check that non-indexed string columns are not in annotations
        # (Only indexed columns and metadata annotations should be present)
        assert "name" not in annotations["string_annotations"]
        
        # Check metadata annotations
        assert annotations["string_annotations"]["row_type"] == "json"
        assert annotations["string_annotations"]["relation"] == "default.test_table"
    
    def test_query_translator_encoding(self, schema_manager):
        """Test that query translator applies signed integer encoding to query conditions."""
        from golemdb_sql.query_translator import QueryTranslator
        
        translator = QueryTranslator(schema_manager)
        
        # Test INTEGER column query
        result = translator.translate_select("SELECT * FROM test_table WHERE id = -50")
        encoded_value = encode_signed_to_uint64(-50, 32)
        assert f"id={encoded_value}" in result.golem_query
        
        # Test BIGINT column query
        result = translator.translate_select("SELECT * FROM test_table WHERE big_id > -1000")
        encoded_value = encode_signed_to_uint64(-1000, 64)
        assert f"big_id>{encoded_value}" in result.golem_query
        
        # Test SMALLINT column query (should be encoded)
        result = translator.translate_select("SELECT * FROM test_table WHERE small_id = -25")
        encoded_value = encode_signed_to_uint64(-25, 16)
        assert f"small_id={encoded_value}" in result.golem_query
        
        # Test TINYINT column query (should be encoded)
        result = translator.translate_select("SELECT * FROM test_table WHERE tiny_id = -10")
        encoded_value = encode_signed_to_uint64(-10, 8)
        assert f"tiny_id={encoded_value}" in result.golem_query
    
    def test_complex_query_encoding(self, schema_manager):
        """Test encoding in complex queries with multiple conditions."""
        from golemdb_sql.query_translator import QueryTranslator
        
        translator = QueryTranslator(schema_manager)
        
        # Test complex WHERE clause
        result = translator.translate_select(
            "SELECT * FROM test_table WHERE id >= -100 AND big_id < 500 AND small_id = -10 AND tiny_id > -5"
        )
        
        # Check that all integer values are encoded
        encoded_id = encode_signed_to_uint64(-100, 32)
        encoded_big_id = encode_signed_to_uint64(500, 64)
        encoded_small_id = encode_signed_to_uint64(-10, 16)
        encoded_tiny_id = encode_signed_to_uint64(-5, 8)
        
        query = result.golem_query
        assert f"id>={encoded_id}" in query
        assert f"big_id<{encoded_big_id}" in query
        assert f"small_id={encoded_small_id}" in query
        assert f"tiny_id>{encoded_tiny_id}" in query