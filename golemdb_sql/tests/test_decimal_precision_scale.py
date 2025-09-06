"""Test DECIMAL precision and scale handling according to SQL92 standard."""

import pytest
from decimal import Decimal
from golemdb_sql.schema_manager import (
    SchemaManager, TableDefinition, ColumnDefinition, parse_column_type
)
from golemdb_sql.types import (
    encode_decimal_for_string_ordering,
    decode_decimal_from_string_ordering,
    get_decimal_precision_scale
)
from golemdb_sql.query_translator import QueryTranslator


class TestColumnTypeParsing:
    """Test parsing of SQL column type strings."""
    
    def test_parse_decimal_types(self):
        """Test parsing various DECIMAL type formats."""
        # DECIMAL with precision and scale
        base_type, precision, scale, length = parse_column_type("DECIMAL(10,2)")
        assert base_type == "DECIMAL"
        assert precision == 10
        assert scale == 2
        assert length is None
        
        # NUMERIC with precision only
        base_type, precision, scale, length = parse_column_type("NUMERIC(8)")
        assert base_type == "NUMERIC"
        assert precision == 8
        assert scale is None
        assert length is None
        
        # NUMBER Oracle style
        base_type, precision, scale, length = parse_column_type("NUMBER(12,4)")
        assert base_type == "NUMBER"
        assert precision == 12
        assert scale == 4
        assert length is None
        
        # Plain DECIMAL
        base_type, precision, scale, length = parse_column_type("DECIMAL")
        assert base_type == "DECIMAL"
        assert precision is None
        assert scale is None
        assert length is None
    
    def test_parse_varchar_types(self):
        """Test parsing VARCHAR types (should use length, not precision)."""
        base_type, precision, scale, length = parse_column_type("VARCHAR(50)")
        assert base_type == "VARCHAR"
        assert precision is None
        assert scale is None
        assert length == 50
        
        base_type, precision, scale, length = parse_column_type("CHAR(10)")
        assert base_type == "CHAR"
        assert precision is None
        assert scale is None  
        assert length == 10
    
    def test_parse_other_types(self):
        """Test parsing other types (should ignore parameters)."""
        base_type, precision, scale, length = parse_column_type("INTEGER")
        assert base_type == "INTEGER"
        assert precision is None
        assert scale is None
        assert length is None


class TestDecimalStringEncoding:
    """Test decimal string encoding for lexicographic ordering."""
    
    def test_encoding_positive_numbers(self):
        """Test encoding positive decimal numbers."""
        # Test DECIMAL(8,2): 6 digits before decimal, 2 after
        encoded = encode_decimal_for_string_ordering("123.45", precision=8, scale=2)
        assert encoded == ".000123.45"
        
        # Test with different precision/scale
        encoded = encode_decimal_for_string_ordering("99.9", precision=5, scale=1)
        assert encoded == ".0099.9"
        
        # Test integer (scale=0)
        encoded = encode_decimal_for_string_ordering("12345", precision=6, scale=0)
        assert encoded == ".012345"
    
    def test_encoding_negative_numbers(self):
        """Test encoding negative decimal numbers with digit inversion."""
        # Test DECIMAL(8,2): -123.45 should invert digits
        encoded = encode_decimal_for_string_ordering("-123.45", precision=8, scale=2)
        # 000123.45 -> 999876.54 (digit inversion) with '-' prefix
        assert encoded == "-999876.54"
        
        # Test -99.9
        encoded = encode_decimal_for_string_ordering("-99.9", precision=5, scale=1)
        assert encoded == "-9900.0"  # 0099.9 -> 9900.0
    
    def test_encoding_zero(self):
        """Test encoding zero."""
        encoded = encode_decimal_for_string_ordering("0", precision=6, scale=2)
        assert encoded == ".0000.00"
        
        encoded = encode_decimal_for_string_ordering("0.00", precision=8, scale=2)
        assert encoded == ".000000.00"
    
    def test_encoding_edge_cases(self):
        """Test encoding at precision boundaries."""
        # Maximum positive for DECIMAL(4,2): 99.99
        encoded = encode_decimal_for_string_ordering("99.99", precision=4, scale=2)
        assert encoded == ".99.99"
        
        # Maximum negative for DECIMAL(4,2): -99.99
        encoded = encode_decimal_for_string_ordering("-99.99", precision=4, scale=2)
        assert encoded == "-00.00"  # Digit inversion of 99.99
    
    def test_precision_validation(self):
        """Test that values exceeding precision/scale raise errors."""
        # Value too large for DECIMAL(4,2)
        with pytest.raises(ValueError, match="exceeds DECIMAL"):
            encode_decimal_for_string_ordering("1000.00", precision=4, scale=2)
        
        # Too many decimal places handled by rounding/truncation in format()
        # This should work - format() will handle rounding
        encoded = encode_decimal_for_string_ordering("12.999", precision=4, scale=2)
        assert encoded == ".13.00"  # Rounded by format()
    
    def test_ordering_preservation(self):
        """Test that lexicographic ordering matches numeric ordering."""
        values = ["-99.99", "-10.50", "-0.01", "0.00", "0.01", "10.50", "99.99"]
        precision, scale = 8, 2
        
        encoded_values = [
            encode_decimal_for_string_ordering(v, precision, scale) for v in values
        ]
        
        # Check that encoded values are in ascending lexicographic order
        for i in range(len(encoded_values) - 1):
            assert encoded_values[i] < encoded_values[i + 1], \
                f"Ordering broken: {values[i]} ({encoded_values[i]}) >= {values[i+1]} ({encoded_values[i+1]})"
    
    def test_roundtrip_encoding(self):
        """Test that encode/decode roundtrip works correctly."""
        test_values = ["123.45", "-67.89", "0.00", "999.99", "-999.99"]
        precision, scale = 8, 2
        
        for value_str in test_values:
            original = Decimal(value_str)
            encoded = encode_decimal_for_string_ordering(value_str, precision, scale)
            decoded = decode_decimal_from_string_ordering(encoded)
            assert decoded == original, f"Roundtrip failed for {value_str}: {original} != {decoded}"


class TestDecimalColumnDefinition:
    """Test ColumnDefinition with precision and scale."""
    
    def test_column_creation_with_precision_scale(self):
        """Test creating columns with precision/scale metadata."""
        col = ColumnDefinition(
            name="price",
            type="DECIMAL(10,2)",
            precision=10,
            scale=2
        )
        
        assert col.name == "price"
        assert col.type == "DECIMAL(10,2)"
        assert col.precision == 10
        assert col.scale == 2
        assert col.length is None
    
    def test_column_serialization(self):
        """Test that precision/scale are preserved in TOML serialization."""
        col = ColumnDefinition(
            name="amount",
            type="DECIMAL(8,3)",
            precision=8,
            scale=3
        )
        
        # Convert to dict (for TOML)
        col_dict = col.to_dict()
        assert col_dict['precision'] == 8
        assert col_dict['scale'] == 3
        
        # Convert back from dict
        restored_col = ColumnDefinition.from_dict(col_dict)
        assert restored_col.precision == 8
        assert restored_col.scale == 3


class TestDecimalSchemaIntegration:
    """Test DECIMAL integration with schema manager."""
    
    @pytest.fixture
    def schema_manager(self):
        """Create schema manager with DECIMAL columns."""
        sm = SchemaManager("test_decimal_schema")
        
        # Create test table with various DECIMAL columns
        table_def = TableDefinition(
            name="financial_data",
            columns=[
                ColumnDefinition(name="id", type="INTEGER", primary_key=True),
                ColumnDefinition(name="price", type="DECIMAL(10,2)", indexed=True, precision=10, scale=2),
                ColumnDefinition(name="rate", type="DECIMAL(5,4)", indexed=True, precision=5, scale=4), 
                ColumnDefinition(name="count", type="DECIMAL(8,0)", indexed=True, precision=8, scale=0),
                ColumnDefinition(name="description", type="VARCHAR(100)")
            ],
            indexes=[],
            foreign_keys=[]
        )
        sm.add_table(table_def)
        return sm
    
    def test_decimal_annotations_generation(self, schema_manager):
        """Test that DECIMAL values are stored as string annotations."""
        row_data = {
            "id": 1,
            "price": Decimal("123.45"),
            "rate": Decimal("0.1234"),
            "count": Decimal("1000"),
            "description": "Test item"
        }
        
        annotations = schema_manager.get_entity_annotations_for_table("financial_data", row_data)
        
        # Check that DECIMAL values are in string annotations with proper encoding
        string_annotations = annotations["string_annotations"]
        assert "price" in string_annotations
        assert "rate" in string_annotations  
        assert "count" in string_annotations
        
        # Verify encoding format (should start with '.' for positive numbers)
        assert string_annotations["price"].startswith(".")
        assert string_annotations["rate"].startswith(".")
        assert string_annotations["count"].startswith(".")
        
        # Check non-DECIMAL values
        assert annotations["numeric_annotations"]["id"] > 0  # Encoded signed integer
        assert string_annotations["description"] == "Test item"
    
    def test_negative_decimal_annotations(self, schema_manager):
        """Test negative DECIMAL values use '-' prefix and digit inversion."""
        row_data = {
            "id": 2,
            "price": Decimal("-99.99"),
            "rate": Decimal("-0.5000"),
            "count": Decimal("-500"),
            "description": "Negative values"
        }
        
        annotations = schema_manager.get_entity_annotations_for_table("financial_data", row_data)
        
        string_annotations = annotations["string_annotations"]
        
        # All should start with '-' for negative numbers
        assert string_annotations["price"].startswith("-")
        assert string_annotations["rate"].startswith("-") 
        assert string_annotations["count"].startswith("-")
        
        # Test ordering: negative values should sort before positive
        positive_price = encode_decimal_for_string_ordering("99.99", 10, 2)
        assert string_annotations["price"] < positive_price


class TestDecimalQueryTranslation:
    """Test DECIMAL query translation."""
    
    @pytest.fixture
    def query_translator(self):
        """Create query translator with DECIMAL schema."""
        sm = SchemaManager("test_decimal_queries")
        
        table_def = TableDefinition(
            name="products",
            columns=[
                ColumnDefinition(name="id", type="INTEGER", primary_key=True),
                ColumnDefinition(name="price", type="DECIMAL(8,2)", indexed=True, precision=8, scale=2),
                ColumnDefinition(name="weight", type="DECIMAL(6,3)", indexed=True, precision=6, scale=3)
            ],
            indexes=[],
            foreign_keys=[]
        )
        sm.add_table(table_def)
        
        return QueryTranslator(sm)
    
    def test_decimal_equality_queries(self, query_translator):
        """Test DECIMAL equality queries."""
        result = query_translator.translate_select("SELECT * FROM products WHERE price = 99.99")
        
        # Should encode the value and use string comparison
        encoded_price = encode_decimal_for_string_ordering("99.99", 8, 2)
        assert f'price="{encoded_price}"' in result.golem_query
    
    def test_decimal_range_queries(self, query_translator):
        """Test DECIMAL range queries maintain ordering."""
        result = query_translator.translate_select("SELECT * FROM products WHERE price > 50.00 AND price < 100.00")
        
        encoded_min = encode_decimal_for_string_ordering("50.00", 8, 2)
        encoded_max = encode_decimal_for_string_ordering("100.00", 8, 2)
        
        query = result.golem_query
        assert f'price>"{encoded_min}"' in query
        assert f'price<"{encoded_max}"' in query
    
    def test_decimal_negative_queries(self, query_translator):
        """Test queries with negative DECIMAL values."""
        result = query_translator.translate_select("SELECT * FROM products WHERE price >= -10.50")
        
        encoded_value = encode_decimal_for_string_ordering("-10.50", 8, 2)
        assert f'price>="{encoded_value}"' in result.golem_query
    
    def test_float_types_not_queryable(self, query_translator):
        """Test that FLOAT types raise error when used in queries."""
        # Add a FLOAT column to test error handling
        sm = query_translator.schema_manager
        table = sm.get_table("products")
        table.columns.append(
            ColumnDefinition(name="temperature", type="FLOAT", indexed=True)
        )
        
        # This should raise ProgrammingError
        with pytest.raises(Exception, match="not indexable"):
            query_translator.translate_select("SELECT * FROM products WHERE temperature > 20.5")


class TestDecimalPrecisionScaleUtilities:
    """Test utility functions for DECIMAL precision/scale handling."""
    
    def test_get_decimal_precision_scale(self):
        """Test extracting precision/scale from column type strings."""
        precision, scale = get_decimal_precision_scale("DECIMAL(10,2)")
        assert precision == 10
        assert scale == 2
        
        precision, scale = get_decimal_precision_scale("NUMERIC(8)")
        assert precision == 8
        assert scale == 0  # Default scale
        
        precision, scale = get_decimal_precision_scale("DECIMAL")
        assert precision == 18  # Default precision
        assert scale == 0   # Default scale
        
        # Non-decimal types should raise error
        with pytest.raises(ValueError, match="Not a decimal type"):
            get_decimal_precision_scale("INTEGER")


class TestDecimalComplexScenarios:
    """Test complex scenarios combining multiple DECIMAL features."""
    
    def test_mixed_precision_scale_ordering(self):
        """Test ordering with different precision/scale combinations."""
        # Create values that test boundary conditions
        test_cases = [
            ("DECIMAL(4,2)", ["-99.99", "-1.00", "0.00", "1.00", "99.99"]),
            ("DECIMAL(6,3)", ["-999.999", "-0.001", "0.000", "0.001", "999.999"]),
            ("DECIMAL(8,0)", ["-99999999", "-1", "0", "1", "99999999"]),  # Integer decimals
        ]
        
        for col_type, values in test_cases:
            precision, scale = get_decimal_precision_scale(col_type)
            
            encoded_values = [
                encode_decimal_for_string_ordering(v, precision, scale) for v in values
            ]
            
            # Verify ordering is preserved
            assert encoded_values == sorted(encoded_values), \
                f"Ordering failed for {col_type} with values {values}"
            
            # Verify roundtrip
            for original, encoded in zip(values, encoded_values):
                decoded = decode_decimal_from_string_ordering(encoded)
                assert decoded == Decimal(original), \
                    f"Roundtrip failed for {original}: got {decoded}"
    
    def test_decimal_sql_standard_compliance(self):
        """Test compliance with SQL92 DECIMAL standard."""
        # Test default precision/scale
        encoded = encode_decimal_for_string_ordering("123.456789", precision=18, scale=6)
        decoded = decode_decimal_from_string_ordering(encoded)
        assert str(decoded) == "123.456789"
        
        # Test scale=0 (integer decimals)
        encoded = encode_decimal_for_string_ordering("12345", precision=10, scale=0)
        assert "." not in encoded[1:]  # No decimal point after prefix
        decoded = decode_decimal_from_string_ordering(encoded)
        assert decoded == Decimal("12345")