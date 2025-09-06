"""Tests for exception hierarchy."""

import pytest
from golemdb_sql.exceptions import (
    Error,
    DatabaseError,
    InterfaceError,
    DataError,
    OperationalError,
    IntegrityError,
    InternalError,
    ProgrammingError,
    NotSupportedError
)


class TestExceptionHierarchy:
    """Test PEP 249 exception hierarchy."""
    
    def test_exception_inheritance(self):
        """Test exception inheritance structure."""
        # All exceptions should inherit from Error
        assert issubclass(DatabaseError, Error)
        assert issubclass(InterfaceError, Error)
        
        # DatabaseError subclasses
        assert issubclass(DataError, DatabaseError)
        assert issubclass(OperationalError, DatabaseError)
        assert issubclass(IntegrityError, DatabaseError)
        assert issubclass(InternalError, DatabaseError)
        assert issubclass(ProgrammingError, DatabaseError)
        assert issubclass(NotSupportedError, DatabaseError)
    
    def test_exception_creation(self):
        """Test exception creation with messages."""
        # Test base Error
        error = Error("Base error message")
        assert str(error) == "Base error message"
        
        # Test DatabaseError
        db_error = DatabaseError("Database error")
        assert str(db_error) == "Database error"
        assert isinstance(db_error, Error)
        
        # Test InterfaceError
        interface_error = InterfaceError("Interface error")
        assert str(interface_error) == "Interface error"
        assert isinstance(interface_error, Error)
        
        # Test specific database errors
        data_error = DataError("Invalid data")
        assert str(data_error) == "Invalid data"
        assert isinstance(data_error, DatabaseError)
        assert isinstance(data_error, Error)
        
        op_error = OperationalError("Connection failed")
        assert str(op_error) == "Connection failed"
        assert isinstance(op_error, DatabaseError)
        
        integrity_error = IntegrityError("Constraint violation")
        assert str(integrity_error) == "Constraint violation"
        assert isinstance(integrity_error, DatabaseError)
        
        internal_error = InternalError("Internal database error")
        assert str(internal_error) == "Internal database error"
        assert isinstance(internal_error, DatabaseError)
        
        prog_error = ProgrammingError("SQL syntax error")
        assert str(prog_error) == "SQL syntax error"
        assert isinstance(prog_error, DatabaseError)
        
        not_supported_error = NotSupportedError("Feature not supported")
        assert str(not_supported_error) == "Feature not supported"
        assert isinstance(not_supported_error, DatabaseError)
    
    def test_exception_raising(self):
        """Test that exceptions can be raised and caught properly."""
        # Test Error catching
        with pytest.raises(Error):
            raise DatabaseError("Test database error")
        
        with pytest.raises(Error):
            raise InterfaceError("Test interface error")
        
        # Test DatabaseError catching
        with pytest.raises(DatabaseError):
            raise DataError("Test data error")
        
        with pytest.raises(DatabaseError):
            raise OperationalError("Test operational error")
        
        with pytest.raises(DatabaseError):
            raise ProgrammingError("Test programming error")
        
        # Test specific exception catching
        with pytest.raises(DataError):
            raise DataError("Specific data error")
        
        with pytest.raises(OperationalError):
            raise OperationalError("Specific operational error")
        
        with pytest.raises(InterfaceError):
            raise InterfaceError("Specific interface error")
    
    def test_exception_chaining(self):
        """Test exception chaining with cause."""
        original_error = ValueError("Original cause")
        
        try:
            raise original_error
        except ValueError as e:
            try:
                raise DatabaseError("Database operation failed") from e
            except DatabaseError as chained_error:
                assert chained_error.__cause__ is original_error
                assert isinstance(chained_error, DatabaseError)
                assert isinstance(chained_error, Error)
    
    def test_exception_with_no_message(self):
        """Test exceptions without message."""
        error = Error()
        assert str(error) == ""
        
        db_error = DatabaseError()
        assert str(db_error) == ""
    
    def test_exception_equality(self):
        """Test exception equality."""
        error1 = DataError("Same message")
        error2 = DataError("Same message")
        error3 = DataError("Different message")
        
        # Exceptions with same message should be equal
        assert str(error1) == str(error2)
        assert str(error1) != str(error3)
        
        # But they are different objects
        assert error1 is not error2
    
    def test_standard_exception_behavior(self):
        """Test that exceptions behave like standard Python exceptions."""
        error = ProgrammingError("Test error", "Additional info")
        
        # Should be able to access args
        assert error.args == ("Test error", "Additional info")
        
        # String representation
        assert "Test error" in str(error)
        
        # Should be truthy
        assert error
        
        # Should work with isinstance
        assert isinstance(error, Exception)
        assert isinstance(error, ProgrammingError)
        assert isinstance(error, DatabaseError)
        assert isinstance(error, Error)
    
    def test_exception_repr(self):
        """Test exception repr."""
        error = DataError("Test message")
        repr_str = repr(error)
        
        assert "DataError" in repr_str
        assert "Test message" in repr_str
    
    def test_multiple_inheritance_compatibility(self):
        """Test that exceptions work with multiple inheritance patterns."""
        # This tests compatibility with potential future extensions
        class CustomError(DatabaseError):
            """Custom database error."""
            pass
        
        custom_error = CustomError("Custom error message")
        
        assert isinstance(custom_error, CustomError)
        assert isinstance(custom_error, DatabaseError)
        assert isinstance(custom_error, Error)
        assert isinstance(custom_error, Exception)
        
        # Should be catchable as any parent type
        with pytest.raises(Error):
            raise custom_error
        
        with pytest.raises(DatabaseError):
            raise custom_error
        
        with pytest.raises(CustomError):
            raise custom_error