"""PEP 249 DB-API 2.0 compliant exception hierarchy for GolemBase."""


class Warning(Exception):
    """Exception raised for important warnings like data truncations while inserting, etc.
    
    This exception is not a subclass of StandardError. It is not considered an error.
    """
    pass


class Error(Exception):
    """Exception that is the base class of all other error exceptions.
    
    You can use this to catch all errors with one single except statement.
    """
    pass


class InterfaceError(Error):
    """Exception raised for errors that are related to the database interface 
    rather than the database itself.
    
    This includes errors such as incorrect number of parameters specified, 
    data type inconsistencies, etc.
    """
    pass


class DatabaseError(Error):
    """Exception raised for errors that are related to the database.
    
    This includes errors such as connection failures, SQL syntax errors, etc.
    """
    pass


class DataError(DatabaseError):
    """Exception raised for errors that are due to problems with the processed data.
    
    Examples: division by zero, numeric value out of range, etc.
    """
    pass


class OperationalError(DatabaseError):
    """Exception raised for errors that are related to the database's operation 
    and not necessarily under the control of the programmer.
    
    Examples: unexpected disconnect, database name not found, transaction could 
    not be processed, memory allocation error, etc.
    """
    pass


class IntegrityError(DatabaseError):
    """Exception raised when the relational integrity of the database is affected.
    
    Examples: foreign key check fails, duplicate key, etc.
    """
    pass


class InternalError(DatabaseError):
    """Exception raised when the database encounters an internal error.
    
    Examples: the cursor is not valid anymore, the transaction is out of sync, etc.
    """
    pass


class ProgrammingError(DatabaseError):
    """Exception raised for programming errors.
    
    Examples: table not found or already exists, error in the SQL statement, 
    wrong number of parameters specified, etc.
    """
    pass


class NotSupportedError(DatabaseError):
    """Exception raised in case a method or database API was used which is not 
    supported by the database.
    
    Examples: requesting a .rollback() on a connection that does not support 
    transactions or has transactions turned off.
    """
    pass