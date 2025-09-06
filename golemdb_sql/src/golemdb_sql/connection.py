"""PEP 249 DB-API 2.0 compliant Connection class for GolemBase."""

import asyncio
import threading
import requests
from typing import Any, Dict, List, Optional, Union
from golem_base_sdk import GolemBaseClient
from .connection_parser import parse_connection_kwargs, GolemBaseConnectionParams
from .cursor import Cursor
from .exceptions import (
    DatabaseError, 
    InterfaceError, 
    OperationalError, 
    ProgrammingError
)


class Connection:
    """DB-API 2.0 compliant connection to GolemBase database.
    
    This class provides transaction support and cursor management
    according to PEP 249 specifications.
    """
    
    def __init__(self, **kwargs: Any):
        """Initialize connection to GolemBase.
        
        Args:
            **kwargs: Connection parameters for GolemBase
        """
        self._closed = False
        self._autocommit = False
        self._in_transaction = False
        self._params: Optional[GolemBaseConnectionParams] = None
        self._client: Optional[GolemBaseClient] = None
        self._event_loop: Optional[asyncio.AbstractEventLoop] = None
        self._loop_thread: Optional[threading.Thread] = None
        
        # Batch operations for transaction emulation
        self._pending_operations: List[Dict[str, Any]] = []
        
        try:
            # Parse connection parameters
            self._params = parse_connection_kwargs(**kwargs)
            
            # Check basic connectivity first
            self._check_connectivity()
            
            # Initialize async client in background thread
            self._init_async_client()
            
        except Exception as e:
            raise DatabaseError(f"Failed to connect to GolemBase: {e}")
    
    def _init_async_client(self) -> None:
        """Initialize async GolemBase client with background event loop."""
        import threading
        
        try:
            # First, create the client in the main thread (for signal handler setup)
            try:
                # Try to get existing event loop or create new one in main thread
                try:
                    temp_loop = asyncio.get_event_loop()
                    if temp_loop.is_closed():
                        raise RuntimeError("Event loop is closed")
                except RuntimeError:
                    # No event loop in current thread, create a new one
                    temp_loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(temp_loop)
                
                # Create GolemBase client in main thread (for signal handlers)
                client_coro = GolemBaseClient.create(
                    rpc_url=self._params.rpc_url,
                    ws_url=self._params.ws_url,
                    private_key=self._params.get_private_key_bytes()
                )
                
                # Initialize client in main thread
                self._client = temp_loop.run_until_complete(
                    asyncio.wait_for(client_coro, timeout=30.0)
                )
                
                # Now create a separate event loop for background operations
                self._event_loop = asyncio.new_event_loop()
                
                # Start event loop in background thread
                def run_event_loop():
                    asyncio.set_event_loop(self._event_loop)
                    self._event_loop.run_forever()
                    
                self._loop_thread = threading.Thread(target=run_event_loop, daemon=True)
                self._loop_thread.start()
                
                # Wait a moment for the background loop to start
                import time
                time.sleep(0.1)
                
            except Exception as e:
                raise e
            
        except asyncio.TimeoutError:
            raise DatabaseError(
                f"Timeout connecting to GolemBase endpoints:\n"
                f"  RPC URL: {self._params.rpc_url}\n"
                f"  WS URL: {self._params.ws_url}\n"
                f"Check network connectivity and endpoint availability."
            )
        except Exception as e:
            error_msg = str(e)
            if "signal only works in main thread" in error_msg:
                raise DatabaseError(
                    f"GolemBase SDK threading issue. Try running in main thread or use:\n"
                    f"  import asyncio\n"
                    f"  asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())  # Windows only\n"
                    f"Original error: {e}"
                )
            else:
                raise DatabaseError(
                    f"Failed to initialize GolemBase client: {e}\n"
                    f"  RPC URL: {self._params.rpc_url}\n"
                    f"  WS URL: {self._params.ws_url}\n"
                    f"  Error type: {type(e).__name__}"
                )
    
    def _check_connectivity(self) -> None:
        """Check basic connectivity to GolemBase endpoints before full initialization."""
        import urllib.parse
        
        # Check RPC endpoint with a simple HTTP request
        try:
            response = requests.get(
                self._params.rpc_url,
                timeout=5,
                headers={'User-Agent': 'golemdb-sql/0.1.0'}
            )
            # Any response (even error) means the endpoint is reachable
        except requests.exceptions.Timeout:
            raise DatabaseError(
                f"RPC endpoint timeout:\n"
                f"  URL: {self._params.rpc_url}\n"
                f"  The server did not respond within 5 seconds."
            )
        except requests.exceptions.ConnectionError as e:
            raise DatabaseError(
                f"Cannot connect to RPC endpoint:\n"
                f"  URL: {self._params.rpc_url}\n"
                f"  Error: {e}\n"
                f"  Check if the server is running and the URL is correct."
            )
        except Exception as e:
            # Don't fail on other HTTP errors - the endpoint is reachable
            pass
        
        # Check WebSocket URL format
        parsed_ws = urllib.parse.urlparse(self._params.ws_url)
        if parsed_ws.scheme not in ('ws', 'wss'):
            raise DatabaseError(
                f"Invalid WebSocket URL scheme:\n"
                f"  URL: {self._params.ws_url}\n"
                f"  Expected: ws:// or wss://"
            )
    
    def close(self) -> None:
        """Close the connection now.
        
        The connection will be unusable from this point forward; an Error 
        exception will be raised if any operation is attempted.
        """
        if self._closed:
            return
            
        try:
            # Stop event loop
            if self._event_loop and not self._event_loop.is_closed():
                self._event_loop.call_soon_threadsafe(self._event_loop.stop)
                
            # Wait for thread to finish
            if self._loop_thread and self._loop_thread.is_alive():
                self._loop_thread.join(timeout=5.0)
                
        except Exception as e:
            raise DatabaseError(f"Error closing connection: {e}")
        finally:
            self._closed = True
            self._client = None
            self._event_loop = None
            self._loop_thread = None
    
    def _run_async(self, coro) -> Any:
        """Run async coroutine in background event loop.
        
        Args:
            coro: Coroutine to run
            
        Returns:
            Result of coroutine execution
        """
        import logging
        logger = logging.getLogger(__name__)
        
        if self._closed:
            raise InterfaceError("Connection is closed")
            
        if not self._event_loop or not self._client:
            raise InterfaceError("Connection not properly initialized")
        
        logger.debug(f"Starting async operation: {coro}")
        
        # Log query parameters if this is a query_entities call
        if hasattr(coro, 'cr_frame') and coro.cr_frame:
            try:
                frame_locals = coro.cr_frame.f_locals
                if 'query_string' in frame_locals:
                    logger.debug(f"GolemBase query string: '{frame_locals['query_string']}'")
                elif 'query' in frame_locals:
                    logger.debug(f"GolemBase query: '{frame_locals['query']}'")
            except:
                pass  # Ignore errors in introspection
        
        # Create a future to get the result
        future = asyncio.run_coroutine_threadsafe(coro, self._event_loop)
        
        try:
            logger.debug("Waiting for async operation to complete (30s timeout)...")
            result = future.result(timeout=30.0)  # 30 second timeout
            logger.debug(f"Async operation completed successfully: {result}")
            return result
        except asyncio.TimeoutError:
            logger.error("Async operation timed out after 30 seconds")
            raise OperationalError("Operation timed out")
        except Exception as e:
            logger.error(f"Async operation failed with exception: {e}")
            raise DatabaseError(f"Async operation failed: {e}")
    
    def commit(self) -> None:
        """Commit any pending transaction to the database.
        
        In GolemBase, this executes all batched operations atomically.
        """
        self._check_connection()
        
        if not self._in_transaction:
            return  # No transaction to commit
            
        try:
            # Execute all pending operations
            if self._pending_operations:
                self._execute_batch_operations()
            
            self._in_transaction = False
            self._pending_operations.clear()
            
        except Exception as e:
            # Clear pending operations on error
            self._pending_operations.clear()
            raise DatabaseError(f"Failed to commit transaction: {e}")
    
    def rollback(self) -> None:
        """Roll back to the start of any pending transaction.
        
        In GolemBase, this discards all batched operations.
        """
        self._check_connection()
        
        if not self._in_transaction:
            return  # No transaction to rollback
            
        try:
            # Clear pending operations without executing them
            self._pending_operations.clear()
            self._in_transaction = False
            
        except Exception as e:
            raise DatabaseError(f"Failed to rollback transaction: {e}")
    
    def cursor(self) -> Cursor:
        """Return a new Cursor Object using the connection.
        
        If the database does not provide a direct cursor concept, the module will 
        have to emulate cursors using other means to the extent needed by this specification.
        """
        self._check_connection()
        return Cursor(self)
    
    def _execute_batch_operations(self) -> None:
        """Execute all pending batch operations atomically."""
        if not self._pending_operations:
            return
            
        # Group operations by type for efficient batch execution
        creates = []
        updates = []
        deletes = []
        
        for op in self._pending_operations:
            op_type = op.get('type')
            if op_type == 'create':
                creates.append(op['entity'])
            elif op_type == 'update':
                updates.append(op['entity'])
            elif op_type == 'delete':
                deletes.append(op['entity'])
        
        # Execute operations in order: creates, updates, deletes
        if creates:
            self._run_async(self._client.create_entities(creates))
        if updates:
            self._run_async(self._client.update_entities(updates))
        if deletes:
            self._run_async(self._client.delete_entities(deletes))
    
    def add_pending_operation(self, operation: Dict[str, Any]) -> None:
        """Add operation to pending batch.
        
        Args:
            operation: Operation dictionary with 'type' and 'entity' keys
        """
        if not self._in_transaction and not self._autocommit:
            # Auto-start transaction if not in autocommit mode
            self.begin()
        
        if self._autocommit:
            # Execute immediately in autocommit mode
            op_type = operation.get('type')
            entity = operation.get('entity')
            
            if op_type == 'create':
                self._run_async(self._client.create_entities([entity]))
            elif op_type == 'update':
                self._run_async(self._client.update_entities([entity]))
            elif op_type == 'delete':
                self._run_async(self._client.delete_entities([entity]))
        else:
            # Add to batch for later execution
            self._pending_operations.append(operation)
    
    @property
    def client(self) -> GolemBaseClient:
        """Get the underlying GolemBase client.
        
        Returns:
            GolemBase client instance
        """
        self._check_connection()
        if not self._client:
            raise InterfaceError("Connection not initialized")
        return self._client
    
    @property
    def params(self) -> GolemBaseConnectionParams:
        """Get connection parameters.
        
        Returns:
            Connection parameters
        """
        self._check_connection()
        if not self._params:
            raise InterfaceError("Connection parameters not available")
        return self._params
    
    def execute(self, operation: str, parameters: Optional[Union[Dict[str, Any], List[Any]]] = None) -> Cursor:
        """Execute a database operation (query or command) directly on the connection.
        
        This is a convenience method that creates a cursor, executes the operation,
        and returns the cursor for result retrieval.
        
        Args:
            operation: SQL statement to execute
            parameters: Parameters for the SQL statement
            
        Returns:
            Cursor object with results
        """
        cursor = self.cursor()
        cursor.execute(operation, parameters)
        return cursor
    
    def executemany(self, operation: str, seq_of_parameters: List[Union[Dict[str, Any], List[Any]]]) -> Cursor:
        """Execute a database operation multiple times.
        
        Args:
            operation: SQL statement to execute  
            seq_of_parameters: Sequence of parameter sets
            
        Returns:
            Cursor object
        """
        cursor = self.cursor()
        cursor.executemany(operation, seq_of_parameters)
        return cursor
    
    def begin(self) -> None:
        """Start a new transaction explicitly.
        
        This method is not part of PEP 249 but is commonly provided
        for explicit transaction control.
        """
        self._check_connection()
        
        if self._in_transaction:
            raise ProgrammingError("Transaction already in progress")
            
        try:
            # GolemBase doesn't have explicit transaction begin/commit
            # We emulate transactions by batching operations
            self._in_transaction = True
        except Exception as e:
            raise DatabaseError(f"Failed to begin transaction: {e}")
    
    def _check_connection(self) -> None:
        """Check if connection is still valid.
        
        Raises InterfaceError if connection is closed.
        """
        if self._closed:
            raise InterfaceError("Connection is closed")
    
    def _ensure_transaction(self) -> None:
        """Ensure a transaction is active.
        
        Start a new transaction if not already in one and autocommit is off.
        """
        if not self._autocommit and not self._in_transaction:
            self.begin()
    
    @property
    def closed(self) -> bool:
        """Return True if connection is closed."""
        return self._closed
    
    @property
    def autocommit(self) -> bool:
        """Return current autocommit setting."""
        return self._autocommit
    
    @autocommit.setter
    def autocommit(self, value: bool) -> None:
        """Set autocommit mode."""
        self._check_connection()
        
        if value and self._in_transaction:
            # Commit any pending transaction before enabling autocommit
            self.commit()
            
        self._autocommit = value
    
    def __enter__(self) -> 'Connection':
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit with transaction handling."""
        if exc_type is None:
            # No exception, commit the transaction
            if self._in_transaction:
                self.commit()
        else:
            # Exception occurred, rollback the transaction
            if self._in_transaction:
                try:
                    self.rollback()
                except Exception:
                    # Ignore rollback errors in cleanup
                    pass
        
        # Always close the connection
        self.close()


def connect(**kwargs: Any) -> Connection:
    """Create a connection to GolemBase database.
    
    This is the module-level connect function required by PEP 249.
    
    Args:
        **kwargs: Connection parameters for GolemBase
        
    Supported connection formats:
    1. Connection string:
        connect(connection_string="golembase://private_key@host/app_id?ws_url=...")
        
    2. Key-value string:
        connect(connection_string="rpc_url=https://... ws_url=wss://... private_key=0x...")
        
    3. Individual parameters:
        connect(rpc_url="https://...", ws_url="wss://...", private_key="0x...", 
                app_id="myapp", schema_id="myschema")
        
    4. Host-based (constructs URLs):
        connect(host="example.com", port=443, private_key="0x...", database="myapp")
        
    Returns:
        Connection object
    """
    return Connection(**kwargs)