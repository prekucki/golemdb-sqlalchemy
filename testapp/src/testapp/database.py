"""Database connection and session management for testapp."""

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool
from .models import Base
import os
from dotenv import load_dotenv


class DatabaseManager:
    """Manages database connections and sessions."""
    
    def __init__(self, database_url: str = None):
        """Initialize database manager."""
        if database_url is None:
            # Load environment variables from .env file
            load_dotenv()
            
            # Get GolemBase connection parameters from environment
            private_key = os.environ.get('PRIVATE_KEY')
            rpc_url = os.environ.get('RPC_URL', 'https://ethwarsaw.holesky.golemdb.io/rpc')
            ws_url = os.environ.get('WS_URL', 'wss://ethwarsaw.holesky.golemdb.io/rpc/ws')
            app_id = os.environ.get('APP_ID', 'testapp')
            schema_id = os.environ.get('SCHEMA_ID', 'testapp_schema')
            
            # Check if we have a complete connection URL from environment
            database_url = os.environ.get('GOLEMBASE_DATABASE_URL')
            
            if database_url is None:
                if not private_key:
                    raise ValueError(
                        "GolemBase connection requires PRIVATE_KEY environment variable. "
                        "Either set PRIVATE_KEY, RPC_URL (optional) or set complete GOLEMBASE_DATABASE_URL"
                    )
                
                # Build GolemBase URL with query parameters
                from urllib.parse import urlencode
                params = {
                    'rpc_url': rpc_url,
                    'ws_url': ws_url,
                    'private_key': private_key,
                    'app_id': app_id
                }
                query_string = urlencode(params)
                database_url = f'golembase:///{schema_id}?{query_string}'
        
        self.database_url = database_url
        self.engine = None
        self.SessionLocal = None
        
    def create_engine(self, **engine_kwargs):
        """Create SQLAlchemy engine for GolemBase."""
        default_kwargs = {
            'echo': True,  # Enable SQL logging for testing
            'pool_pre_ping': True,
            'pool_recycle': 300,
        }
        default_kwargs.update(engine_kwargs)
        
        self.engine = create_engine(self.database_url, **default_kwargs)
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
        
        return self.engine
    
    def create_tables(self):
        """Create all tables in the database."""
        if self.engine is None:
            raise RuntimeError("Engine not created. Call create_engine() first.")
        
        Base.metadata.create_all(bind=self.engine)
    
    def drop_tables(self):
        """Drop all tables from the database."""
        if self.engine is None:
            raise RuntimeError("Engine not created. Call create_engine() first.")
        
        Base.metadata.drop_all(bind=self.engine)
    
    def get_session(self):
        """Get a database session."""
        if self.SessionLocal is None:
            raise RuntimeError("SessionLocal not created. Call create_engine() first.")
        
        return self.SessionLocal()
    
    def close(self):
        """Close database connections."""
        if self.engine:
            self.engine.dispose()


# Global database manager instance
db_manager = DatabaseManager()

# Pre-initialized connection pool for web server use
_connection_pool = None

def initialize_connection_pool():
    """Initialize GolemBase connections in the main thread before starting the web server."""
    global _connection_pool
    if _connection_pool is None:
        import golemdb_sql
        from dotenv import load_dotenv
        import os
        import threading
        
        # Load environment variables
        load_dotenv()
        
        # Get connection parameters
        private_key = os.environ.get('PRIVATE_KEY')
        rpc_url = os.environ.get('RPC_URL', 'https://ethwarsaw.holesky.golemdb.io/rpc')
        ws_url = os.environ.get('WS_URL', 'wss://ethwarsaw.holesky.golemdb.io/rpc/ws')
        app_id = os.environ.get('APP_ID', 'testapp')
        schema_id = os.environ.get('SCHEMA_ID', 'testapp_schema')
        
        if not private_key:
            raise ValueError("PRIVATE_KEY environment variable is required")
        
        # Check if we're in the main thread
        if threading.current_thread() is not threading.main_thread():
            print("⚠️  Warning: Connection pool initialization attempted in worker thread")
            print("🔄 This may fail due to GolemBase signal handler requirements")
        
        # Create a connection pool (simple approach - create one connection)
        print(f"🔗 Initializing GolemBase connection pool...")
        print(f"📡 RPC URL: {rpc_url}")
        print(f"🏷️  App ID: {app_id}")
        print(f"📋 Schema ID: {schema_id}")
        print(f"🧵 Current thread: {threading.current_thread().name}")
        
        # Force initialization in the current thread (might work in some cases)
        _connection_pool = golemdb_sql.connect(
            rpc_url=rpc_url,
            ws_url=ws_url,
            private_key=private_key,
            app_id=app_id,
            schema_id=schema_id
        )
        print("✅ GolemBase connection pool initialized successfully")

def get_golembase_connection():
    """Get a GolemBase connection from the pool."""
    global _connection_pool
    if _connection_pool is None:
        # Initialize connection pool
        try:
            print("🔄 Attempting to initialize connection pool...")
            initialize_connection_pool()
        except Exception as e:
            raise RuntimeError(
                f"Connection pool not initialized and initialization failed: {e}\n"
                f"Make sure you're running in an environment where GolemBase client can be initialized"
            )
    return _connection_pool


def get_db():
    """Dependency to get database session."""
    session = db_manager.get_session()
    try:
        yield session
    finally:
        session.close()