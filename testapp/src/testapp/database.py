"""Database connection and session management for testapp."""

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool
from .models import Base
import os


class DatabaseManager:
    """Manages database connections and sessions."""
    
    def __init__(self, database_url: str = None):
        """Initialize database manager."""
        if database_url is None:
            # Default to GolemBase connection
            # Format: golembase://username:password@host:port/database
            database_url = os.environ.get(
                'GOLEMBASE_DATABASE_URL',
                'golembase://user:password@localhost:5432/testdb'
            )
        
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


def get_db():
    """Dependency to get database session."""
    session = db_manager.get_session()
    try:
        yield session
    finally:
        session.close()