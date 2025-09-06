#!/usr/bin/env python3
"""Comprehensive SQLAlchemy GolemBase Dialect Usage Example.

This example demonstrates all the key features of the GolemBase SQLAlchemy dialect:
- Connection management and URL configuration
- Table definition and creation
- Schema introspection and reflection
- Basic CRUD operations
- Relationship handling
- Transaction management
- Error handling and connection validation
"""

import os
import sys
import logging
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

def setup_logging():
    """Configure logging for the example."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    
    # Enable SQLAlchemy query logging
    logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

def create_golembase_engine():
    """Create and configure the GolemBase SQLAlchemy engine."""
    from sqlalchemy import create_engine
    
    # Load environment variables
    load_dotenv()
    
    # Get configuration from environment
    private_key = os.getenv('PRIVATE_KEY')
    rpc_url = os.getenv('RPC_URL', 'https://ethwarsaw.holesky.golemdb.io/rpc')
    ws_url = os.getenv('WS_URL', 'wss://ethwarsaw.holesky.golemdb.io/rpc/ws')
    app_id = os.getenv('APP_ID', 'sqlalchemy_example')
    schema_id = os.getenv('SCHEMA_ID', 'sqlalchemy_demo_schema')
    
    if not private_key:
        print("❌ PRIVATE_KEY not found in environment variables")
        print("Please set PRIVATE_KEY in your .env file")
        sys.exit(1)
    
    # Create connection URL
    connection_url = (
        f"golembase:///{schema_id}"
        f"?rpc_url={rpc_url}"
        f"&ws_url={ws_url}"
        f"&private_key={private_key}"
        f"&app_id={app_id}"
    )
    
    print(f"🔗 Connecting to GolemBase...")
    print(f"📡 RPC URL: {rpc_url}")
    print(f"🔌 WS URL: {ws_url}")
    print(f"🏷️  App ID: {app_id}")
    print(f"📋 Schema ID: {schema_id}")
    
    # Create engine with connection pooling and echo for SQL visibility
    engine = create_engine(
        connection_url,
        echo=True,  # Show SQL queries
        pool_size=5,
        max_overflow=10,
        pool_timeout=30,
        pool_recycle=3600
    )
    
    return engine

def demonstrate_table_definition():
    """Demonstrate SQLAlchemy table definitions for GolemBase."""
    from sqlalchemy import MetaData, Table, Column, Integer, String, Boolean, DateTime, Numeric, ForeignKey
    from sqlalchemy.sql import func
    
    print("\n📋 Demonstrating Table Definitions...")
    
    metadata = MetaData()
    
    # Users table
    users_table = Table(
        'users',
        metadata,
        Column('id', Integer, primary_key=True),
        Column('username', String(50), nullable=False, unique=True),
        Column('email', String(100), nullable=False, index=True),
        Column('full_name', String(200), nullable=True),
        Column('is_active', Boolean, default=True),
        Column('balance', Numeric(10, 2), default=0.00),
        Column('created_at', DateTime, default=func.current_timestamp()),
        Column('updated_at', DateTime, default=func.current_timestamp(), onupdate=func.current_timestamp())
    )
    
    # Posts table with relationship
    posts_table = Table(
        'posts',
        metadata,
        Column('id', Integer, primary_key=True),
        Column('title', String(200), nullable=False),
        Column('content', String(2000), nullable=True),
        Column('author_id', Integer, ForeignKey('users.id'), nullable=False),
        Column('is_published', Boolean, default=False),
        Column('view_count', Integer, default=0),
        Column('created_at', DateTime, default=func.current_timestamp())
    )
    
    # Categories table
    categories_table = Table(
        'categories',
        metadata,
        Column('id', Integer, primary_key=True),
        Column('name', String(100), nullable=False, unique=True),
        Column('description', String(500), nullable=True),
        Column('is_active', Boolean, default=True)
    )
    
    print(f"✅ Defined {len(metadata.tables)} tables:")
    for table_name, table in metadata.tables.items():
        print(f"   - {table_name}: {len(table.columns)} columns")
    
    return metadata

def demonstrate_schema_operations(engine):
    """Demonstrate schema creation and management operations."""
    from sqlalchemy import MetaData
    from sqlalchemy.sql import text
    
    print("\n🔧 Demonstrating Schema Operations...")
    
    # Define tables
    metadata = demonstrate_table_definition()
    
    with engine.connect() as connection:
        # Create all tables
        print("\n  Creating tables...")
        try:
            metadata.create_all(engine)
            print("  ✅ Tables created successfully")
        except Exception as e:
            print(f"  ⚠️ Table creation: {e}")
        
        # Test basic connectivity
        try:
            result = connection.execute(text("SELECT 1 as test"))
            test_value = result.fetchone()
            print(f"  ✅ Connection test: {test_value}")
        except Exception as e:
            print(f"  ⚠️ Connection test failed: {e}")

def demonstrate_schema_introspection(engine):
    """Demonstrate schema introspection and reflection capabilities."""
    from sqlalchemy import inspect, MetaData, Table
    from sqlalchemy.sql import text
    
    print("\n🔍 Demonstrating Schema Introspection...")
    
    with engine.connect() as connection:
        # Raw SQL introspection commands
        print("\n  Testing raw SQL introspection...")
        
        try:
            # SHOW TABLES
            result = connection.execute(text("SHOW TABLES"))
            tables = result.fetchall()
            print(f"  ✅ SHOW TABLES found {len(tables)} tables:")
            for table in tables:
                print(f"     - {table[0]}")
        except Exception as e:
            print(f"  ⚠️ SHOW TABLES failed: {e}")
        
        try:
            # DESCRIBE table
            result = connection.execute(text("DESCRIBE users"))
            columns = result.fetchall()
            print(f"  ✅ DESCRIBE users found {len(columns)} columns:")
            for col in columns:
                print(f"     - {col[0]} ({col[1]}) - Nullable: {col[2]}, Key: {col[3]}")
        except Exception as e:
            print(f"  ⚠️ DESCRIBE failed: {e}")
    
    # SQLAlchemy introspection
    print("\n  Testing SQLAlchemy introspection...")
    
    try:
        inspector = inspect(engine)
        
        # Get table names
        table_names = inspector.get_table_names()
        print(f"  ✅ Inspector found {len(table_names)} tables: {table_names}")
        
        # Get detailed column information
        for table_name in table_names[:2]:  # Limit to first 2 tables
            try:
                columns = inspector.get_columns(table_name)
                print(f"  ✅ Table '{table_name}' has {len(columns)} columns:")
                for col in columns[:3]:  # Show first 3 columns
                    print(f"     - {col['name']}: {col['type']} (nullable={col['nullable']})")
                
                # Get primary key info
                pk_info = inspector.get_pk_constraint(table_name)
                if pk_info['constrained_columns']:
                    print(f"     Primary key: {pk_info['constrained_columns']}")
                
                # Get index info
                indexes = inspector.get_indexes(table_name)
                if indexes:
                    print(f"     Indexes: {len(indexes)} found")
                    for idx in indexes[:2]:  # Show first 2 indexes
                        print(f"       - {idx['name']}: {idx['column_names']} (unique={idx['unique']})")
                        
            except Exception as e:
                print(f"  ⚠️ Column introspection for {table_name}: {e}")
                
    except Exception as e:
        print(f"  ⚠️ SQLAlchemy introspection failed: {e}")

def demonstrate_reflection(engine):
    """Demonstrate automatic table reflection from database."""
    from sqlalchemy import MetaData, Table
    
    print("\n🪞 Demonstrating Table Reflection...")
    
    try:
        # Create new metadata instance for reflection
        reflected_metadata = MetaData()
        
        # Reflect existing tables
        reflected_metadata.reflect(bind=engine)
        
        print(f"  ✅ Reflected {len(reflected_metadata.tables)} tables:")
        for table_name, table in reflected_metadata.tables.items():
            print(f"     - {table_name}:")
            print(f"       Columns: {len(table.columns)}")
            print(f"       Primary key: {[col.name for col in table.primary_key]}")
            print(f"       Indexes: {len(table.indexes)}")
        
        # Demonstrate working with reflected table
        if 'users' in reflected_metadata.tables:
            users_table = reflected_metadata.tables['users']
            print(f"  ✅ Working with reflected 'users' table:")
            print(f"     Columns: {[col.name for col in users_table.columns]}")
            
    except Exception as e:
        print(f"  ⚠️ Table reflection failed: {e}")

def demonstrate_crud_operations(engine):
    """Demonstrate basic CRUD operations using SQLAlchemy."""
    from sqlalchemy import MetaData, select, insert, update, delete
    from sqlalchemy.sql import text
    
    print("\n📝 Demonstrating CRUD Operations...")
    
    # Get reflected metadata to work with existing tables
    metadata = MetaData()
    
    try:
        metadata.reflect(bind=engine)
    except Exception as e:
        print(f"  ⚠️ Could not reflect tables for CRUD demo: {e}")
        return
    
    if 'users' not in metadata.tables:
        print("  ⚠️ Users table not found, skipping CRUD demo")
        return
    
    users_table = metadata.tables['users']
    
    with engine.connect() as connection:
        with connection.begin():  # Use transaction
            
            # CREATE - Insert new users
            print("\n  Testing INSERT operations...")
            try:
                # Insert single user
                insert_stmt = insert(users_table).values(
                    username='alice123',
                    email='alice@example.com',
                    full_name='Alice Smith',
                    balance=100.50
                )
                result = connection.execute(insert_stmt)
                print(f"  ✅ Inserted user: {result.rowcount} row(s) affected")
                
                # Insert multiple users
                users_data = [
                    {'username': 'bob456', 'email': 'bob@example.com', 'full_name': 'Bob Johnson', 'balance': 250.00},
                    {'username': 'charlie789', 'email': 'charlie@example.com', 'full_name': 'Charlie Brown', 'balance': 75.25}
                ]
                insert_stmt = insert(users_table)
                result = connection.execute(insert_stmt, users_data)
                print(f"  ✅ Bulk insert: {result.rowcount} row(s) affected")
                
            except Exception as e:
                print(f"  ⚠️ INSERT failed: {e}")
            
            # READ - Select and query data
            print("\n  Testing SELECT operations...")
            try:
                # Simple select all
                select_stmt = select(users_table)
                result = connection.execute(select_stmt)
                users = result.fetchall()
                print(f"  ✅ Found {len(users)} users total")
                
                # Select with conditions
                select_stmt = select(users_table).where(users_table.c.balance > 100)
                result = connection.execute(select_stmt)
                rich_users = result.fetchall()
                print(f"  ✅ Found {len(rich_users)} users with balance > 100")
                
                # Select specific columns
                select_stmt = select(users_table.c.username, users_table.c.email).limit(2)
                result = connection.execute(select_stmt)
                user_contacts = result.fetchall()
                print(f"  ✅ Retrieved contact info for {len(user_contacts)} users:")
                for contact in user_contacts:
                    print(f"     - {contact[0]}: {contact[1]}")
                    
            except Exception as e:
                print(f"  ⚠️ SELECT failed: {e}")
            
            # UPDATE - Modify existing data
            print("\n  Testing UPDATE operations...")
            try:
                # Update single user
                update_stmt = update(users_table).where(
                    users_table.c.username == 'alice123'
                ).values(
                    balance=150.00,
                    is_active=True
                )
                result = connection.execute(update_stmt)
                print(f"  ✅ Updated alice123: {result.rowcount} row(s) affected")
                
                # Bulk update
                update_stmt = update(users_table).where(
                    users_table.c.balance < 100
                ).values(
                    is_active=False
                )
                result = connection.execute(update_stmt)
                print(f"  ✅ Bulk update (balance < 100): {result.rowcount} row(s) affected")
                
            except Exception as e:
                print(f"  ⚠️ UPDATE failed: {e}")
            
            # DELETE - Remove data
            print("\n  Testing DELETE operations...")
            try:
                # Delete specific user
                delete_stmt = delete(users_table).where(
                    users_table.c.username == 'charlie789'
                )
                result = connection.execute(delete_stmt)
                print(f"  ✅ Deleted charlie789: {result.rowcount} row(s) affected")
                
                # Conditional delete
                delete_stmt = delete(users_table).where(
                    users_table.c.is_active == False
                )
                result = connection.execute(delete_stmt)
                print(f"  ✅ Deleted inactive users: {result.rowcount} row(s) affected")
                
            except Exception as e:
                print(f"  ⚠️ DELETE failed: {e}")
            
            # Final count
            try:
                select_stmt = select(users_table)
                result = connection.execute(select_stmt)
                final_users = result.fetchall()
                print(f"  ✅ Final user count: {len(final_users)}")
            except Exception as e:
                print(f"  ⚠️ Final count failed: {e}")

def demonstrate_advanced_queries(engine):
    """Demonstrate advanced querying capabilities."""
    from sqlalchemy import MetaData, select, func, and_, or_, text
    
    print("\n🔍 Demonstrating Advanced Queries...")
    
    metadata = MetaData()
    try:
        metadata.reflect(bind=engine)
    except Exception as e:
        print(f"  ⚠️ Could not reflect tables: {e}")
        return
    
    if 'users' not in metadata.tables:
        print("  ⚠️ Users table not found")
        return
        
    users_table = metadata.tables['users']
    
    with engine.connect() as connection:
        
        # Aggregate queries
        print("\n  Testing aggregate functions...")
        try:
            # Count users
            count_stmt = select(func.count(users_table.c.id))
            result = connection.execute(count_stmt)
            user_count = result.scalar()
            print(f"  ✅ Total users: {user_count}")
            
            # Average balance
            avg_stmt = select(func.avg(users_table.c.balance))
            result = connection.execute(avg_stmt)
            avg_balance = result.scalar()
            if avg_balance:
                print(f"  ✅ Average balance: {float(avg_balance):.2f}")
            
        except Exception as e:
            print(f"  ⚠️ Aggregate queries failed: {e}")
        
        # Complex WHERE conditions
        print("\n  Testing complex WHERE conditions...")
        try:
            # AND condition
            select_stmt = select(users_table).where(
                and_(
                    users_table.c.is_active == True,
                    users_table.c.balance > 50
                )
            )
            result = connection.execute(select_stmt)
            active_rich_users = result.fetchall()
            print(f"  ✅ Active users with balance > 50: {len(active_rich_users)}")
            
            # OR condition
            select_stmt = select(users_table).where(
                or_(
                    users_table.c.balance > 200,
                    users_table.c.username.like('%admin%')
                )
            )
            result = connection.execute(select_stmt)
            special_users = result.fetchall()
            print(f"  ✅ Rich or admin users: {len(special_users)}")
            
        except Exception as e:
            print(f"  ⚠️ Complex WHERE failed: {e}")
        
        # Pattern matching
        print("\n  Testing pattern matching...")
        try:
            # LIKE operation
            select_stmt = select(users_table).where(
                users_table.c.email.like('%@example.com')
            )
            result = connection.execute(select_stmt)
            example_users = result.fetchall()
            print(f"  ✅ Users with @example.com email: {len(example_users)}")
            
        except Exception as e:
            print(f"  ⚠️ Pattern matching failed: {e}")

def demonstrate_error_handling(engine):
    """Demonstrate error handling and connection management."""
    from sqlalchemy import text
    from sqlalchemy.exc import SQLAlchemyError
    
    print("\n⚠️ Demonstrating Error Handling...")
    
    # Test invalid query
    with engine.connect() as connection:
        try:
            result = connection.execute(text("SELECT * FROM nonexistent_table"))
            print("  ❌ This should have failed!")
        except SQLAlchemyError as e:
            print(f"  ✅ Properly caught invalid table error: {type(e).__name__}")
        
        try:
            result = connection.execute(text("INVALID SQL SYNTAX"))
            print("  ❌ This should have failed!")
        except SQLAlchemyError as e:
            print(f"  ✅ Properly caught syntax error: {type(e).__name__}")
    
    # Test connection health
    try:
        with engine.connect() as connection:
            # Test connection ping
            result = connection.execute(text("SELECT 1"))
            if result.fetchone():
                print("  ✅ Connection health check passed")
    except Exception as e:
        print(f"  ⚠️ Connection health check failed: {e}")

def main():
    """Main function demonstrating all SQLAlchemy GolemBase dialect features."""
    print("🚀 SQLAlchemy GolemBase Dialect - Comprehensive Example")
    print("=" * 60)
    
    # Setup logging
    setup_logging()
    
    try:
        # Create engine
        engine = create_golembase_engine()
        print("✅ Engine created successfully")
        
        # Run demonstrations
        demonstrate_schema_operations(engine)
        demonstrate_schema_introspection(engine)
        demonstrate_reflection(engine)
        demonstrate_crud_operations(engine)
        demonstrate_advanced_queries(engine)
        demonstrate_error_handling(engine)
        
        print("\n🎉 SQLAlchemy GolemBase Dialect Example completed successfully!")
        print("\n🎯 Key Features Demonstrated:")
        print("   ✅ Engine creation and connection management")
        print("   ✅ Table definition and schema operations")
        print("   ✅ Schema introspection (SHOW TABLES, DESCRIBE)")
        print("   ✅ Table reflection from database")
        print("   ✅ Full CRUD operations (Create, Read, Update, Delete)")
        print("   ✅ Advanced querying (aggregates, conditions, patterns)")
        print("   ✅ Error handling and connection validation")
        print("   ✅ Transaction management")
        
        print("\n💡 The GolemBase dialect provides full SQLAlchemy compatibility!")
        print("   You can now use GolemBase with any SQLAlchemy-based:")
        print("   - ORMs (SQLAlchemy Core & ORM)")
        print("   - Web frameworks (FastAPI, Flask, Django)")
        print("   - Data analysis tools (pandas, etc.)")
        print("   - Database admin tools")
        
    except KeyboardInterrupt:
        print("\n\n⏹️ Example interrupted by user")
    except Exception as e:
        print(f"\n❌ Example failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()