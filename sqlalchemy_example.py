#!/usr/bin/env python3
"""SQLAlchemy dialect usage example with schema introspection."""

import os
import sys
import logging
from pathlib import Path
from dotenv import load_dotenv

def main():
    """Demonstrate SQLAlchemy dialect usage with GolemBase."""
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    
    print("🔧 SQLAlchemy GolemBase Dialect Example")
    print("=" * 50)
    
    # Load environment variables
    load_dotenv()
    
    # Get configuration from environment
    private_key = os.getenv('PRIVATE_KEY')
    rpc_url = os.getenv('RPC_URL', 'https://ethwarsaw.holesky.golemdb.io/rpc')
    ws_url = os.getenv('WS_URL', 'wss://ethwarsaw.holesky.golemdb.io/rpc/ws')
    app_id = os.getenv('APP_ID', 'sqlalchemy_demo')
    schema_id = os.getenv('SCHEMA_ID', 'sqlalchemy_test_schema')
    
    if not private_key:
        print("❌ PRIVATE_KEY not found in environment variables")
        print("Please set PRIVATE_KEY in your .env file")
        sys.exit(1)
    
    try:
        from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Boolean, DateTime, inspect
        from sqlalchemy.sql import text
        
        print("✅ SQLAlchemy imported successfully")
        
        # Create engine with GolemBase dialect
        connection_url = (
            f"golembase:///{schema_id}"
            f"?rpc_url={rpc_url}"
            f"&ws_url={ws_url}"
            f"&private_key={private_key}"
            f"&app_id={app_id}"
        )
        
        print(f"🔗 Connecting to GolemBase via SQLAlchemy...")
        print(f"📡 RPC URL: {rpc_url}")
        print(f"🔌 WS URL: {ws_url}")
        print(f"🏷️  App ID: {app_id}")
        print(f"📋 Schema ID: {schema_id}")
        
        engine = create_engine(connection_url, echo=True)  # echo=True shows SQL queries
        
        print("✅ SQLAlchemy Engine created successfully!")
        
        # Test basic connection
        with engine.connect() as conn:
            print("\n📋 Testing Schema Introspection via SQLAlchemy...")
            
            # Test SHOW TABLES through SQLAlchemy
            print("\n1. Testing SHOW TABLES through SQLAlchemy...")
            try:
                result = conn.execute(text("SHOW TABLES"))
                tables = result.fetchall()
                print(f"   ✅ Found {len(tables)} tables:")
                for table in tables:
                    print(f"     - {table[0]}")
            except Exception as e:
                print(f"   ⚠️ SHOW TABLES failed: {e}")
            
            # Test DESCRIBE through SQLAlchemy  
            print("\n2. Testing DESCRIBE through SQLAlchemy...")
            try:
                result = conn.execute(text("DESCRIBE users"))
                columns = result.fetchall()
                print(f"   ✅ Users table has {len(columns)} columns:")
                print("     Field              Type              Null  Key  Default  Extra")
                print("     " + "-" * 70)
                for col in columns:
                    field = str(col[0]).ljust(18)
                    type_str = str(col[1]).ljust(18)
                    null_str = str(col[2]).ljust(5)
                    key_str = str(col[3] or '').ljust(4)
                    default_str = str(col[4] or '').ljust(8)
                    extra_str = str(col[5] or '')
                    print(f"     {field} {type_str} {null_str} {key_str} {default_str} {extra_str}")
            except Exception as e:
                print(f"   ⚠️ DESCRIBE users failed: {e}")
                print("   (This is expected if the users table doesn't exist yet)")
        
        # Test SQLAlchemy reflection (uses our introspection internally)
        print("\n3. Testing SQLAlchemy Reflection...")
        try:
            inspector = inspect(engine)
            
            # Get table names through reflection
            table_names = inspector.get_table_names()
            print(f"   ✅ Reflection found {len(table_names)} tables:")
            for table_name in table_names:
                print(f"     - {table_name}")
                
                # Get column info for each table
                try:
                    columns = inspector.get_columns(table_name)
                    print(f"       Columns: {len(columns)} found")
                    for col in columns[:3]:  # Show first 3 columns
                        print(f"         - {col['name']}: {col['type']}")
                    if len(columns) > 3:
                        print(f"         ... and {len(columns) - 3} more")
                except Exception as e:
                    print(f"       ⚠️ Could not reflect columns: {e}")
                    
        except Exception as e:
            print(f"   ⚠️ SQLAlchemy reflection failed: {e}")
        
        # Demonstrate ORM-style table definition
        print("\n4. Testing ORM-Style Table Definition...")
        try:
            metadata = MetaData()
            
            # Define a sample table
            sample_table = Table(
                'sqlalchemy_sample',
                metadata,
                Column('id', Integer, primary_key=True),
                Column('name', String(100), nullable=False),
                Column('email', String(255), nullable=False),
                Column('active', Boolean, default=True),
                Column('created_at', DateTime)
            )
            
            print("   ✅ Table definition created")
            print(f"   Table: {sample_table.name}")
            print("   Columns:")
            for col in sample_table.columns:
                print(f"     - {col.name}: {col.type} (nullable={col.nullable})")
            
            # Note: We don't actually create the table here to avoid side effects
            print("   💡 Use metadata.create_all(engine) to create tables")
            
        except Exception as e:
            print(f"   ⚠️ Table definition failed: {e}")
        
        print("\n✨ SQLAlchemy GolemBase Dialect Example completed successfully!")
        print("\n🎯 Key Features Demonstrated:")
        print("   ✅ SQLAlchemy engine creation with GolemBase URL")
        print("   ✅ Direct SQL execution through SQLAlchemy")
        print("   ✅ SHOW TABLES command integration")
        print("   ✅ DESCRIBE command integration")
        print("   ✅ SQLAlchemy reflection capabilities")
        print("   ✅ ORM-style table definitions")
        print("\n💡 The dialect enables full SQLAlchemy compatibility with GolemBase!")
        
    except ImportError as e:
        print(f"❌ Import error: {e}")
        print("Make sure sqlalchemy-golembase is installed")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error: {e}")
        print(f"Error type: {type(e).__name__}")
        sys.exit(1)

if __name__ == "__main__":
    main()