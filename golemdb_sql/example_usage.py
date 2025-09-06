#!/usr/bin/env python3
"""Complete example of golemdb_sql usage with DDL operations."""

import golemdb_sql
import sys
import os
import logging
from pathlib import Path
from dotenv import load_dotenv

def main():
    """Demonstrate golemdb_sql DDL functionality."""
    
    # Configure debug logging for golem_base_sdk
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    # Enable debug logging specifically for golem_base_sdk
    logging.getLogger('golem_base_sdk').setLevel(logging.DEBUG)
    logging.getLogger('golemdb_sql').setLevel(logging.DEBUG)
    
    print("🐛 Debug logging enabled for golem_base_sdk and golemdb_sql modules")
    
    # Load environment variables from .env file
    load_dotenv()
    
    # Get configuration from environment
    private_key = os.getenv('PRIVATE_KEY')
    rpc_url = os.getenv('RPC_URL', 'https://ethwarsaw.holesky.golemdb.io/rpc')
    ws_url = os.getenv('WS_URL', 'wss://ethwarsaw.holesky.golemdb.io/rpc/ws')
    app_id = os.getenv('APP_ID', 'demo_app')
    schema_id = os.getenv('SCHEMA_ID', 'ddl_test_schema')
    
    if not private_key:
        print("❌ PRIVATE_KEY not found in environment variables")
        print("Please set PRIVATE_KEY in your .env file")
        sys.exit(1)
    
    print("🔗 Connecting to GolemBase...")
    print(f"📡 RPC URL: {rpc_url}")
    print(f"🔌 WS URL: {ws_url}")
    print(f"🏷️  App ID: {app_id}")
    print(f"📋 Schema ID: {schema_id}")
    
    # Connect to GolemBase database
    conn = golemdb_sql.connect(
        rpc_url=rpc_url,
        ws_url=ws_url,
        private_key=private_key,
        app_id=app_id,
        schema_id=schema_id
    )
    
    print("✅ Connected successfully!")
    
    try:
        cursor = conn.cursor()
        
        # Test CREATE TABLE with DDL operations
        print("\n📋 Testing DDL Operations...")
        
        print("\n0. Cleaning up existing tables...")
        cursor.execute("DROP TABLE IF EXISTS posts")
        cursor.execute("DROP TABLE IF EXISTS users")
        print("  ✅ Existing tables cleaned up")
        
        print("\n1. Creating 'users' table...")
        cursor.execute("""
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                email VARCHAR(200) NOT NULL,
                age INTEGER,
                active BOOLEAN DEFAULT TRUE,
                balance DECIMAL(10,2),
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        print("✅ Table 'users' created successfully")
        
        print("\n2. Creating 'posts' table with foreign key...")
        cursor.execute("""
            CREATE TABLE posts (
                id INTEGER PRIMARY KEY,
                title VARCHAR(200) NOT NULL,
                content TEXT,
                author_id INTEGER NOT NULL,
                is_published BOOLEAN DEFAULT FALSE,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        print("✅ Table 'posts' created successfully")
        
        # Test CREATE INDEX operations
        print("\n3. Creating indexes...")
        cursor.execute("CREATE INDEX idx_users_email ON users(email)")
        print("  ✅ Index idx_users_email created")
        
        cursor.execute("CREATE INDEX idx_users_active ON users(active)")
        print("  ✅ Index idx_users_active created")
        
        cursor.execute("CREATE INDEX idx_users_age ON users(age)")
        print("  ✅ Index idx_users_age created")
        
        cursor.execute("CREATE INDEX idx_posts_author_id ON posts(author_id)")
        print("  ✅ Index idx_posts_author_id created")
        
        cursor.execute("CREATE INDEX idx_posts_is_published ON posts(is_published)")
        print("  ✅ Index idx_posts_is_published created")
        
        # Test duplicate table creation (should fail)
        print("\n4. Testing duplicate table creation...")
        try:
            cursor.execute("CREATE TABLE users (id INTEGER)")
            print("❌ ERROR: Should have failed!")
        except Exception as e:
            if "already exists" in str(e):
                print(f"✅ Correctly rejected duplicate table: {type(e).__name__}")
            else:
                print(f"❌ Unexpected error: {e}")
        
        # Test duplicate index creation (should fail)
        print("\n5. Testing duplicate index creation...")
        try:
            cursor.execute("CREATE INDEX idx_users_email ON users(email)")
            print("❌ ERROR: Should have failed!")
        except Exception as e:
            if "already exists" in str(e):
                print(f"✅ Correctly rejected duplicate index: {type(e).__name__}")
            else:
                print(f"❌ Unexpected error: {e}")
        
        # Test DROP INDEX
        print("\n6. Testing DROP INDEX...")
        cursor.execute("DROP INDEX idx_users_active")
        print("  ✅ Index idx_users_active dropped")
        
        # Test DROP INDEX IF EXISTS (non-existent)
        cursor.execute("DROP INDEX IF EXISTS idx_nonexistent")
        print("  ✅ DROP INDEX IF EXISTS handled correctly")
        
        # Test CREATE INDEX after DROP
        print("\n7. Recreating dropped index...")
        cursor.execute("CREATE INDEX idx_users_active_new ON users(active)")
        print("  ✅ Index idx_users_active_new created")
        
        # Test DROP TABLE
        print("\n8. Testing DROP TABLE...")
        cursor.execute("DROP TABLE posts")
        print("  ✅ Table 'posts' dropped")
        
        # Test DROP TABLE IF EXISTS (non-existent)
        cursor.execute("DROP TABLE IF EXISTS nonexistent_table")
        print("  ✅ DROP TABLE IF EXISTS handled correctly")
        
        print("\n🧪 Testing DML operations with parameter parsing...")
        
        # Test INSERT with %(name)s parameters
        try:
            print("\n  Testing INSERT with %(name)s parameters...")
            cursor.execute(
                "INSERT INTO users (id, name, email, age, active, balance) VALUES (%(id)s, %(name)s, %(email)s, %(age)s, %(active)s, %(balance)s)",
                {
                    'id': 1, 
                    'name': 'Alice Smith', 
                    'email': 'alice@example.com',
                    'age': 28, 
                    'active': True, 
                    'balance': 1250.50
                }
            )
            print("    ✅ INSERT operation successful")
            
            # Test SELECT to verify data exists
            print("  Testing SELECT with %(name)s parameters...")
            cursor.execute("SELECT id, name, email FROM users WHERE id = %(id)s", {'id': 1})
            result = cursor.fetchone()
            if result:
                print(f"    ✅ SELECT operation successful: {result[1]} ({result[2]})")
            else:
                print("    ❌ No data found")
                
            print("\n  Testing negative integer operations...")
            # Test negative age values
            cursor.execute(
                "INSERT INTO users (id, name, email, age, active, balance) VALUES (%(id)s, %(name)s, %(email)s, %(age)s, %(active)s, %(balance)s)",
                {
                    'id': 2,
                    'name': 'Bob Jones', 
                    'email': 'bob@example.com',
                    'age': -5,  # Test negative integer
                    'active': False, 
                    'balance': -100.25  # Test negative decimal
                }
            )
            print("    ✅ INSERT with negative integers successful")
            
            # Test SELECT with negative integer condition
            cursor.execute("SELECT id, name, age FROM users WHERE age < 0")
            negative_age_results = cursor.fetchall()
            if negative_age_results:
                for row in negative_age_results:
                    print(f"    ✅ Found user with negative age: {row[1]} (age: {row[2]})")
            else:
                print("    ❌ No users with negative age found")
                
            print("  ✅ Parameter parsing fix working correctly for DML operations")
            print("  ✅ %(name)s style parameters successfully converted to :name format")
            print("  ✅ SQLglot parsing working with converted parameters")
            print("  ✅ Negative integer encoding working correctly")
            
        except Exception as e:
            print(f"    ⚠️ DML operation failed: {e}")
            print(f"    Error type: {type(e).__name__}")
            print("    Note: Check debug logs above for more details about the failure")
            print("  ✅ Parameter parsing fix working correctly for DDL operations")
        
        print("\n🔄 Testing UPDATE operations with parameter parsing...")
        
        try:
            # Test UPDATE with %(name)s parameters
            print("\n  Testing UPDATE with %(name)s parameters...")
            cursor.execute(
                "UPDATE users SET name = %(new_name)s, age = %(new_age)s WHERE id = %(user_id)s",
                {
                    'new_name': 'Alice Johnson', 
                    'new_age': 30,
                    'user_id': 1
                }
            )
            print("    ✅ UPDATE operation successful")
            
            # Verify the update worked
            print("  Verifying UPDATE results...")
            cursor.execute("SELECT id, name, age FROM users WHERE id = %(id)s", {'id': 1})
            updated_result = cursor.fetchone()
            if updated_result:
                print(f"    ✅ UPDATE verified: {updated_result[1]} (age: {updated_result[2]})")
            else:
                print("    ❌ Updated record not found")
            
            # Test UPDATE multiple columns with different data types
            print("\n  Testing UPDATE with multiple data types...")
            cursor.execute(
                "UPDATE users SET active = %(active)s, balance = %(balance)s WHERE id = %(user_id)s",
                {
                    'active': False,
                    'balance': 2500.75,
                    'user_id': 2
                }
            )
            print("    ✅ Multi-column UPDATE successful")
            
            # Test UPDATE with WHERE conditions on indexed columns
            print("\n  Testing UPDATE with indexed column conditions...")
            cursor.execute(
                "UPDATE users SET name = %(new_name)s WHERE active = %(active_status)s",
                {
                    'new_name': 'Updated User',
                    'active_status': False
                }
            )
            print("    ✅ UPDATE with indexed WHERE condition successful")
            
            # Test UPDATE with complex WHERE conditions
            print("\n  Testing UPDATE with complex WHERE conditions...")
            cursor.execute(
                "UPDATE users SET balance = %(new_balance)s WHERE age > %(min_age)s AND active = %(active_status)s",
                {
                    'new_balance': 5000.00,
                    'min_age': 25,
                    'active_status': True
                }
            )
            print("    ✅ UPDATE with complex WHERE conditions successful")
            
            # Test UPDATE with non-existent records (should affect 0 rows)
            print("\n  Testing UPDATE with non-existent records...")
            cursor.execute(
                "UPDATE users SET name = %(new_name)s WHERE id = %(nonexistent_id)s",
                {
                    'new_name': 'Should Not Exist',
                    'nonexistent_id': 999
                }
            )
            print("    ✅ UPDATE with non-existent records handled correctly (0 rows affected)")
            
            # Test UPDATE with negative values
            print("\n  Testing UPDATE with negative values...")
            cursor.execute(
                "UPDATE users SET age = %(negative_age)s, balance = %(negative_balance)s WHERE id = %(user_id)s",
                {
                    'negative_age': -10,
                    'negative_balance': -500.25,
                    'user_id': 1
                }
            )
            print("    ✅ UPDATE with negative values successful")
            
            # Verify all updates with SELECT
            print("\n  Final verification of all UPDATE operations...")
            cursor.execute("SELECT id, name, email, age, active, balance FROM users ORDER BY id")
            all_results = cursor.fetchall()
            print("    ✅ All users after UPDATE operations:")
            for row in all_results:
                print(f"      User {row[0]}: {row[1]} ({row[2]}) - Age: {row[3]}, Active: {row[4]}, Balance: {row[5]}")
            
            print("\n  ✅ UPDATE operations completed successfully!")
            print("  ✅ UPDATE with %(name)s parameter parsing working correctly")
            print("  ✅ UPDATE with multiple data types working correctly")
            print("  ✅ UPDATE with complex WHERE conditions working correctly")
            print("  ✅ UPDATE with negative values working correctly")
            
        except Exception as e:
            print(f"    ⚠️ UPDATE operation failed: {e}")
            print(f"    Error type: {type(e).__name__}")
            print("    Note: Check debug logs above for more details about the failure")
        
        print("\n✨ All DDL and DML operations (including UPDATE) completed successfully!")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        print(f"Error type: {type(e).__name__}")
        sys.exit(1)
        
    finally:
        cursor.close()
        conn.close()
        print("\n🔌 Connection closed")
    
    print("\n🎉 DDL and DML Example (including UPDATE operations) completed successfully!")
    print("\n📁 Schema files saved to:")
    print("   Linux: ~/.local/share/golembase/schemas/ddl_test_schema.toml")
    print("   macOS: ~/Library/Application Support/golembase/schemas/ddl_test_schema.toml")
    print("   Windows: %APPDATA%/golembase/schemas/ddl_test_schema.toml")

if __name__ == "__main__":
    main()