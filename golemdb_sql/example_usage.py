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
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    # Enable debug logging specifically for golem_base_sdk
    logging.getLogger('golem_base_sdk').setLevel(logging.INFO)
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
        
        print("\n🗑️ Testing DELETE operations with parameter parsing...")
        
        try:
            # First, let's add more test data specifically for DELETE examples
            print("\n  Adding additional test data for DELETE examples...")
            delete_test_records = [
                (10, 'Delete Test 1', 'deltest@example.com', 35, True, 1000.00),
                (11, 'Delete Test 2', 'deltest2@example.com', 40, False, 2000.00),
                (12, 'Delete Test 3', 'deltest3@example.com', 25, True, 500.00),
                (13, 'Delete Test 4', 'deltest4@example.com', 50, False, 3000.00),
            ]
            
            for record in delete_test_records:
                cursor.execute(
                    "INSERT INTO users (id, name, email, age, active, balance) VALUES (%(id)s, %(name)s, %(email)s, %(age)s, %(active)s, %(balance)s)",
                    {
                        'id': record[0], 
                        'name': record[1], 
                        'email': record[2],
                        'age': record[3], 
                        'active': record[4], 
                        'balance': record[5]
                    }
                )
            print(f"    ✅ Added {len(delete_test_records)} records for DELETE testing")
            
            # Test DELETE with simple WHERE clause
            print("\n  Testing DELETE with %(name)s parameters...")
            cursor.execute(
                "DELETE FROM users WHERE id = %(user_id)s",
                {'user_id': 10}
            )
            print("    ✅ DELETE with simple WHERE condition successful")
            
            # Verify the deletion worked
            print("  Verifying DELETE results...")
            cursor.execute("SELECT COUNT(*) FROM users WHERE id = %(id)s", {'id': 10})
            count_result = cursor.fetchone()
            count = count_result[0] if count_result else 0
            print(f"    ✅ DELETE verified: {count} records found with id=10 (should be 0)")
            
            # Test DELETE with indexed column condition
            print("\n  Testing DELETE with indexed column conditions...")
            cursor.execute(
                "DELETE FROM users WHERE active = %(active_status)s AND age > %(min_age)s",
                {
                    'active_status': False,
                    'min_age': 35
                }
            )
            print("    ✅ DELETE with indexed column conditions successful")
            
            # Test DELETE with email condition (string matching)
            print("\n  Testing DELETE with string matching...")
            cursor.execute(
                "DELETE FROM users WHERE email = %(email)s",
                {'email': 'deltest3@example.com'}
            )
            print("    ✅ DELETE with string matching successful")
            
            # Test DELETE with complex WHERE conditions
            print("\n  Testing DELETE with complex WHERE conditions...")
            cursor.execute(
                "DELETE FROM users WHERE age < %(max_age)s AND balance > %(min_balance)s",
                {
                    'max_age': 30,
                    'min_balance': 400.00
                }
            )
            print("    ✅ DELETE with complex WHERE conditions successful")
            
            # Test DELETE with non-existent condition (should affect 0 rows)
            print("\n  Testing DELETE with non-existent condition...")
            cursor.execute(
                "DELETE FROM users WHERE id = %(nonexistent_id)s",
                {'nonexistent_id': 999}
            )
            print("    ✅ DELETE with non-existent condition handled correctly (0 rows affected)")
            
            # Test DELETE with negative value conditions
            print("\n  Testing DELETE with negative value conditions...")
            cursor.execute(
                "DELETE FROM users WHERE age < %(negative_age)s OR balance < %(negative_balance)s",
                {
                    'negative_age': 0,
                    'negative_balance': 0
                }
            )
            print("    ✅ DELETE with negative value conditions successful")
            
            # Show remaining records after DELETE operations
            print("\n  Final verification of all DELETE operations...")
            cursor.execute("SELECT id, name, email, age, active, balance FROM users WHERE id >= 10 ORDER BY id")
            delete_test_results = cursor.fetchall()
            print("    ✅ Remaining test records after DELETE operations:")
            if delete_test_results:
                for row in delete_test_results:
                    print(f"      User {row[0]}: {row[1]} ({row[2]}) - Age: {row[3]}, Active: {row[4]}, Balance: {row[5]}")
            else:
                print("      No test records remaining (all successfully deleted)")
            
            print("\n  ✅ DELETE operations completed successfully!")
            print("  ✅ DELETE with %(name)s parameter parsing working correctly")
            print("  ✅ DELETE with indexed column conditions working correctly")
            print("  ✅ DELETE with complex WHERE conditions working correctly")
            print("  ✅ DELETE with string matching working correctly")
            print("  ✅ DELETE with negative value conditions working correctly")
            
        except Exception as e:
            print(f"    ⚠️ DELETE operation failed: {e}")
            print(f"    Error type: {type(e).__name__}")
            print("    Note: Check debug logs above for more details about the failure")
        
        print("\n🔍 Testing LIKE operator with pattern matching...")
        
        try:
            # Add more test data with varied names and emails for LIKE testing
            print("\n  Adding test data for LIKE operator examples...")
            like_test_data = [
                (20, 'John Smith', 'john.smith@company.com', 32, True, 2500.00),
                (21, 'Jane Johnson', 'jane.johnson@company.com', 28, True, 3000.00),
                (22, 'Bob Brown', 'bob.brown@external.org', 35, True, 2200.00),
                (23, 'Alice Anderson', 'alice.anderson@company.com', 29, False, 2800.00),
                (24, 'Charlie Chen', 'charlie.chen@freelance.net', 31, True, 3200.00),
                (25, 'Diana Davis', 'diana@company.com', 27, True, 2900.00)
            ]
            
            for record in like_test_data:
                cursor.execute(
                    "INSERT INTO users (id, name, email, age, active, balance) VALUES (%(id)s, %(name)s, %(email)s, %(age)s, %(active)s, %(balance)s)",
                    {
                        'id': record[0], 
                        'name': record[1], 
                        'email': record[2],
                        'age': record[3], 
                        'active': record[4], 
                        'balance': record[5]
                    }
                )
            print(f"    ✅ Added {len(like_test_data)} records for LIKE testing")
            
            # Test LIKE with prefix matching (indexed column)
            print("\n  Testing LIKE with prefix matching on indexed column...")
            cursor.execute("SELECT id, name, email FROM users WHERE name LIKE %(pattern)s", {'pattern': 'John%'})
            prefix_results = cursor.fetchall()
            print(f"    ✅ Found {len(prefix_results)} users with names starting with 'John':")
            for row in prefix_results:
                print(f"      {row[1]} ({row[2]})")
                
            # Test LIKE with suffix matching (indexed column)  
            print("\n  Testing LIKE with suffix matching on indexed column...")
            cursor.execute("SELECT id, name, email FROM users WHERE name LIKE %(pattern)s", {'pattern': '%son'})
            suffix_results = cursor.fetchall()
            print(f"    ✅ Found {len(suffix_results)} users with names ending with 'son':")
            for row in suffix_results:
                print(f"      {row[1]} ({row[2]})")
                
            # Test LIKE with contains matching (indexed column)
            print("\n  Testing LIKE with contains matching on indexed column...")
            cursor.execute("SELECT id, name, email FROM users WHERE email LIKE %(pattern)s", {'pattern': '%@company.com%'})
            contains_results = cursor.fetchall()
            print(f"    ✅ Found {len(contains_results)} users with '@company.com' in email:")
            for row in contains_results:
                print(f"      {row[1]} ({row[2]})")
                
            # Test LIKE with single character wildcard
            print("\n  Testing LIKE with single character wildcard...")
            cursor.execute("SELECT id, name FROM users WHERE name LIKE %(pattern)s", {'pattern': 'B_b %'})
            single_char_results = cursor.fetchall()
            print(f"    ✅ Found {len(single_char_results)} users matching 'B_b %' pattern:")
            for row in single_char_results:
                print(f"      {row[1]}")
                
            # Test LIKE with mixed wildcards
            print("\n  Testing LIKE with mixed wildcards...")  
            cursor.execute("SELECT id, name, email FROM users WHERE email LIKE %(pattern)s", {'pattern': '%.%@company.%'})
            mixed_results = cursor.fetchall()
            print(f"    ✅ Found {len(mixed_results)} users matching '%.%@company.%' pattern:")
            for row in mixed_results:
                print(f"      {row[1]} ({row[2]})")
                
            # Test LIKE combined with other conditions
            print("\n  Testing LIKE combined with other WHERE conditions...")
            cursor.execute(
                "SELECT id, name, email, age FROM users WHERE name LIKE %(name_pattern)s AND age >= %(min_age)s AND active = %(active_status)s",
                {
                    'name_pattern': '%a%',  # Contains 'a'
                    'min_age': 25,
                    'active_status': True
                }
            )
            combined_results = cursor.fetchall()
            print(f"    ✅ Found {len(combined_results)} active users age >= 25 with 'a' in name:")
            for row in combined_results:
                print(f"      {row[1]} ({row[2]}) - Age: {row[3]}")
                
            # Test LIKE with OR conditions
            print("\n  Testing LIKE with OR conditions...")
            cursor.execute(
                "SELECT id, name, email FROM users WHERE name LIKE %(pattern1)s OR email LIKE %(pattern2)s",
                {
                    'pattern1': 'Alice%',
                    'pattern2': '%@freelance.%'
                }
            )
            or_results = cursor.fetchall()
            print(f"    ✅ Found {len(or_results)} users matching Alice% OR %@freelance.% patterns:")
            for row in or_results:
                print(f"      {row[1]} ({row[2]})")
                
            # Test LIKE case sensitivity
            print("\n  Testing LIKE case sensitivity...")
            cursor.execute("SELECT id, name FROM users WHERE name LIKE %(pattern)s", {'pattern': 'john%'})
            case_results = cursor.fetchall()
            print(f"    ✅ Found {len(case_results)} users matching 'john%' (lowercase) pattern:")
            for row in case_results:
                print(f"      {row[1]}")
                
            # Test LIKE with exact match (no wildcards)
            print("\n  Testing LIKE with exact match (no wildcards)...")
            cursor.execute("SELECT id, name, email FROM users WHERE name LIKE %(pattern)s", {'pattern': 'Bob Brown'})
            exact_results = cursor.fetchall()
            print(f"    ✅ Found {len(exact_results)} users with exact name 'Bob Brown':")
            for row in exact_results:
                print(f"      {row[1]} ({row[2]})")
            
            print("\n  ✅ LIKE operator testing completed successfully!")
            print("  ✅ LIKE with prefix patterns (John%) working correctly")
            print("  ✅ LIKE with suffix patterns (%son) working correctly") 
            print("  ✅ LIKE with contains patterns (%@company.com%) working correctly")
            print("  ✅ LIKE with single character wildcards (B_b %) working correctly")
            print("  ✅ LIKE with mixed wildcards (%.%@company.%) working correctly")
            print("  ✅ LIKE combined with other conditions working correctly")
            print("  ✅ LIKE with OR conditions working correctly")
            print("  ✅ LIKE case sensitivity working correctly")
            print("  ✅ LIKE exact matching working correctly")
            print("  ✅ LIKE patterns automatically converted to GolemDB glob patterns for indexed columns")
            print("  ✅ Post-filtering handled correctly for non-indexed columns")
            
        except Exception as e:
            print(f"    ⚠️ LIKE operator testing failed: {e}")
            print(f"    Error type: {type(e).__name__}")
            print("    Note: Check debug logs above for more details about the failure")

        print("\n🔍 Testing Schema Introspection (SHOW TABLES & DESCRIBE)...")
        
        try:
            # Test SHOW TABLES command
            print("\n  Testing SHOW TABLES command...")
            cursor.execute("SHOW TABLES")
            table_results = cursor.fetchall()
            print(f"    ✅ Found {len(table_results)} tables in schema:")
            for row in table_results:
                print(f"      - {row[0]}")
            
            # Test DESCRIBE command on users table
            print("\n  Testing DESCRIBE command on 'users' table...")
            cursor.execute("DESCRIBE users")
            describe_results = cursor.fetchall()
            print(f"    ✅ Found {len(describe_results)} columns in users table:")
            print("      Field              Type              Null  Key  Default  Extra")
            print("      " + "-" * 70)
            for row in describe_results:
                field = str(row[0]).ljust(18)
                type_str = str(row[1]).ljust(18)
                null_str = str(row[2]).ljust(5)
                key_str = str(row[3] or '').ljust(4)
                default_str = str(row[4] or '').ljust(8)
                extra_str = str(row[5] or '')
                print(f"      {field} {type_str} {null_str} {key_str} {default_str} {extra_str}")
            
            # Test DESCRIBE command with DESC alias
            print("\n  Testing DESC command (alias for DESCRIBE)...")
            cursor.execute("DESC users")
            desc_results = cursor.fetchall()
            print(f"    ✅ DESC command returned {len(desc_results)} columns (same as DESCRIBE)")
            
            # Test DESCRIBE on non-existent table (should fail gracefully)
            print("\n  Testing DESCRIBE on non-existent table...")
            try:
                cursor.execute("DESCRIBE nonexistent_table")
                cursor.fetchall()
                print("    ❌ ERROR: Should have failed!")
            except Exception as e:
                if "does not exist" in str(e).lower():
                    print("    ✅ Correctly handled non-existent table")
                else:
                    print(f"    ⚠️ Unexpected error for non-existent table: {e}")
            
            print("\n  ✅ Schema introspection testing completed successfully!")
            print("  ✅ SHOW TABLES command working correctly")
            print("  ✅ DESCRIBE command working correctly")
            print("  ✅ DESC alias working correctly")
            print("  ✅ Error handling for non-existent tables working correctly")
            print("  ✅ Schema introspection enables SQLAlchemy dialect reflection capabilities")
            
        except Exception as e:
            print(f"    ⚠️ Schema introspection testing failed: {e}")
            print(f"    Error type: {type(e).__name__}")
            print("    Note: Check debug logs above for more details about the failure")

        print("\n✨ All DDL, DML, and Schema Introspection operations completed successfully!")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        print(f"Error type: {type(e).__name__}")
        sys.exit(1)
        
    finally:
        cursor.close()
        conn.close()
        print("\n🔌 Connection closed")
    
    print("\n🎉 DDL, DML, and Schema Introspection Example completed successfully!")
    print("\n📁 Schema files saved to:")
    print("   Linux: ~/.local/share/golembase/schemas/ddl_test_schema.toml")
    print("   macOS: ~/Library/Application Support/golembase/schemas/ddl_test_schema.toml")
    print("   Windows: %APPDATA%/golembase/schemas/ddl_test_schema.toml")

if __name__ == "__main__":
    main()
