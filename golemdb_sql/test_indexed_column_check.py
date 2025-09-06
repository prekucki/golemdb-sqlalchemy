#!/usr/bin/env python3
"""Test script to verify indexed column checking."""

import golemdb_sql
import sys
import os
from dotenv import load_dotenv

def main():
    """Test indexed column validation."""
    
    load_dotenv()
    
    private_key = os.getenv('PRIVATE_KEY')
    if not private_key:
        print("❌ PRIVATE_KEY not found")
        sys.exit(1)
    
    conn = golemdb_sql.connect(
        rpc_url='https://ethwarsaw.holesky.golemdb.io/rpc',
        ws_url='wss://ethwarsaw.holesky.golemdb.io/rpc/ws',
        private_key=private_key,
        app_id='demo_app',
        schema_id='indexed_column_test'
    )
    
    try:
        cursor = conn.cursor()
        
        print("🧪 Testing indexed column validation...")
        
        # Create table with only some indexed columns
        cursor.execute("DROP TABLE IF EXISTS test_users")
        cursor.execute("""
            CREATE TABLE test_users (
                id INTEGER PRIMARY KEY,
                name VARCHAR(100),
                email VARCHAR(200), 
                age INTEGER,
                active BOOLEAN
            )
        """)
        
        # Only index email and active
        cursor.execute("CREATE INDEX idx_email ON test_users(email)")
        cursor.execute("CREATE INDEX idx_active ON test_users(active)")
        
        print("✅ Created table with indexed columns: id (PK), email, active")
        print("   Non-indexed columns: name, age")
        
        print("\n🧪 Testing queries on indexed columns...")
        
        # These should work (indexed columns)
        try:
            cursor.execute("SELECT * FROM test_users WHERE id = 1")
            print("✅ Query on indexed column 'id' works")
        except Exception as e:
            print(f"❌ Query on indexed column 'id' failed: {e}")
        
        try:
            cursor.execute("SELECT * FROM test_users WHERE active = true")
            print("✅ Query on indexed column 'active' works")
        except Exception as e:
            print(f"❌ Query on indexed column 'active' failed: {e}")
            
        print("\n🧪 Testing queries on non-indexed columns...")
        
        # These should fail (non-indexed columns)
        try:
            cursor.execute("SELECT * FROM test_users WHERE age < 0")
            print("❌ Query on non-indexed column 'age' should have failed!")
        except Exception as e:
            if "not indexed" in str(e):
                print(f"✅ Correctly rejected query on non-indexed column 'age': {type(e).__name__}")
            else:
                print(f"❌ Unexpected error for non-indexed column 'age': {e}")
        
        try:
            cursor.execute("SELECT * FROM test_users WHERE name = 'John'")
            print("❌ Query on non-indexed column 'name' should have failed!")
        except Exception as e:
            if "not indexed" in str(e):
                print(f"✅ Correctly rejected query on non-indexed column 'name': {type(e).__name__}")
            else:
                print(f"❌ Unexpected error for non-indexed column 'name': {e}")
        
    except Exception as e:
        print(f"❌ Test error: {e}")
        
    finally:
        cursor.close()
        conn.close()
        print("\n🔌 Connection closed")

if __name__ == "__main__":
    main()