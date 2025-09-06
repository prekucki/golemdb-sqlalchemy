#!/usr/bin/env python3
"""Test script to verify hybrid filtering (indexed + post-filter)."""

import golemdb_sql
import sys
import os
import logging
from dotenv import load_dotenv

def main():
    """Test hybrid filtering approach."""
    
    # Configure debug logging
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    
    # Enable specific loggers
    logging.getLogger('golemdb_sql.cursor').setLevel(logging.DEBUG)
    logging.getLogger('golemdb_sql.query_translator').setLevel(logging.DEBUG)
    logging.getLogger('golem_base_sdk').setLevel(logging.INFO)
    
    load_dotenv()
    
    private_key = os.getenv('PRIVATE_KEY')
    if not private_key:
        print("❌ PRIVATE_KEY not found")
        sys.exit(1)
    
    print("🧪 Testing hybrid filtering (indexed + post-filter)...")
    
    conn = golemdb_sql.connect(
        rpc_url='https://ethwarsaw.holesky.golemdb.io/rpc',
        ws_url='wss://ethwarsaw.holesky.golemdb.io/rpc/ws',
        private_key=private_key,
        app_id='demo_app',
        schema_id='hybrid_filter_test'
    )
    
    try:
        cursor = conn.cursor()
        
        print("📋 Setting up test data...")
        cursor.execute("DROP TABLE IF EXISTS users")
        cursor.execute("""
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name VARCHAR(100),
                email VARCHAR(200), 
                age INTEGER,
                active BOOLEAN
            )
        """)
        
        # Only index id (PK), email, and active - leave name and age non-indexed
        cursor.execute("CREATE INDEX idx_email ON users(email)")
        cursor.execute("CREATE INDEX idx_active ON users(active)")
        
        print("✅ Created table: indexed columns (id, email, active), non-indexed (name, age)")
        
        # Insert test data
        cursor.execute("""
            INSERT INTO users (id, name, email, age, active) 
            VALUES (%(id)s, %(name)s, %(email)s, %(age)s, %(active)s)
        """, {'id': 1, 'name': 'Alice', 'email': 'alice@example.com', 'age': 25, 'active': True})
        
        cursor.execute("""
            INSERT INTO users (id, name, email, age, active) 
            VALUES (%(id)s, %(name)s, %(email)s, %(age)s, %(active)s)
        """, {'id': 2, 'name': 'Bob', 'email': 'bob@example.com', 'age': -5, 'active': False})
        
        cursor.execute("""
            INSERT INTO users (id, name, email, age, active) 
            VALUES (%(id)s, %(name)s, %(email)s, %(age)s, %(active)s)
        """, {'id': 3, 'name': 'Charlie', 'email': 'charlie@example.com', 'age': 35, 'active': True})
        
        print("✅ Inserted 3 test users")
        
        print("\n🔍 Testing hybrid queries...")
        
        # Test 1: Query on indexed column only (should use GolemBase query)
        print("\n1. Query indexed column only: WHERE active = true")
        cursor.execute("SELECT id, name, age FROM users WHERE active = true")
        results = cursor.fetchall()
        print(f"   Results: {len(results)} rows")
        for row in results:
            print(f"     ID: {row[0]}, Name: {row[1]}, Age: {row[2]}")
        
        # Test 2: Query on non-indexed column only (should use post-filter)
        print("\n2. Query non-indexed column only: WHERE age < 0")
        cursor.execute("SELECT id, name, age FROM users WHERE age < 0")
        results = cursor.fetchall()
        print(f"   Results: {len(results)} rows")
        for row in results:
            print(f"     ID: {row[0]}, Name: {row[1]}, Age: {row[2]}")
        
        # Test 3: Hybrid query (indexed + non-indexed)
        print("\n3. Hybrid query: WHERE active = true AND age > 30")
        cursor.execute("SELECT id, name, age, active FROM users WHERE active = true AND age > 30")
        results = cursor.fetchall()
        print(f"   Results: {len(results)} rows")
        for row in results:
            print(f"     ID: {row[0]}, Name: {row[1]}, Age: {row[2]}, Active: {row[3]}")
        
        print("\n✅ Hybrid filtering test completed!")
        
    except Exception as e:
        print(f"❌ Test error: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        cursor.close()
        conn.close()
        print("\n🔌 Connection closed")

if __name__ == "__main__":
    main()