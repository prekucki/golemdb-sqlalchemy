#!/usr/bin/env python3
"""Test script to verify negative integer encoding works with GolemBase database."""

import golemdb_sql
import sys
import os
import logging
from dotenv import load_dotenv


def main():
    """Test negative integer INSERT and SELECT operations."""
    
    # Configure debug logging
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    
    # Enable debug logging for GolemDB modules
    logging.getLogger('golemdb_sql.query_translator').setLevel(logging.DEBUG)
    logging.getLogger('golem_base_sdk').setLevel(logging.INFO)  # Reduce noise
    
    # Load environment variables
    load_dotenv()
    
    # Get configuration from environment
    private_key = os.getenv('PRIVATE_KEY')
    rpc_url = os.getenv('RPC_URL', 'https://ethwarsaw.holesky.golemdb.io/rpc')
    ws_url = os.getenv('WS_URL', 'wss://ethwarsaw.holesky.golemdb.io/rpc/ws')
    app_id = os.getenv('APP_ID', 'demo_app')
    schema_id = 'negative_int_test'
    
    if not private_key:
        print("❌ PRIVATE_KEY not found in environment variables")
        sys.exit(1)
    
    print("🧪 Testing negative integer encoding with GolemBase...")
    print(f"📡 RPC URL: {rpc_url}")
    print(f"🏷️  Schema ID: {schema_id}")
    
    # Connect to GolemBase database
    conn = golemdb_sql.connect(
        rpc_url=rpc_url,
        ws_url=ws_url,
        private_key=private_key,
        app_id=app_id,
        schema_id=schema_id
    )
    
    try:
        cursor = conn.cursor()
        
        print("\n📋 Setting up test table...")
        cursor.execute("DROP TABLE IF EXISTS test_integers")
        cursor.execute("""
            CREATE TABLE test_integers (
                id INTEGER PRIMARY KEY,
                tiny_val TINYINT,
                small_val SMALLINT,
                int_val INTEGER,
                big_val BIGINT,
                description VARCHAR(100)
            )
        """)
        
        # Create indexes to ensure values go to numeric annotations
        cursor.execute("CREATE INDEX idx_tiny ON test_integers(tiny_val)")
        cursor.execute("CREATE INDEX idx_small ON test_integers(small_val)")
        cursor.execute("CREATE INDEX idx_int ON test_integers(int_val)")
        cursor.execute("CREATE INDEX idx_big ON test_integers(big_val)")
        
        print("✅ Test table and indexes created")
        
        print("\n🔧 Testing INSERT with negative values...")
        test_data = [
            (1, -128, -32768, -2147483648, -4611686018427387904, "Min values (safe range)"),
            (2, -50, -1000, -100000, -123456789, "Regular negative values"),
            (3, -1, -1, -1, -1, "Negative one"),
            (4, 0, 0, 0, 0, "Zero values"),
            (5, 1, 1, 1, 1, "Positive one"),
            (6, 50, 1000, 100000, 123456789, "Regular positive values"),
            (7, 127, 32767, 2147483647, 4611686018427387903, "Max values (safe range)")
        ]
        
        for row in test_data:
            try:
                cursor.execute(
                    "INSERT INTO test_integers (id, tiny_val, small_val, int_val, big_val, description) VALUES (%(id)s, %(tiny_val)s, %(small_val)s, %(int_val)s, %(big_val)s, %(description)s)",
                    {
                        'id': row[0],
                        'tiny_val': row[1], 
                        'small_val': row[2],
                        'int_val': row[3],
                        'big_val': row[4],
                        'description': row[5]
                    }
                )
                print(f"  ✅ Inserted: {row[5]} (values: {row[1:5]})")
            except Exception as e:
                print(f"  ❌ Failed to insert {row[5]}: {e}")
                
        print("\n🔍 Testing SELECT with negative value conditions...")
        test_queries = [
            ("SELECT id, tiny_val FROM test_integers WHERE tiny_val = -50", "TINYINT equality"),
            ("SELECT id, small_val FROM test_integers WHERE small_val < 0", "SMALLINT negative"),
            ("SELECT id, int_val FROM test_integers WHERE int_val >= -100000", "INTEGER range"),
            ("SELECT id, big_val FROM test_integers WHERE big_val > -1000000", "BIGINT comparison"),
            ("SELECT id, description FROM test_integers WHERE tiny_val = -1 AND small_val = -1", "Multiple negative conditions"),
            ("SELECT id, tiny_val, small_val, int_val, big_val FROM test_integers ORDER BY tiny_val", "Ordering by TINYINT"),
            ("SELECT id, tiny_val, small_val, int_val, big_val FROM test_integers ORDER BY big_val", "Ordering by BIGINT")
        ]
        
        for query, description in test_queries:
            try:
                cursor.execute(query)
                results = cursor.fetchall()
                print(f"  ✅ {description}: Found {len(results)} rows")
                if results:
                    for row in results[:3]:  # Show first 3 results
                        print(f"     {row}")
                    if len(results) > 3:
                        print(f"     ... and {len(results) - 3} more rows")
            except Exception as e:
                print(f"  ❌ {description}: {e}")
                
        print("\n🎯 Testing ordering preservation...")
        cursor.execute("SELECT tiny_val FROM test_integers ORDER BY tiny_val")
        tiny_vals = [row[0] for row in cursor.fetchall()]
        print(f"  TINYINT ordering: {tiny_vals}")
        print(f"  ✅ Correctly ordered: {tiny_vals == sorted(tiny_vals)}")
        
        cursor.execute("SELECT big_val FROM test_integers ORDER BY big_val")
        big_vals = [row[0] for row in cursor.fetchall()]
        print(f"  BIGINT ordering: {big_vals[:3]}...{big_vals[-3:]}")
        print(f"  ✅ Correctly ordered: {big_vals == sorted(big_vals)}")
        
        print("\n✨ All negative integer tests completed successfully!")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        print(f"Error type: {type(e).__name__}")
        sys.exit(1)
        
    finally:
        cursor.close()
        conn.close()
        print("\n🔌 Connection closed")


if __name__ == "__main__":
    main()