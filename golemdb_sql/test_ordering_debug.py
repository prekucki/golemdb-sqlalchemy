#!/usr/bin/env python3
"""Debug script to test ordering preservation with detailed logging."""

import golemdb_sql
import sys
import os
import logging
from dotenv import load_dotenv


def main():
    """Test ordering preservation with debug logging."""
    
    # Configure debug logging
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    
    # Enable debug logging
    logging.getLogger('golemdb_sql.query_translator').setLevel(logging.DEBUG)
    logging.getLogger('golemdb_sql.schema_manager').setLevel(logging.DEBUG)
    logging.getLogger('golem_base_sdk').setLevel(logging.INFO)
    
    # Load environment variables
    load_dotenv()
    
    # Get configuration from environment
    private_key = os.getenv('PRIVATE_KEY')
    rpc_url = os.getenv('RPC_URL', 'https://ethwarsaw.holesky.golemdb.io/rpc')
    ws_url = os.getenv('WS_URL', 'wss://ethwarsaw.holesky.golemdb.io/rpc/ws')
    app_id = os.getenv('APP_ID', 'demo_app')
    schema_id = 'ordering_debug_test'
    
    if not private_key:
        print("❌ PRIVATE_KEY not found in environment variables")
        sys.exit(1)
    
    print("🧪 Testing ordering preservation with debug logging...")
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
        
        print("\n📋 Setting up minimal test...")
        cursor.execute("DROP TABLE IF EXISTS order_test")
        cursor.execute("""
            CREATE TABLE order_test (
                id INTEGER PRIMARY KEY,
                test_val TINYINT
            )
        """)
        cursor.execute("CREATE INDEX idx_test_val ON order_test(test_val)")
        
        print("\n🔧 Inserting ordered test values...")
        test_values = [-10, -1, 0, 1, 10]
        for i, val in enumerate(test_values, 1):
            cursor.execute(
                "INSERT INTO order_test (id, test_val) VALUES (%(id)s, %(test_val)s)",
                {'id': i, 'test_val': val}
            )
            print(f"  Inserted: id={i}, test_val={val}")
        
        print("\n🔍 Testing ORDER BY query...")
        cursor.execute("SELECT id, test_val FROM order_test ORDER BY test_val")
        results = cursor.fetchall()
        
        print("Results from ORDER BY test_val:")
        retrieved_values = []
        for row in results:
            print(f"  id={row[0]}, test_val={row[1]}")
            retrieved_values.append(row[1])
        
        print(f"\nOriginal order: {test_values}")
        print(f"Retrieved order: {retrieved_values}")
        print(f"Correctly ordered: {retrieved_values == sorted(test_values)}")
        
        if retrieved_values != sorted(test_values):
            print("\n⚠️  ORDERING ISSUE DETECTED!")
            
            # Let's check the encoding manually
            from golemdb_sql.types import encode_signed_to_uint64
            print("\nManual encoding check:")
            for val in test_values:
                encoded = encode_signed_to_uint64(val, 8)
                print(f"  {val:>3} -> {encoded:>3} (binary: {bin(encoded)})")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
        
    finally:
        cursor.close()
        conn.close()
        print("\n🔌 Connection closed")


if __name__ == "__main__":
    main()