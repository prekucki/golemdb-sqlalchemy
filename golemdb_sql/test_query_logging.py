#!/usr/bin/env python3
"""Test script to verify query logging shows full GolemBase queries."""

import golemdb_sql
import sys
import os
import logging
from dotenv import load_dotenv

def main():
    """Test query logging with a simple SELECT."""
    
    # Configure debug logging
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    
    # Enable debug logging for our modules
    logging.getLogger('golemdb_sql.cursor').setLevel(logging.DEBUG)
    logging.getLogger('golemdb_sql.connection').setLevel(logging.DEBUG)
    logging.getLogger('golemdb_sql.query_translator').setLevel(logging.DEBUG)
    logging.getLogger('golem_base_sdk').setLevel(logging.INFO)  # Reduce noise
    
    # Load environment variables
    load_dotenv()
    
    private_key = os.getenv('PRIVATE_KEY')
    rpc_url = os.getenv('RPC_URL', 'https://ethwarsaw.holesky.golemdb.io/rpc')
    ws_url = os.getenv('WS_URL', 'wss://ethwarsaw.holesky.golemdb.io/rpc/ws')
    app_id = os.getenv('APP_ID', 'demo_app')
    schema_id = 'query_logging_test'
    
    if not private_key:
        print("❌ PRIVATE_KEY not found")
        sys.exit(1)
    
    print(f"🧪 Testing query logging...")
    print(f"🏷️  App ID: {app_id}")
    print(f"📋 Schema ID: {schema_id}")
    
    conn = golemdb_sql.connect(
        rpc_url=rpc_url,
        ws_url=ws_url,
        private_key=private_key,
        app_id=app_id,
        schema_id=schema_id
    )
    
    try:
        cursor = conn.cursor()
        
        print("\n🔍 Testing simple SELECT query...")
        cursor.execute("SELECT * FROM users WHERE id = 1")
        results = cursor.fetchall()
        print(f"Results: {len(results)} rows")
        
        print("\n🔍 Testing SELECT with negative integer condition...")
        cursor.execute("SELECT * FROM users WHERE age < 0")
        results = cursor.fetchall()
        print(f"Results: {len(results)} rows")
        
    except Exception as e:
        print(f"Error: {e}")
        
    finally:
        cursor.close()
        conn.close()
        print("\n🔌 Connection closed")

if __name__ == "__main__":
    main()