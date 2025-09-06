#!/usr/bin/env python3
"""
Standalone script to initialize GolemBase connections and store them.
Run this before starting the web server.
"""

import sys
import os
import pickle
import tempfile

# Add testapp to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

def main():
    """Initialize GolemBase connection and save it to a file."""
    try:
        print("🔗 Initializing GolemBase connection in main thread...")
        
        # Import and initialize
        from testapp.database import initialize_connection_pool, _connection_pool
        initialize_connection_pool()
        
        # Save connection to a temporary file
        temp_file = os.path.join(tempfile.gettempdir(), 'golembase_connection.pkl')
        with open(temp_file, 'wb') as f:
            pickle.dump(_connection_pool, f)
        
        print(f"✅ Connection saved to: {temp_file}")
        print("🚀 Now you can start the web server with: python -m uvicorn testapp.main:app --host 0.0.0.0 --port 8000")
        
        return 0
        
    except Exception as e:
        print(f"❌ Error initializing connection: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())