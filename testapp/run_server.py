#!/usr/bin/env python3
"""
Server startup script that initializes GolemBase connections in the main thread
before starting Uvicorn.
"""

import sys
import os

# Add testapp to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

def main():
    """Initialize connections and start the server."""
    try:
        print("🚀 Starting server with GolemBase connection pool...")
        
        # Initialize GolemBase connection pool in main thread
        print("📡 Initializing GolemBase connection pool in main thread...")
        from testapp.database import initialize_connection_pool
        initialize_connection_pool()
        print("✅ Connection pool initialized successfully!")
        
        # Import the app after connection pool is initialized
        from testapp.main import app
        
        # Start Uvicorn server with the app instance directly
        import uvicorn
        print("🌐 Starting Uvicorn server...")
        uvicorn.run(
            app,  # Pass the app instance, not the module string
            host="0.0.0.0", 
            port=8000,
            reload=False,  # Disable reload to avoid reinitializing connections
            log_level="info"
        )
        
    except KeyboardInterrupt:
        print("\n🛑 Server stopped by user")
    except Exception as e:
        print(f"❌ Error starting server: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())