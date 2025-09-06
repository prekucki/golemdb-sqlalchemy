#!/usr/bin/env python3
"""
Simple HTTP API server that works with GolemBase.
This uses a standalone process approach that respects signal handler requirements.
"""

import sys
import os
import json
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
import threading

# Add golemdb_sql to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'golemdb_sql', 'src'))

# Global connection - initialized in main thread
golembase_conn = None

class GolemBaseAPIHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        """Handle GET requests."""
        parsed_path = urlparse(self.path)
        path = parsed_path.path
        
        try:
            if path == '/health':
                self.send_json_response({"status": "healthy", "thread": threading.current_thread().name})
            elif path == '/test/golembase':
                result = self.test_connection()
                self.send_json_response(result)
            elif path == '/users':
                users = self.get_users()
                self.send_json_response(users)
            else:
                self.send_error(404, "Not Found")
        except Exception as e:
            self.send_error(500, f"Internal Server Error: {str(e)}")
    
    def do_POST(self):
        """Handle POST requests."""
        parsed_path = urlparse(self.path)
        path = parsed_path.path
        
        try:
            if path == '/users':
                content_length = int(self.headers['Content-Length'])
                post_data = self.rfile.read(content_length)
                user_data = json.loads(post_data.decode('utf-8'))
                
                result = self.create_user(user_data)
                self.send_json_response(result)
            else:
                self.send_error(404, "Not Found")
        except Exception as e:
            self.send_error(500, f"Internal Server Error: {str(e)}")
    
    def test_connection(self):
        """Test the GolemBase connection."""
        cursor = golembase_conn.cursor()
        try:
            cursor.execute("SELECT 1 as test_value")
            result = cursor.fetchone()
            return {
                "status": "success",
                "test_value": result[0] if result else None,
                "message": "GolemBase connection working",
                "thread": threading.current_thread().name
            }
        finally:
            cursor.close()
    
    def create_user(self, user_data):
        """Create a user."""
        cursor = golembase_conn.cursor()
        try:
            # Try to create table, ignore if it already exists
            try:
                cursor.execute("""
                    CREATE TABLE users (
                        id INTEGER PRIMARY KEY,
                        username VARCHAR(100) NOT NULL,
                        email VARCHAR(200) NOT NULL,
                        full_name VARCHAR(200),
                        is_active BOOLEAN DEFAULT TRUE
                    )
                """)
                print("✅ Created users table")
            except Exception as e:
                if "already exist" in str(e):
                    print("📋 Users table already exists")
                else:
                    print(f"⚠️  Table creation warning: {e}")
            
            # Try to create indexes
            try:
                cursor.execute("CREATE INDEX idx_users_username ON users(username)")
                cursor.execute("CREATE INDEX idx_users_email ON users(email)")
                print("✅ Created user indexes")
            except Exception as e:
                if "already exist" in str(e):
                    print("📋 User indexes already exist")
                else:
                    print(f"⚠️  Index creation warning: {e}")
            
            # Generate ID
            import time
            import random
            user_id = int(time.time() * 1000) + random.randint(1, 1000)
            user_id = user_id % 2147483647  # Keep within int32 range
            
            # Insert user
            cursor.execute("""
                INSERT INTO users (id, username, email, full_name, is_active) 
                VALUES (%(id)s, %(username)s, %(email)s, %(full_name)s, %(is_active)s)
            """, {
                'id': user_id,
                'username': user_data['username'],
                'email': user_data['email'],
                'full_name': user_data.get('full_name'),
                'is_active': True
            })
            
            print(f"✅ Created user: {user_data['username']} (ID: {user_id})")
            
            return {
                'id': user_id,
                'username': user_data['username'],
                'email': user_data['email'],
                'full_name': user_data.get('full_name'),
                'is_active': True
            }
            
        finally:
            cursor.close()
    
    def get_users(self):
        """Get all users."""
        cursor = golembase_conn.cursor()
        try:
            cursor.execute("SELECT id, username, email, full_name, is_active FROM users LIMIT 100")
            users = []
            for row in cursor.fetchall():
                users.append({
                    'id': row[0],
                    'username': row[1],
                    'email': row[2],
                    'full_name': row[3],
                    'is_active': row[4]
                })
            return users
        finally:
            cursor.close()
    
    def send_json_response(self, data):
        """Send JSON response."""
        json_data = json.dumps(data, indent=2)
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.send_header('Content-length', len(json_data))
        self.end_headers()
        self.wfile.write(json_data.encode('utf-8'))

def main():
    """Initialize GolemBase and start server."""
    global golembase_conn
    
    print("🚀 Starting GolemBase API Server...")
    print(f"🧵 Main thread: {threading.current_thread().name}")
    
    try:
        # Initialize GolemBase connection in main thread
        import golemdb_sql
        from dotenv import load_dotenv
        
        load_dotenv()
        
        private_key = os.environ.get('PRIVATE_KEY')
        rpc_url = os.environ.get('RPC_URL', 'https://ethwarsaw.holesky.golemdb.io/rpc')
        ws_url = os.environ.get('WS_URL', 'wss://ethwarsaw.holesky.golemdb.io/rpc/ws')
        app_id = os.environ.get('APP_ID', 'simple_api')
        schema_id = os.environ.get('SCHEMA_ID', 'simple_api_schema')
        
        if not private_key:
            print("❌ PRIVATE_KEY environment variable is required")
            return 1
        
        print("🔗 Initializing GolemBase connection...")
        print(f"📡 RPC URL: {rpc_url}")
        print(f"🏷️  App ID: {app_id}")
        print(f"📋 Schema ID: {schema_id}")
        
        golembase_conn = golemdb_sql.connect(
            rpc_url=rpc_url,
            ws_url=ws_url,
            private_key=private_key,
            app_id=app_id,
            schema_id=schema_id
        )
        print("✅ GolemBase connection initialized successfully!")
        
        # Start HTTP server (single-threaded, same process)
        server = HTTPServer(('0.0.0.0', 8000), GolemBaseAPIHandler)
        print("🌐 HTTP Server running on http://0.0.0.0:8000")
        print("📋 Available endpoints:")
        print("   GET  /health")
        print("   GET  /test/golembase")
        print("   GET  /users")
        print("   POST /users")
        print("\n🔄 Server running... Press Ctrl+C to stop")
        
        server.serve_forever()
        
    except KeyboardInterrupt:
        print("\n🛑 Server stopped by user")
        return 0
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1
    finally:
        if golembase_conn:
            golembase_conn.close()

if __name__ == "__main__":
    sys.exit(main())