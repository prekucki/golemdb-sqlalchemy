"""CRUD operations using direct GolemBase connection."""

from typing import List, Optional, Dict, Any
from .database import get_golembase_connection

class GolemBaseUserCRUD:
    """User CRUD operations using direct GolemBase connection."""
    
    @staticmethod
    def create_user(username: str, email: str, full_name: str = None) -> Dict[str, Any]:
        """Create a new user using GolemBase."""
        conn = get_golembase_connection()
        cursor = conn.cursor()
        
        try:
            # Create users table if it doesn't exist
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY,
                    username VARCHAR(100) NOT NULL,
                    email VARCHAR(200) NOT NULL,
                    full_name VARCHAR(200),
                    is_active BOOLEAN DEFAULT TRUE,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create indexes
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_users_username ON users(username)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_users_email ON users(email)")
            
            # Generate ID (simple approach)
            import time
            user_id = int(time.time() * 1000) % 2147483647  # Keep within int32 range
            
            # Insert user
            cursor.execute("""
                INSERT INTO users (id, username, email, full_name, is_active) 
                VALUES (%(id)s, %(username)s, %(email)s, %(full_name)s, %(is_active)s)
            """, {
                'id': user_id,
                'username': username,
                'email': email,
                'full_name': full_name,
                'is_active': True
            })
            
            return {
                'id': user_id,
                'username': username,
                'email': email,
                'full_name': full_name,
                'is_active': True
            }
            
        finally:
            cursor.close()
    
    @staticmethod
    def get_users(skip: int = 0, limit: int = 100) -> List[Dict[str, Any]]:
        """Get all users using GolemBase."""
        conn = get_golembase_connection()
        cursor = conn.cursor()
        
        try:
            # Simple SELECT - GolemBase will handle the query
            cursor.execute("SELECT id, username, email, full_name, is_active FROM users LIMIT %(limit)s OFFSET %(skip)s", {
                'limit': limit,
                'skip': skip
            })
            
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
    
    @staticmethod
    def get_user_by_id(user_id: int) -> Optional[Dict[str, Any]]:
        """Get user by ID using GolemBase."""
        conn = get_golembase_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("SELECT id, username, email, full_name, is_active FROM users WHERE id = %(id)s", {
                'id': user_id
            })
            
            row = cursor.fetchone()
            if row:
                return {
                    'id': row[0],
                    'username': row[1],
                    'email': row[2],
                    'full_name': row[3],
                    'is_active': row[4]
                }
            return None
            
        finally:
            cursor.close()
    
    @staticmethod
    def get_user_by_username(username: str) -> Optional[Dict[str, Any]]:
        """Get user by username using GolemBase."""
        conn = get_golembase_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("SELECT id, username, email, full_name, is_active FROM users WHERE username = %(username)s", {
                'username': username
            })
            
            row = cursor.fetchone()
            if row:
                return {
                    'id': row[0],
                    'username': row[1],
                    'email': row[2],
                    'full_name': row[3],
                    'is_active': row[4]
                }
            return None
            
        finally:
            cursor.close()
    
    @staticmethod
    def delete_user(user_id: int) -> bool:
        """Delete user by ID using GolemBase."""
        conn = get_golembase_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("DELETE FROM users WHERE id = %(id)s", {
                'id': user_id
            })
            
            # For now, assume success if no exception
            return True
            
        except Exception:
            return False
        finally:
            cursor.close()