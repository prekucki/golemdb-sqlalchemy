"""Main FastAPI application for testing GolemBase dialect."""

from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List
from pydantic import BaseModel
from .database import get_db, db_manager
from .crud import UserCRUD, PostCRUD, CategoryCRUD
from .models import User, Post, Category


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifespan events."""
    # Startup
    try:
        db_manager.create_engine()
        db_manager.create_tables()
        print("Database initialized successfully")
    except Exception as e:
        print(f"Failed to initialize database: {e}")
    
    yield
    
    # Shutdown
    db_manager.close()


app = FastAPI(
    title="GolemBase SQLAlchemy Test App",
    description="Test application for SQLAlchemy GolemBase dialect",
    version="0.1.0",
    lifespan=lifespan,
)


# Pydantic models for API
class UserCreate(BaseModel):
    username: str
    email: str
    full_name: str = None


class UserResponse(BaseModel):
    id: int
    username: str
    email: str
    full_name: str = None
    is_active: bool
    
    class Config:
        from_attributes = True


class PostCreate(BaseModel):
    title: str
    content: str
    author_id: int


class PostResponse(BaseModel):
    id: int
    title: str
    content: str
    author_id: int
    is_published: bool
    
    class Config:
        from_attributes = True




# Health check endpoint
@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "dialect": "golembase"}


# User endpoints
@app.post("/users/", response_model=UserResponse)
async def create_user(user: UserCreate, db: Session = Depends(get_db)):
    """Create a new user."""
    # Check if user already exists
    existing_user = UserCRUD.get_user_by_username(db, user.username)
    if existing_user:
        raise HTTPException(status_code=400, detail="Username already exists")
    
    existing_email = UserCRUD.get_user_by_email(db, user.email)
    if existing_email:
        raise HTTPException(status_code=400, detail="Email already exists")
    
    return UserCRUD.create_user(db, user.username, user.email, user.full_name)


@app.get("/users/", response_model=List[UserResponse])
async def get_users(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    """Get all users."""
    return UserCRUD.get_users(db, skip=skip, limit=limit)


@app.get("/users/{user_id}", response_model=UserResponse)
async def get_user(user_id: int, db: Session = Depends(get_db)):
    """Get user by ID."""
    user = UserCRUD.get_user(db, user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return user


@app.delete("/users/{user_id}")
async def delete_user(user_id: int, db: Session = Depends(get_db)):
    """Delete user by ID."""
    success = UserCRUD.delete_user(db, user_id)
    if not success:
        raise HTTPException(status_code=404, detail="User not found")
    return {"message": "User deleted successfully"}


# Post endpoints
@app.post("/posts/", response_model=PostResponse)
async def create_post(post: PostCreate, db: Session = Depends(get_db)):
    """Create a new post."""
    # Check if author exists
    author = UserCRUD.get_user(db, post.author_id)
    if not author:
        raise HTTPException(status_code=400, detail="Author not found")
    
    return PostCRUD.create_post(db, post.title, post.content, post.author_id)


@app.get("/posts/", response_model=List[PostResponse])
async def get_posts(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    """Get all posts."""
    return PostCRUD.get_posts(db, skip=skip, limit=limit)


@app.get("/posts/{post_id}", response_model=PostResponse)
async def get_post(post_id: int, db: Session = Depends(get_db)):
    """Get post by ID."""
    post = PostCRUD.get_post(db, post_id)
    if not post:
        raise HTTPException(status_code=404, detail="Post not found")
    return post


@app.get("/users/{user_id}/posts", response_model=List[PostResponse])
async def get_user_posts(user_id: int, db: Session = Depends(get_db)):
    """Get all posts by user."""
    # Check if user exists
    user = UserCRUD.get_user(db, user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    return PostCRUD.get_posts_by_author(db, user_id)


# Database testing endpoints
@app.get("/test/connection")
async def test_connection(db: Session = Depends(get_db)):
    """Test database connection."""
    try:
        # Simple query to test connection
        result = db.execute("SELECT 1 as test_value")
        row = result.fetchone()
        return {"status": "success", "test_value": row[0] if row else None}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database connection failed: {str(e)}")


@app.get("/test/schema")
async def test_schema_inspection():
    """Test schema inspection capabilities."""
    try:
        from sqlalchemy import inspect
        inspector = inspect(db_manager.engine)
        
        return {
            "tables": inspector.get_table_names(),
            "schemas": inspector.get_schema_names() if hasattr(inspector, 'get_schema_names') else [],
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Schema inspection failed: {str(e)}")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)