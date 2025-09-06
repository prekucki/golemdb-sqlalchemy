"""CRUD operations for testing GolemBase dialect functionality."""

from sqlalchemy.orm import Session
from typing import List, Optional
from .models import User, Post, Category


class UserCRUD:
    """CRUD operations for User model."""
    
    @staticmethod
    def create_user(db: Session, username: str, email: str, full_name: str = None) -> User:
        """Create a new user."""
        user = User(username=username, email=email, full_name=full_name)
        db.add(user)
        db.commit()
        db.refresh(user)
        return user
    
    @staticmethod
    def get_user(db: Session, user_id: int) -> Optional[User]:
        """Get user by ID."""
        return db.query(User).filter(User.id == user_id).first()
    
    @staticmethod
    def get_user_by_username(db: Session, username: str) -> Optional[User]:
        """Get user by username."""
        return db.query(User).filter(User.username == username).first()
    
    @staticmethod
    def get_user_by_email(db: Session, email: str) -> Optional[User]:
        """Get user by email."""
        return db.query(User).filter(User.email == email).first()
    
    @staticmethod
    def get_users(db: Session, skip: int = 0, limit: int = 100) -> List[User]:
        """Get all users with pagination."""
        return db.query(User).offset(skip).limit(limit).all()
    
    @staticmethod
    def update_user(db: Session, user_id: int, **kwargs) -> Optional[User]:
        """Update user by ID."""
        user = db.query(User).filter(User.id == user_id).first()
        if user:
            for key, value in kwargs.items():
                if hasattr(user, key):
                    setattr(user, key, value)
            db.commit()
            db.refresh(user)
        return user
    
    @staticmethod
    def delete_user(db: Session, user_id: int) -> bool:
        """Delete user by ID."""
        user = db.query(User).filter(User.id == user_id).first()
        if user:
            db.delete(user)
            db.commit()
            return True
        return False


class PostCRUD:
    """CRUD operations for Post model."""
    
    @staticmethod
    def create_post(db: Session, title: str, content: str, author_id: int) -> Post:
        """Create a new post."""
        post = Post(title=title, content=content, author_id=author_id)
        db.add(post)
        db.commit()
        db.refresh(post)
        return post
    
    @staticmethod
    def get_post(db: Session, post_id: int) -> Optional[Post]:
        """Get post by ID."""
        return db.query(Post).filter(Post.id == post_id).first()
    
    @staticmethod
    def get_posts(db: Session, skip: int = 0, limit: int = 100) -> List[Post]:
        """Get all posts with pagination."""
        return db.query(Post).offset(skip).limit(limit).all()
    
    @staticmethod
    def get_posts_by_author(db: Session, author_id: int) -> List[Post]:
        """Get all posts by author."""
        return db.query(Post).filter(Post.author_id == author_id).all()
    
    @staticmethod
    def update_post(db: Session, post_id: int, **kwargs) -> Optional[Post]:
        """Update post by ID."""
        post = db.query(Post).filter(Post.id == post_id).first()
        if post:
            for key, value in kwargs.items():
                if hasattr(post, key):
                    setattr(post, key, value)
            db.commit()
            db.refresh(post)
        return post
    
    @staticmethod
    def delete_post(db: Session, post_id: int) -> bool:
        """Delete post by ID."""
        post = db.query(Post).filter(Post.id == post_id).first()
        if post:
            db.delete(post)
            db.commit()
            return True
        return False


class CategoryCRUD:
    """CRUD operations for Category model."""
    
    @staticmethod
    def create_category(db: Session, name: str, description: str = None) -> Category:
        """Create a new category."""
        category = Category(name=name, description=description)
        db.add(category)
        db.commit()
        db.refresh(category)
        return category
    
    @staticmethod
    def get_category(db: Session, category_id: int) -> Optional[Category]:
        """Get category by ID."""
        return db.query(Category).filter(Category.id == category_id).first()
    
    @staticmethod
    def get_categories(db: Session, skip: int = 0, limit: int = 100) -> List[Category]:
        """Get all categories with pagination."""
        return db.query(Category).offset(skip).limit(limit).all()
    
    @staticmethod
    def update_category(db: Session, category_id: int, **kwargs) -> Optional[Category]:
        """Update category by ID."""
        category = db.query(Category).filter(Category.id == category_id).first()
        if category:
            for key, value in kwargs.items():
                if hasattr(category, key):
                    setattr(category, key, value)
            db.commit()
            db.refresh(category)
        return category
    
    @staticmethod
    def delete_category(db: Session, category_id: int) -> bool:
        """Delete category by ID."""
        category = db.query(Category).filter(Category.id == category_id).first()
        if category:
            db.delete(category)
            db.commit()
            return True
        return False