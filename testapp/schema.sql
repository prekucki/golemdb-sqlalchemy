-- ============================================================================
-- GolemBase TestApp Database Schema
-- ============================================================================
-- This schema defines the database structure for the SQLAlchemy GolemBase
-- dialect test application. It includes tables for users, posts, and categories
-- to test various database operations and relationships.
-- ============================================================================

-- ============================================================================
-- Table: users
-- ============================================================================
-- Stores user account information for testing basic CRUD operations,
-- unique constraints, and boolean/datetime handling.

CREATE TABLE users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username VARCHAR(50) NOT NULL,
    email VARCHAR(100) NOT NULL,
    full_name VARCHAR(100),
    is_active BOOLEAN DEFAULT TRUE,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Unique constraints
ALTER TABLE users ADD CONSTRAINT uk_users_username UNIQUE (username);
ALTER TABLE users ADD CONSTRAINT uk_users_email UNIQUE (email);

-- Performance indexes
CREATE INDEX idx_users_is_active ON users(is_active);
CREATE INDEX idx_users_created_at ON users(created_at);

-- ============================================================================
-- Table: posts
-- ============================================================================
-- Stores blog posts or articles, demonstrating foreign key relationships,
-- text fields, and one-to-many relationships with users.

CREATE TABLE posts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title VARCHAR(200) NOT NULL,
    content TEXT,
    author_id INTEGER NOT NULL,
    is_published BOOLEAN DEFAULT FALSE,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Foreign key constraints
ALTER TABLE posts ADD CONSTRAINT fk_posts_author_id 
    FOREIGN KEY (author_id) REFERENCES users(id) 
    ON DELETE CASCADE ON UPDATE CASCADE;

-- Performance indexes
CREATE INDEX idx_posts_author_id ON posts(author_id);
CREATE INDEX idx_posts_is_published ON posts(is_published);
CREATE INDEX idx_posts_created_at ON posts(created_at DESC);
CREATE INDEX idx_posts_title ON posts(title);

-- ============================================================================
-- Table: categories
-- ============================================================================
-- Stores content categories, demonstrating additional table structures
-- and testing category-based operations.

CREATE TABLE categories (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name VARCHAR(50) NOT NULL,
    description TEXT,
    is_active BOOLEAN DEFAULT TRUE,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Unique constraints
ALTER TABLE categories ADD CONSTRAINT uk_categories_name UNIQUE (name);

-- Performance indexes
CREATE INDEX idx_categories_is_active ON categories(is_active);
CREATE INDEX idx_categories_created_at ON categories(created_at);

-- ============================================================================
-- Sample Data for Testing
-- ============================================================================
-- Insert sample data to test the GolemBase dialect functionality

-- Sample users
INSERT INTO users (username, email, full_name, is_active) VALUES
    ('admin', 'admin@example.com', 'System Administrator', TRUE),
    ('john_doe', 'john@example.com', 'John Doe', TRUE),
    ('jane_smith', 'jane@example.com', 'Jane Smith', TRUE),
    ('inactive_user', 'inactive@example.com', 'Inactive User', FALSE);

-- Sample categories
INSERT INTO categories (name, description, is_active) VALUES
    ('Technology', 'Posts about technology and programming', TRUE),
    ('Science', 'Scientific articles and research', TRUE),
    ('Lifestyle', 'Lifestyle and personal development', TRUE),
    ('Archive', 'Archived category', FALSE);

-- Sample posts
INSERT INTO posts (title, content, author_id, is_published) VALUES
    ('Welcome to GolemBase', 
     'This is a welcome post demonstrating the GolemBase SQLAlchemy dialect functionality. It shows how we can store and retrieve text content efficiently.',
     1, TRUE),
    
    ('Getting Started with SQLAlchemy',
     'SQLAlchemy is a powerful ORM for Python. This post explains the basics of using SQLAlchemy with custom database dialects like GolemBase.',
     2, TRUE),
    
    ('Database Performance Tips',
     'Here are some tips for optimizing database performance when using GolemBase with SQLAlchemy. Proper indexing and query optimization are key.',
     2, FALSE),
    
    ('Testing Database Connections',
     'This post demonstrates how to test database connections and ensure your GolemBase setup is working correctly.',
     3, TRUE),
    
    ('Draft Post',
     'This is a draft post that has not been published yet. It can be used to test filtering by publication status.',
     1, FALSE);

-- ============================================================================
-- Views for Testing
-- ============================================================================
-- Create views to test view introspection and complex queries

-- View: Active users with post counts
CREATE VIEW active_users_with_posts AS
SELECT 
    u.id,
    u.username,
    u.email,
    u.full_name,
    u.created_at,
    COUNT(p.id) as post_count,
    COUNT(CASE WHEN p.is_published = TRUE THEN 1 END) as published_post_count
FROM users u
LEFT JOIN posts p ON u.id = p.author_id
WHERE u.is_active = TRUE
GROUP BY u.id, u.username, u.email, u.full_name, u.created_at;

-- View: Published posts with author info
CREATE VIEW published_posts_view AS
SELECT 
    p.id,
    p.title,
    p.content,
    p.created_at as post_created_at,
    u.username as author_username,
    u.full_name as author_full_name,
    u.email as author_email
FROM posts p
JOIN users u ON p.author_id = u.id
WHERE p.is_published = TRUE
ORDER BY p.created_at DESC;

-- ============================================================================
-- Database Statistics and Info Queries
-- ============================================================================
-- Queries to check database state and test the dialect

-- Check table row counts
SELECT 'users' as table_name, COUNT(*) as row_count FROM users
UNION ALL
SELECT 'posts' as table_name, COUNT(*) as row_count FROM posts
UNION ALL
SELECT 'categories' as table_name, COUNT(*) as row_count FROM categories;

-- Check foreign key relationships
SELECT 
    u.username,
    COUNT(p.id) as total_posts,
    COUNT(CASE WHEN p.is_published = TRUE THEN 1 END) as published_posts
FROM users u
LEFT JOIN posts p ON u.id = p.author_id
WHERE u.is_active = TRUE
GROUP BY u.id, u.username
ORDER BY total_posts DESC;

-- ============================================================================
-- Test Queries for Dialect Validation
-- ============================================================================
-- These queries can be used to test various aspects of the GolemBase dialect

-- Test basic SELECT with WHERE clause
-- SELECT * FROM users WHERE is_active = TRUE;

-- Test JOIN operations
-- SELECT u.username, p.title FROM users u JOIN posts p ON u.id = p.author_id;

-- Test aggregate functions
-- SELECT author_id, COUNT(*) as post_count FROM posts GROUP BY author_id;

-- Test ORDER BY and LIMIT
-- SELECT * FROM posts ORDER BY created_at DESC LIMIT 5;

-- Test LIKE operations
-- SELECT * FROM users WHERE username LIKE 'john%';

-- Test date operations
-- SELECT * FROM posts WHERE created_at > '2023-01-01';

-- Test boolean operations
-- SELECT * FROM users WHERE is_active = TRUE AND full_name IS NOT NULL;

-- Test text search
-- SELECT * FROM posts WHERE content LIKE '%SQLAlchemy%';

-- ============================================================================
-- End of Schema
-- ============================================================================