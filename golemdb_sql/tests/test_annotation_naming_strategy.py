"""Test the annotation naming strategy with multi-tenant project isolation."""

import pytest
from datetime import datetime
from golemdb_sql.schema_manager import (
    SchemaManager, TableDefinition, ColumnDefinition, IndexDefinition
)
from golemdb_sql.query_translator import QueryTranslator
from golemdb_sql.types import encode_signed_to_uint64


class TestAnnotationNamingStrategy:
    """Test the idx_ prefix and relation-based annotation naming."""
    
    @pytest.fixture
    def posts_schema_manager(self):
        """Create schema manager with posts table schema."""
        sm = SchemaManager(schema_id="test_schema", project_id="myproject")
        
        # Create posts table matching the SQL schema from the user
        posts_table = TableDefinition(
            name="posts",
            columns=[
                ColumnDefinition(name="id", type="INTEGER", primary_key=True),
                ColumnDefinition(name="title", type="VARCHAR(200)", nullable=False, indexed=True),
                ColumnDefinition(name="content", type="TEXT"),
                ColumnDefinition(name="author_id", type="INTEGER", nullable=False, indexed=True),
                ColumnDefinition(name="is_published", type="BOOLEAN", default=False, indexed=True),
                ColumnDefinition(name="created_at", type="DATETIME", indexed=True),
                ColumnDefinition(name="updated_at", type="DATETIME")
            ],
            indexes=[
                IndexDefinition(name="idx_posts_author_id", columns=["author_id"]),
                IndexDefinition(name="idx_posts_is_published", columns=["is_published"]),
                IndexDefinition(name="idx_posts_created_at", columns=["created_at"], desc=True),
                IndexDefinition(name="idx_posts_title", columns=["title"])
            ],
            foreign_keys=[]
        )
        
        sm.add_table(posts_table)
        return sm
    
    def test_metadata_annotations_generation(self, posts_schema_manager):
        """Test that metadata annotations are correctly generated."""
        row_data = {
            "id": 1,
            "title": "My First Blog Post",
            "content": "This is the content of my blog post...",
            "author_id": 42,
            "is_published": True,
            "created_at": datetime(2024, 1, 15, 10, 30, 0),
            "updated_at": datetime(2024, 1, 15, 10, 30, 0)
        }
        
        annotations = posts_schema_manager.get_entity_annotations_for_table("posts", row_data)
        
        # Check metadata annotations
        string_annotations = annotations["string_annotations"]
        assert string_annotations["row_type"] == "json"
        assert string_annotations["relation"] == "myproject.posts"
    
    def test_indexed_columns_with_idx_prefix(self, posts_schema_manager):
        """Test that indexed columns use idx_ prefix."""
        row_data = {
            "id": 1,
            "title": "My First Blog Post", 
            "content": "This is the content...",
            "author_id": 42,
            "is_published": True,
            "created_at": datetime(2024, 1, 15, 10, 30, 0)
        }
        
        annotations = posts_schema_manager.get_entity_annotations_for_table("posts", row_data)
        
        string_annotations = annotations["string_annotations"]
        numeric_annotations = annotations["numeric_annotations"]
        
        # Check that indexed columns have idx_ prefix
        assert "idx_title" in string_annotations
        assert string_annotations["idx_title"] == "My First Blog Post"
        
        assert "idx_id" in numeric_annotations
        assert "idx_author_id" in numeric_annotations
        assert "idx_is_published" in numeric_annotations
        assert "idx_created_at" in numeric_annotations
        
        # Check encoded values
        expected_id = encode_signed_to_uint64(1, 32)
        expected_author_id = encode_signed_to_uint64(42, 32)
        
        assert numeric_annotations["idx_id"] == expected_id
        assert numeric_annotations["idx_author_id"] == expected_author_id
        assert numeric_annotations["idx_is_published"] == 1  # Boolean true
        assert isinstance(numeric_annotations["idx_created_at"], int)  # Unix timestamp
    
    def test_non_indexed_columns_excluded(self, posts_schema_manager):
        """Test that non-indexed columns are not included in annotations."""
        row_data = {
            "id": 1,
            "title": "Test Post",
            "content": "This is not indexed content",
            "author_id": 42,
            "is_published": False,
            "created_at": datetime.now(),
            "updated_at": datetime.now()  # This is not indexed
        }
        
        annotations = posts_schema_manager.get_entity_annotations_for_table("posts", row_data)
        
        string_annotations = annotations["string_annotations"]
        numeric_annotations = annotations["numeric_annotations"]
        
        # content is not indexed, should not appear
        assert "idx_content" not in string_annotations
        assert "content" not in string_annotations
        
        # updated_at is not indexed, should not appear  
        assert "idx_updated_at" not in numeric_annotations
        assert "updated_at" not in numeric_annotations
    
    def test_multi_tenant_isolation(self):
        """Test that different projects generate different relation annotations.""" 
        # Project A
        sm_a = SchemaManager(schema_id="schema", project_id="projectA")
        posts_table_a = TableDefinition(
            name="posts",
            columns=[ColumnDefinition(name="id", type="INTEGER", primary_key=True)],
            indexes=[],
            foreign_keys=[]
        )
        sm_a.add_table(posts_table_a)
        
        # Project B  
        sm_b = SchemaManager(schema_id="schema", project_id="projectB")
        posts_table_b = TableDefinition(
            name="posts", 
            columns=[ColumnDefinition(name="id", type="INTEGER", primary_key=True)],
            indexes=[],
            foreign_keys=[]
        )
        sm_b.add_table(posts_table_b)
        
        row_data = {"id": 1}
        
        annotations_a = sm_a.get_entity_annotations_for_table("posts", row_data)
        annotations_b = sm_b.get_entity_annotations_for_table("posts", row_data)
        
        # Should have different relation annotations
        assert annotations_a["string_annotations"]["relation"] == "projectA.posts"
        assert annotations_b["string_annotations"]["relation"] == "projectB.posts"
        
        # But same row_type
        assert annotations_a["string_annotations"]["row_type"] == "json"
        assert annotations_b["string_annotations"]["row_type"] == "json"


class TestQueryTranslationWithNewNaming:
    """Test query translation using the new annotation naming strategy."""
    
    @pytest.fixture
    def query_translator(self):
        """Create query translator with posts schema."""
        sm = SchemaManager(schema_id="test_schema", project_id="blogapp") 
        
        posts_table = TableDefinition(
            name="posts",
            columns=[
                ColumnDefinition(name="id", type="INTEGER", primary_key=True),
                ColumnDefinition(name="title", type="VARCHAR(200)", indexed=True),
                ColumnDefinition(name="author_id", type="INTEGER", indexed=True),
                ColumnDefinition(name="is_published", type="BOOLEAN", indexed=True),
                ColumnDefinition(name="created_at", type="DATETIME", indexed=True)
            ],
            indexes=[],
            foreign_keys=[]
        )
        
        sm.add_table(posts_table)
        return QueryTranslator(sm)
    
    def test_simple_query_with_relation_filter(self, query_translator):
        """Test that queries include relation filter."""
        result = query_translator.translate_select("SELECT * FROM posts")
        
        # Should filter by relation (project.table)
        assert 'relation="blogapp.posts"' in result.golem_query
        assert result.table_name == "posts"
    
    def test_where_clause_with_idx_prefix(self, query_translator):
        """Test WHERE clauses use idx_ prefix for column annotations."""
        result = query_translator.translate_select("SELECT * FROM posts WHERE id = 42")
        
        # Should include relation filter and idx_ prefixed column
        expected_id = encode_signed_to_uint64(42, 32)
        expected_query = f'relation="blogapp.posts" && (idx_id={expected_id})'
        assert result.golem_query == expected_query
    
    def test_multiple_conditions_with_idx_prefix(self, query_translator):
        """Test multiple WHERE conditions all use idx_ prefix."""
        result = query_translator.translate_select(
            "SELECT * FROM posts WHERE author_id = 1 AND is_published = true"
        )
        
        expected_author_id = encode_signed_to_uint64(1, 32)
        query = result.golem_query
        
        # Should have relation filter and both conditions with idx_ prefix
        assert 'relation="blogapp.posts"' in query
        assert f'idx_author_id={expected_author_id}' in query
        assert 'idx_is_published=1' in query
        assert '&&' in query
    
    def test_string_column_query_with_idx_prefix(self, query_translator):
        """Test string column queries use idx_ prefix."""
        result = query_translator.translate_select(
            'SELECT * FROM posts WHERE title = "My Blog Post"'
        )
        
        query = result.golem_query
        assert 'relation="blogapp.posts"' in query
        assert 'idx_title="My Blog Post"' in query
    
    def test_datetime_query_with_idx_prefix(self, query_translator):
        """Test datetime queries use idx_ prefix and timestamp conversion."""
        result = query_translator.translate_select(
            "SELECT * FROM posts WHERE created_at > 1703509800"
        )
        
        query = result.golem_query
        assert 'relation="blogapp.posts"' in query
        assert 'idx_created_at>1703509800' in query
    
    def test_complex_query_with_relation_and_idx_prefixes(self, query_translator):
        """Test complex query with multiple conditions."""
        result = query_translator.translate_select(
            """SELECT * FROM posts 
               WHERE (author_id = 42 OR author_id = 43) 
               AND is_published = true 
               AND created_at > 1703509800"""
        )
        
        query = result.golem_query
        
        # Should start with relation filter
        assert query.startswith('relation="blogapp.posts"')
        
        # All column references should have idx_ prefix
        assert 'idx_author_id=' in query
        assert 'idx_is_published=1' in query
        assert 'idx_created_at>1703509800' in query
        
        # Should contain logical operators
        assert '||' in query  # OR
        assert '&&' in query  # AND


class TestMultiTenantQueryIsolation:
    """Test that queries are properly isolated by project/relation."""
    
    def test_same_table_different_projects(self):
        """Test that same table name in different projects generates different queries."""
        # Setup two identical schemas in different projects
        sm1 = SchemaManager(schema_id="schema", project_id="blog1")
        sm2 = SchemaManager(schema_id="schema", project_id="blog2")
        
        posts_table = TableDefinition(
            name="posts",
            columns=[ColumnDefinition(name="id", type="INTEGER", primary_key=True)],
            indexes=[],
            foreign_keys=[]
        )
        
        sm1.add_table(posts_table)
        sm2.add_table(posts_table)
        
        translator1 = QueryTranslator(sm1)
        translator2 = QueryTranslator(sm2)
        
        # Same SQL query
        sql = "SELECT * FROM posts WHERE id = 1"
        
        result1 = translator1.translate_select(sql)
        result2 = translator2.translate_select(sql)
        
        # Should have different relation filters
        assert 'relation="blog1.posts"' in result1.golem_query
        assert 'relation="blog2.posts"' in result2.golem_query
        
        # But same column conditions
        expected_id = encode_signed_to_uint64(1, 32)
        assert f'idx_id={expected_id}' in result1.golem_query
        assert f'idx_id={expected_id}' in result2.golem_query
    
    def test_cross_project_query_isolation(self):
        """Test that queries cannot accidentally access other projects' data."""
        sm = SchemaManager(schema_id="schema", project_id="secure_project")
        
        posts_table = TableDefinition(
            name="posts",
            columns=[
                ColumnDefinition(name="id", type="INTEGER", primary_key=True),
                ColumnDefinition(name="secret_data", type="VARCHAR(100)", indexed=True)
            ],
            indexes=[],
            foreign_keys=[]
        )
        sm.add_table(posts_table)
        
        translator = QueryTranslator(sm)
        result = translator.translate_select('SELECT * FROM posts WHERE secret_data = "confidential"')
        
        # Query should be scoped to this project only
        query = result.golem_query
        assert 'relation="secure_project.posts"' in query
        assert 'idx_secret_data="confidential"' in query
        
        # This query cannot match entities from other projects due to relation filter


class TestAnnotationNamingEdgeCases:
    """Test edge cases in annotation naming strategy."""
    
    def test_table_with_no_indexed_columns(self):
        """Test table with no indexed columns (only metadata annotations)."""
        sm = SchemaManager(schema_id="test", project_id="testproj")
        
        simple_table = TableDefinition(
            name="logs",
            columns=[
                ColumnDefinition(name="message", type="TEXT"),  # Not indexed
                ColumnDefinition(name="level", type="VARCHAR(10)")  # Not indexed
            ],
            indexes=[],
            foreign_keys=[]
        )
        sm.add_table(simple_table)
        
        row_data = {"message": "Error occurred", "level": "ERROR"}
        annotations = sm.get_entity_annotations_for_table("logs", row_data)
        
        # Should only have metadata annotations
        string_annotations = annotations["string_annotations"]
        numeric_annotations = annotations["numeric_annotations"]
        
        assert string_annotations["row_type"] == "json"
        assert string_annotations["relation"] == "testproj.logs"
        
        # No column annotations since nothing is indexed
        assert len([k for k in string_annotations.keys() if k.startswith("idx_")]) == 0
        assert len([k for k in numeric_annotations.keys() if k.startswith("idx_")]) == 0
    
    def test_project_id_with_special_characters(self):
        """Test project_id with special characters in relation annotation."""
        sm = SchemaManager(schema_id="test", project_id="my-app_v2.0")
        
        table = TableDefinition(
            name="data",
            columns=[ColumnDefinition(name="id", type="INTEGER", primary_key=True)],
            indexes=[],
            foreign_keys=[]
        )
        sm.add_table(table)
        
        annotations = sm.get_entity_annotations_for_table("data", {"id": 1})
        
        # Special characters should be preserved in relation
        assert annotations["string_annotations"]["relation"] == "my-app_v2.0.data"
    
    def test_table_name_with_special_characters(self):
        """Test table name with special characters in relation annotation."""
        sm = SchemaManager(schema_id="test", project_id="myproj")
        
        table = TableDefinition(
            name="user_events_2024",
            columns=[ColumnDefinition(name="id", type="INTEGER", primary_key=True)],
            indexes=[],
            foreign_keys=[]
        )
        sm.add_table(table)
        
        annotations = sm.get_entity_annotations_for_table("user_events_2024", {"id": 1})
        
        # Underscores and numbers should be preserved
        assert annotations["string_annotations"]["relation"] == "myproj.user_events_2024"