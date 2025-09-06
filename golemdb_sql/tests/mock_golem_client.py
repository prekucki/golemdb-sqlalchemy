"""Mock implementation of GolemBase SDK client for testing."""

import asyncio
import json
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass, asdict
from datetime import datetime


@dataclass
class MockEntity:
    """Mock GolemBase entity."""
    id: str
    data: bytes
    string_annotations: Dict[str, str]
    numeric_annotations: Dict[str, Union[int, float]]
    ttl: int = 86400
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.utcnow()
        if self.updated_at is None:
            self.updated_at = datetime.utcnow()


class MockGolemBaseClient:
    """Mock GolemBase SDK client for testing."""
    
    def __init__(self):
        """Initialize mock client."""
        self._entities: Dict[str, MockEntity] = {}
        self._closed = False
        
    @classmethod
    async def create(cls, rpc_url: str, ws_url: str, private_key: bytes) -> 'MockGolemBaseClient':
        """Create mock client instance."""
        # Simulate async initialization
        await asyncio.sleep(0.01)
        return cls()
    
    async def create_entities(self, entities: List[Dict[str, Any]]) -> List[str]:
        """Create entities in mock storage."""
        if self._closed:
            raise Exception("Client is closed")
            
        entity_ids = []
        for entity_data in entities:
            # Generate entity ID
            entity_id = f"entity_{len(self._entities) + 1:06d}"
            
            # Create mock entity
            entity = MockEntity(
                id=entity_id,
                data=entity_data.get('data', b'{}'),
                string_annotations=entity_data.get('string_annotations', {}),
                numeric_annotations=entity_data.get('numeric_annotations', {}),
                ttl=entity_data.get('ttl', 86400)
            )
            
            self._entities[entity_id] = entity
            entity_ids.append(entity_id)
        
        # Simulate network delay
        await asyncio.sleep(0.01)
        return entity_ids
    
    async def update_entities(self, entities: List[Dict[str, Any]]) -> List[str]:
        """Update entities in mock storage."""
        if self._closed:
            raise Exception("Client is closed")
            
        updated_ids = []
        for entity_data in entities:
            entity_id = entity_data.get('id')
            if not entity_id or entity_id not in self._entities:
                raise Exception(f"Entity {entity_id} not found")
            
            # Update entity
            entity = self._entities[entity_id]
            entity.data = entity_data.get('data', entity.data)
            entity.string_annotations = entity_data.get('string_annotations', entity.string_annotations)
            entity.numeric_annotations = entity_data.get('numeric_annotations', entity.numeric_annotations)
            entity.ttl = entity_data.get('ttl', entity.ttl)
            entity.updated_at = datetime.utcnow()
            
            updated_ids.append(entity_id)
        
        # Simulate network delay
        await asyncio.sleep(0.01)
        return updated_ids
    
    async def delete_entities(self, entity_ids: List[str]) -> List[str]:
        """Delete entities from mock storage."""
        if self._closed:
            raise Exception("Client is closed")
            
        deleted_ids = []
        for entity_id in entity_ids:
            if entity_id in self._entities:
                del self._entities[entity_id]
                deleted_ids.append(entity_id)
        
        # Simulate network delay
        await asyncio.sleep(0.01)
        return deleted_ids
    
    async def query_entities(
        self, 
        query: str, 
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        sort_by: Optional[str] = None,
        sort_order: str = "asc"
    ) -> List[Dict[str, Any]]:
        """Query entities from mock storage."""
        if self._closed:
            raise Exception("Client is closed")
            
        # Simple query parsing for testing
        results = []
        for entity in self._entities.values():
            if self._matches_query(entity, query):
                results.append({
                    'id': entity.id,
                    'data': entity.data,
                    'string_annotations': entity.string_annotations,
                    'numeric_annotations': entity.numeric_annotations,
                    'ttl': entity.ttl,
                    'created_at': entity.created_at,
                    'updated_at': entity.updated_at
                })
        
        # Apply sorting
        if sort_by:
            reverse = sort_order.lower() == "desc"
            if sort_by in ['created_at', 'updated_at']:
                results.sort(key=lambda x: x[sort_by] or datetime.min, reverse=reverse)
            elif sort_by in ['id']:
                results.sort(key=lambda x: x[sort_by], reverse=reverse)
            else:
                # Try to sort by annotation
                def get_sort_key(x):
                    return (
                        x['string_annotations'].get(sort_by) or 
                        x['numeric_annotations'].get(sort_by) or 
                        ""
                    )
                results.sort(key=get_sort_key, reverse=reverse)
        
        # Apply pagination
        if offset:
            results = results[offset:]
        if limit:
            results = results[:limit]
        
        # Simulate network delay
        await asyncio.sleep(0.01)
        return results
    
    def _matches_query(self, entity: MockEntity, query: str) -> bool:
        """Simple query matching for testing."""
        if not query or query.strip() == "":
            return True
            
        # Very basic query parsing - just check for table annotation
        if 'table=' in query:
            # Extract table name from query like 'table="users"'
            import re
            table_match = re.search(r'table=["\'](.*?)["\']', query)
            if table_match:
                table_name = table_match.group(1)
                return entity.string_annotations.get('table') == table_name
        
        # For testing purposes, match entities with any annotations
        return bool(entity.string_annotations or entity.numeric_annotations)
    
    async def get_entity(self, entity_id: str) -> Optional[Dict[str, Any]]:
        """Get single entity by ID."""
        if self._closed:
            raise Exception("Client is closed")
            
        entity = self._entities.get(entity_id)
        if not entity:
            return None
        
        # Simulate network delay
        await asyncio.sleep(0.01)
        
        return {
            'id': entity.id,
            'data': entity.data,
            'string_annotations': entity.string_annotations,
            'numeric_annotations': entity.numeric_annotations,
            'ttl': entity.ttl,
            'created_at': entity.created_at,
            'updated_at': entity.updated_at
        }
    
    async def close(self) -> None:
        """Close mock client."""
        self._closed = True
        await asyncio.sleep(0.01)
    
    def is_closed(self) -> bool:
        """Check if client is closed."""
        return self._closed
    
    def get_stored_entities(self) -> Dict[str, MockEntity]:
        """Get stored entities for testing inspection."""
        return self._entities.copy()
    
    def clear_entities(self) -> None:
        """Clear all stored entities."""
        self._entities.clear()
    
    def add_test_entity(
        self,
        entity_id: str,
        data: bytes,
        string_annotations: Dict[str, str],
        numeric_annotations: Dict[str, Union[int, float]]
    ) -> None:
        """Add test entity directly to storage."""
        entity = MockEntity(
            id=entity_id,
            data=data,
            string_annotations=string_annotations,
            numeric_annotations=numeric_annotations
        )
        self._entities[entity_id] = entity