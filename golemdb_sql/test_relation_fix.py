#!/usr/bin/env python3
"""Test script to verify relation annotation uses correct schema_id."""

import sys
import os
import logging
sys.path.insert(0, 'src')

from golemdb_sql.schema_manager import SchemaManager
from golemdb_sql.connection_parser import ConnectionParams

def main():
    """Test relation annotation generation."""
    
    # Test with specific schema_id and app_id
    schema_id = "ddl_test_schema"
    app_id = "demo_app"
    
    print(f"Testing with schema_id='{schema_id}', app_id='{app_id}'")
    
    # Create SchemaManager with both parameters
    schema_manager = SchemaManager(
        schema_id=schema_id,
        project_id=app_id
    )
    
    print(f"SchemaManager.schema_id: {schema_manager.schema_id}")
    print(f"SchemaManager.project_id: {schema_manager.project_id}")
    
    # Test annotation generation
    test_data = {"id": 1, "name": "test"}
    table_name = "users"
    
    try:
        annotations = schema_manager.get_entity_annotations_for_table(table_name, test_data)
        relation = annotations["string_annotations"]["relation"]
        print(f"Generated relation: '{relation}'")
        
        expected_relation = f"{app_id}.{table_name}"
        if relation == expected_relation:
            print(f"✅ Correct! Expected: '{expected_relation}', Got: '{relation}'")
        else:
            print(f"❌ Wrong! Expected: '{expected_relation}', Got: '{relation}'")
            
    except Exception as e:
        print(f"Error: {e}")
        # This is expected if table doesn't exist in schema


if __name__ == "__main__":
    main()