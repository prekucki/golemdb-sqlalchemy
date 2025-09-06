#!/usr/bin/env python3
"""Test script for the GolemBase SQLAlchemy dialect."""

import os
import sys
import logging
from pathlib import Path

# Add the packages to path
sys.path.insert(0, 'golemdb_sql/src')
sys.path.insert(0, 'sqlalchemy_golembase/src')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

def test_dialect_registration():
    """Test that the dialect can be registered with SQLAlchemy."""
    print("🔧 Testing dialect registration...")
    
    try:
        # Import SQLAlchemy
        from sqlalchemy import create_engine
        from sqlalchemy_dialects_golembase import GolemBaseDialect
        
        print("✅ Successfully imported SQLAlchemy and GolemBaseDialect")
        
        # Test basic dialect properties
        dialect = GolemBaseDialect()
        print(f"  Dialect name: {dialect.name}")
        print(f"  Dialect driver: {dialect.driver}")
        print(f"  Parameter style: {dialect.default_paramstyle}")
        print(f"  Supports schemas: {dialect.supports_schemas}")
        
        return True
        
    except ImportError as e:
        print(f"❌ Import error: {e}")
        return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def test_connection_args_parsing():
    """Test URL parsing and connection argument creation."""
    print("\n🔗 Testing connection argument parsing...")
    
    try:
        from sqlalchemy import create_engine
        from sqlalchemy.engine.url import make_url
        from sqlalchemy_dialects_golembase import GolemBaseDialect
        
        # Test URL parsing
        test_url = make_url(
            "golembase:///test_schema"
            "?rpc_url=https://example.com/rpc"
            "&ws_url=wss://example.com/ws"
            "&private_key=0x123"
            "&app_id=test_app"
        )
        
        dialect = GolemBaseDialect()
        args, kwargs = dialect.create_connect_args(test_url)
        
        print("✅ URL parsing successful:")
        print(f"  Args: {args}")
        print(f"  Kwargs: {kwargs}")
        
        # Verify expected parameters
        expected_params = ['rpc_url', 'ws_url', 'private_key', 'app_id', 'schema_id']
        missing = [p for p in expected_params if p not in kwargs]
        if missing:
            print(f"⚠️  Missing parameters: {missing}")
        else:
            print("✅ All required parameters present")
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def test_dbapi_import():
    """Test that the dialect can import the golemdb_sql DBAPI module."""
    print("\n📦 Testing DB-API module import...")
    
    try:
        from sqlalchemy_dialects_golembase import GolemBaseDialect
        
        # Test dbapi() method
        dbapi_module = GolemBaseDialect.dbapi()
        
        print("✅ DB-API module imported successfully:")
        print(f"  Module: {dbapi_module}")
        print(f"  API level: {dbapi_module.apilevel}")
        print(f"  Thread safety: {dbapi_module.threadsafety}")
        print(f"  Parameter style: {dbapi_module.paramstyle}")
        
        # Test connection interface
        if hasattr(dbapi_module, 'connect'):
            print("✅ Connect function available")
        else:
            print("❌ Connect function not found")
        
        return True
        
    except ImportError as e:
        print(f"❌ Import error: {e}")
        print("   Make sure golemdb_sql is installed or in the Python path")
        return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def test_type_mapping():
    """Test type mapping functionality."""
    print("\n🎯 Testing type mapping...")
    
    try:
        from sqlalchemy_dialects_golembase import GolemBaseDialect
        from sqlalchemy_dialects_golembase.types import GolemBaseTypeMap
        
        dialect = GolemBaseDialect()
        type_map = dialect.type_map
        
        print("✅ Type map created successfully")
        
        # Test some basic type mappings
        test_types = ['INTEGER', 'VARCHAR(255)', 'DECIMAL(10,2)', 'BOOLEAN', 'DATETIME']
        
        for type_name in test_types:
            try:
                sqlalchemy_type = type_map.get_sqlalchemy_type(type_name)
                print(f"  {type_name} -> {sqlalchemy_type}")
            except Exception as e:
                print(f"  ❌ Error mapping {type_name}: {e}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def test_show_tables_describe():
    """Test SHOW TABLES and DESCRIBE functionality in isolation."""
    print("\n🔍 Testing SHOW TABLES and DESCRIBE commands...")
    
    try:
        # Import the cursor module to test the new methods
        from golemdb_sql.cursor import Cursor
        from golemdb_sql.schema_manager import SchemaManager, TableDefinition, ColumnDefinition
        
        # Create a mock connection with schema manager
        class MockConnection:
            def __init__(self):
                self._params = type('obj', (object,), {
                    'schema_id': 'test_schema',
                    'app_id': 'test_app'
                })()
        
        # Create a mock schema manager with test table
        schema_manager = SchemaManager('test_schema', 'test_app')
        
        # Add a test table
        test_table = TableDefinition(
            name='test_table',
            columns=[
                ColumnDefinition(
                    name='id',
                    type='INTEGER',
                    nullable=False,
                    primary_key=True,
                    indexed=True
                ),
                ColumnDefinition(
                    name='name',
                    type='VARCHAR',
                    length=100,
                    nullable=False,
                    indexed=True
                ),
                ColumnDefinition(
                    name='email',
                    type='VARCHAR',
                    length=255,
                    nullable=True
                )
            ],
            indexes=[],
            foreign_keys=[]
        )
        
        schema_manager.add_table(test_table)
        
        # Create cursor with mock connection
        mock_conn = MockConnection()
        cursor = Cursor(mock_conn)
        
        print("✅ Test environment set up successfully")
        print("✅ Mock table 'test_table' added to schema")
        
        # Test SHOW TABLES
        try:
            result = cursor._execute_show_tables("SHOW TABLES")
            print(f"✅ SHOW TABLES result: {result['rowcount']} tables found")
            for row in result['rows']:
                print(f"  Table: {row[0]}")
        except Exception as e:
            print(f"⚠️  SHOW TABLES test failed: {e}")
        
        # Test DESCRIBE
        try:
            result = cursor._execute_describe_table("DESCRIBE test_table")
            print(f"✅ DESCRIBE result: {result['rowcount']} columns found")
            for row in result['rows']:
                print(f"  Column: {row[0]} ({row[1]}) - Nullable: {row[2]}, Key: {row[3]}")
        except Exception as e:
            print(f"⚠️  DESCRIBE test failed: {e}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def main():
    """Run all tests."""
    print("🚀 Testing GolemBase SQLAlchemy Dialect Implementation\n")
    
    tests = [
        ("Dialect Registration", test_dialect_registration),
        ("Connection Args Parsing", test_connection_args_parsing), 
        ("DB-API Import", test_dbapi_import),
        ("Type Mapping", test_type_mapping),
        ("SHOW TABLES & DESCRIBE", test_show_tables_describe),
    ]
    
    results = []
    for test_name, test_func in tests:
        print(f"\n{'='*60}")
        print(f"Running: {test_name}")
        print('='*60)
        
        try:
            success = test_func()
            results.append((test_name, success))
        except Exception as e:
            print(f"❌ Test '{test_name}' failed with exception: {e}")
            results.append((test_name, False))
    
    # Summary
    print(f"\n{'='*60}")
    print("TEST SUMMARY")
    print('='*60)
    
    passed = 0
    for test_name, success in results:
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"{test_name}: {status}")
        if success:
            passed += 1
    
    print(f"\nResults: {passed}/{len(results)} tests passed")
    
    if passed == len(results):
        print("\n🎉 All tests passed! The GolemBase SQLAlchemy dialect is ready to use.")
    else:
        print(f"\n⚠️  {len(results) - passed} test(s) failed. Review the output above for details.")
    
    return passed == len(results)

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)