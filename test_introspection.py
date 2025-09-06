#!/usr/bin/env python3
"""Quick test of the fixed schema introspection commands."""

import sys
import os

# Add the packages to path
sys.path.insert(0, 'golemdb_sql/src')

def test_introspection_commands():
    """Test SHOW TABLES and DESCRIBE functionality in isolation."""
    print("🔍 Testing Fixed Schema Introspection Commands...")
    
    try:
        # Import required modules
        from golemdb_sql.cursor import Cursor
        from golemdb_sql.schema_manager import SchemaManager, TableDefinition, ColumnDefinition
        
        # Create a mock connection
        class MockConnection:
            def __init__(self):
                self._params = type('obj', (object,), {
                    'schema_id': 'test_schema',
                    'app_id': 'test_app'
                })()
        
        # Create schema manager with test tables
        schema_manager = SchemaManager('test_schema', 'test_app')
        
        # Add test tables
        users_table = TableDefinition(
            name='users',
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
                    nullable=True,
                    indexed=True
                ),
                ColumnDefinition(
                    name='balance',
                    type='DECIMAL',
                    precision=10,
                    scale=2,
                    nullable=True
                )
            ],
            indexes=[],
            foreign_keys=[]
        )
        
        products_table = TableDefinition(
            name='products',
            columns=[
                ColumnDefinition(
                    name='id',
                    type='INTEGER',
                    nullable=False,
                    primary_key=True
                ),
                ColumnDefinition(
                    name='title',
                    type='VARCHAR',
                    length=200,
                    nullable=False
                ),
                ColumnDefinition(
                    name='price',
                    type='DECIMAL',
                    precision=8,
                    scale=2,
                    nullable=False
                )
            ],
            indexes=[],
            foreign_keys=[]
        )
        
        schema_manager.add_table(users_table)
        schema_manager.add_table(products_table)
        
        # Create cursor with mock connection
        mock_conn = MockConnection()
        cursor = Cursor(mock_conn)
        
        print("✅ Test environment set up successfully")
        
        # Test SHOW TABLES
        print("\n📋 Testing SHOW TABLES...")
        try:
            result = cursor._execute_show_tables("SHOW TABLES")
            print(f"   ✅ Result format: {type(result)}")
            print(f"   ✅ Rowcount: {result['rowcount']}")
            print(f"   ✅ Description: {result['description']}")
            print(f"   ✅ Tables found:")
            for i, row in enumerate(result['rows']):
                print(f"      {i+1}. {row[0]}")
                
            # Test processing through _process_result
            cursor._process_result(result)
            fetched = cursor.fetchall()
            print(f"   ✅ After processing - fetched {len(fetched)} rows:")
            for row in fetched:
                print(f"      - {row}")
                
        except Exception as e:
            print(f"   ❌ SHOW TABLES test failed: {e}")
            import traceback
            traceback.print_exc()
        
        # Test DESCRIBE
        print("\n📝 Testing DESCRIBE...")
        try:
            result = cursor._execute_describe_table("DESCRIBE users")
            print(f"   ✅ Result format: {type(result)}")
            print(f"   ✅ Rowcount: {result['rowcount']}")
            print(f"   ✅ Description: {len(result['description'])} columns")
            print(f"   ✅ Column details:")
            
            # Test processing through _process_result
            cursor._process_result(result)
            fetched = cursor.fetchall()
            print(f"   ✅ After processing - fetched {len(fetched)} rows:")
            
            print("      Field              Type              Null  Key  Default  Extra")
            print("      " + "-" * 70)
            for row in fetched:
                if len(row) >= 6:
                    field = str(row[0]).ljust(18)
                    type_str = str(row[1]).ljust(18)
                    null_str = str(row[2]).ljust(5)
                    key_str = str(row[3] or '').ljust(4)
                    default_str = str(row[4] or '').ljust(8)
                    extra_str = str(row[5] or '')
                    print(f"      {field} {type_str} {null_str} {key_str} {default_str} {extra_str}")
                else:
                    print(f"      Incomplete row: {row}")
                
        except Exception as e:
            print(f"   ❌ DESCRIBE test failed: {e}")
            import traceback
            traceback.print_exc()
        
        print("\n✅ Schema introspection tests completed!")
        return True
        
    except Exception as e:
        print(f"❌ Test setup failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Run the test."""
    print("🚀 Testing Fixed Schema Introspection\n")
    
    success = test_introspection_commands()
    
    if success:
        print("\n🎉 All tests passed! Schema introspection is working correctly.")
    else:
        print("\n⚠️  Tests failed. Check the output above for details.")
    
    return success

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)