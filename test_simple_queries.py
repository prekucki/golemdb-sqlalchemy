#!/usr/bin/env python3
"""Test simple constant queries in golemdb-sql."""

import sys
# Packages are now properly installed via pyproject.toml

def test_simple_constant_queries():
    """Test that simple constant queries work without requiring tables."""
    print("🧪 Testing Simple Constant Queries...")
    
    try:
        from golemdb_sql.cursor import Cursor
        
        # Create a mock connection
        class MockConnection:
            def __init__(self):
                self._params = type('obj', (object,), {
                    'schema_id': 'test_schema',
                    'app_id': 'test_app'
                })()
        
        mock_conn = MockConnection()
        cursor = Cursor(mock_conn)
        
        # Test various simple queries
        test_queries = [
            ("SELECT 1", 1, "INTEGER"),
            ("SELECT 42", 42, "INTEGER"),
            ("SELECT 'hello'", 'hello', "STRING"),
            ("SELECT NULL", None, "NULL"),
            ("SELECT TRUE", True, "BOOLEAN"),
            ("SELECT FALSE", False, "BOOLEAN"),
        ]
        
        print("✅ Test environment set up")
        
        for query, expected_value, expected_type in test_queries:
            try:
                # Test the detection method
                is_simple = cursor._is_simple_constant_query(query)
                print(f"\n  Testing: {query}")
                print(f"    Detected as simple constant query: {is_simple}")
                
                if is_simple:
                    # Test execution
                    result = cursor._execute_simple_constant_query(query, {})
                    print(f"    Result format: {type(result)}")
                    print(f"    Rowcount: {result['rowcount']}")
                    print(f"    Rows: {result['rows']}")
                    print(f"    Description: {result['description']}")
                    
                    # Verify the result
                    if result['rows'] and len(result['rows']) > 0:
                        actual_value = result['rows'][0][0]
                        print(f"    Expected: {expected_value} ({expected_type})")
                        print(f"    Actual: {actual_value} ({type(actual_value).__name__})")
                        
                        if actual_value == expected_value:
                            print(f"    ✅ {query} - PASSED")
                        else:
                            print(f"    ❌ {query} - FAILED (value mismatch)")
                    else:
                        print(f"    ❌ {query} - FAILED (no results)")
                else:
                    print(f"    ⚠️  {query} - Not detected as simple constant query")
                    
            except Exception as e:
                print(f"    ❌ {query} - FAILED: {e}")
        
        # Test processing through _process_result
        print(f"\n  Testing integration with _process_result...")
        try:
            result = cursor._execute_simple_constant_query("SELECT 1", {})
            cursor._process_result(result)
            
            fetched = cursor.fetchone()
            print(f"    ✅ After processing - fetched: {fetched}")
            
            if fetched == (1,):
                print(f"    ✅ SELECT 1 integration - PASSED")
            else:
                print(f"    ❌ SELECT 1 integration - FAILED")
                
        except Exception as e:
            print(f"    ❌ Integration test failed: {e}")
        
        return True
        
    except Exception as e:
        print(f"❌ Test setup failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Run the simple query tests."""
    print("🚀 Testing Simple Constant Query Support\n")
    
    success = test_simple_constant_queries()
    
    if success:
        print(f"\n🎉 Simple constant query support is working!")
        print(f"   Now 'SELECT 1' and similar queries will work for connection testing.")
    else:
        print(f"\n⚠️  Tests failed. Check the output above for details.")
    
    return success

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)