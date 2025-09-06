
# Row Mapping

```sql
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
```

 - UNIQUE constrains can not be implemented (?? TODO check this)
 - DATETIME - should be mapped to uint64. as unix time seconds.
 - BOOLEAN - will be true/false in encoded row but 0/1 in index.
 - VARCHAR - as string in json. and string annotation in indexes.
 - INTEGER/BIGINT/SMALLINT/TINYINT - all signed integers are encoded to preserve ordering in uint64 numeric annotations
 - DECIMAL/NUMERIC - stored as string annotations with lexicographic ordering encoding
 - FLOAT/DOUBLE/REAL - NOT indexable due to precision issues, stored only in JSON data

## Signed Integer Encoding

GolemDB numeric annotations only support uint64 values, but SQL signed integer types need special encoding to preserve ordering. Without encoding, negative values would appear larger than positive values when stored as uint64.

### Encoding Strategy

**For TINYINT (8-bit signed):**
- Range: -2⁷ to 2⁷-1 (-128 to 127)
- Encoded as: `(value + 2⁷) + 2⁶³`

**For SMALLINT (16-bit signed):**
- Range: -2¹⁵ to 2¹⁵-1 (-32,768 to 32,767)
- Encoded as: `(value + 2¹⁵) + 2⁶³`

**For INTEGER (32-bit signed):**
- Range: -2³¹ to 2³¹-1 
- Encoded as: `(value + 2³¹) + 2⁶³`

**For BIGINT (64-bit signed):**
- Range: -2⁶³ to 2⁶³-1
- Encoded as: `(value + 2⁶³) % 2⁶⁴`  
- This flips the most significant bit, mapping:
  - Negative numbers: 0x0000000000000000 - 0x7FFFFFFFFFFFFFFF
  - Zero: 0x8000000000000000
  - Positive numbers: 0x8000000000000001 - 0xFFFFFFFFFFFFFFFF

### Ordering Preservation

The encoding preserves ordering so that `-2 < -1 < 0 < 1 < 2` remains true when values are stored as uint64:

```python
# Example: encoding preserves ordering
values = [-100, -1, 0, 1, 100]
encoded = [encode_signed_to_uint64(v, 32) for v in values]
# encoded values will be in ascending order
assert encoded == sorted(encoded)
```

### Implementation

- **Storage**: When creating/updating entities, signed integers are encoded before storing in numeric annotations
- **Queries**: When translating SQL queries, comparison values are encoded so queries work correctly  
- **Retrieval**: When deserializing entities back to table rows, encoded values are decoded to original signed integers

**Note**: All signed integer types (TINYINT, SMALLINT, INTEGER, BIGINT) require encoding to preserve ordering when stored as uint64.

## DECIMAL/NUMERIC String Encoding

DECIMAL and NUMERIC types (with fixed precision) are stored as string annotations using lexicographic ordering encoding to ensure correct range queries and sorting.

### SQL92 DECIMAL Standard

DECIMAL(precision, scale) where:
- **precision**: Total number of significant digits (1-38, default: 18)
- **scale**: Number of digits after decimal point (0 to precision, default: 0)

Examples:
- `DECIMAL(10,2)`: 10 total digits, 2 after decimal → range: -99999999.99 to 99999999.99
- `DECIMAL(8,0)`: 8 digits, 0 after decimal → integers only: -99999999 to 99999999
- `DECIMAL(5,3)`: 5 total digits, 3 after decimal → range: -99.999 to 99.999

### Encoding Strategy for String Ordering

**Positive numbers**: Prefix with `.` and pad with leading zeros
```
123.45 in DECIMAL(8,2) → ".000123.45"
99.9 in DECIMAL(5,1) → ".0099.9"  
12345 in DECIMAL(6,0) → ".012345"
```

**Negative numbers**: Prefix with `-` and invert digits (0→9, 1→8, ..., 9→0)
```
-123.45 in DECIMAL(8,2) → "-999876.54"  (digits inverted from 000123.45)
-99.9 in DECIMAL(5,1) → "-9900.0"     (digits inverted from 0099.9)
-12345 in DECIMAL(6,0) → "-987654"    (digits inverted from 012345)
```

### Ordering Preservation

This encoding ensures lexicographic string ordering matches numeric ordering:
```
"-999876.54" < "-9900.0" < ".0099.9" < ".000123.45"
     ↓           ↓           ↓           ↓
   -123.45    < -99.9     < 99.9      < 123.45
```

### Schema Storage Location

Schema metadata is stored in platform-appropriate user data directories using the `appdirs` library:
- **Linux**: `~/.local/share/golembase/schemas/{schema_id}.toml`
- **macOS**: `~/Library/Application Support/golembase/schemas/{schema_id}.toml`
- **Windows**: `%APPDATA%/golembase/schemas/{schema_id}.toml`

The TOML schema files contain table definitions with column precision/scale metadata:

```toml
[tables.financial_data]
entity_ttl = 86400

[[tables.financial_data.columns]]
name = "price"
type = "DECIMAL(10,2)"
precision = 10
scale = 2
indexed = true
nullable = true

[[tables.financial_data.columns]]  
name = "rate"
type = "DECIMAL(5,4)"
precision = 5
scale = 4
indexed = true
nullable = true
```

### Implementation Details

- **Storage**: DECIMAL values encoded and stored in `string_annotations`
- **Queries**: SQL comparisons use encoded string values for correct ordering
- **Validation**: Values must fit within declared precision/scale constraints
- **JSON Data**: Original DECIMAL values stored in entity JSON for exact retrieval

**Non-Indexable Types**: FLOAT, DOUBLE, and REAL are NOT stored in annotations due to floating-point precision issues. They are only available in the JSON entity data and cannot be used in WHERE clauses.

## Annotation Naming Strategy

To enable multi-tenancy and efficient querying, GolemDB entities use a structured annotation naming convention:

### Metadata Annotations (String)

- `row_type = "json"` - Identifies this entity as a table row
- `relation = "<projectId>.<tableName>"` - Full table identifier for multi-tenant isolation

### Indexed Column Annotations

- `idx_<columnName> = "<encodedValue>"` - For each indexed column
- Uses appropriate encoding strategies (signed integers → uint64, DECIMAL → string ordering)

### Example: Posts Table Entity

For the posts schema:
```sql
CREATE TABLE posts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title VARCHAR(200) NOT NULL,
    content TEXT,
    author_id INTEGER NOT NULL,
    is_published BOOLEAN DEFAULT FALSE,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Indexes
CREATE INDEX idx_posts_author_id ON posts(author_id);
CREATE INDEX idx_posts_is_published ON posts(is_published);  
CREATE INDEX idx_posts_created_at ON posts(created_at);
CREATE INDEX idx_posts_title ON posts(title);
```

A posts record would generate these annotations:
```json
{
  "string_annotations": {
    "row_type": "json",
    "relation": "myproject.posts",
    "idx_title": "My Blog Post Title"
  },
  "numeric_annotations": {
    "idx_id": 9223372036854775809,        // Encoded signed integer
    "idx_author_id": 9223372036854775810, // Encoded signed integer  
    "idx_is_published": 1,                // 1 for true, 0 for false
    "idx_created_at": 1703509800          // Unix timestamp
  }
}
```

### Multi-Tenant Query Examples

**Find posts by author in specific project:**
```
relation="myproject.posts" && idx_author_id=9223372036854775810
```

**Find published posts created after timestamp:**
```  
relation="myproject.posts" && idx_is_published=1 && idx_created_at>1703509800
```

**Cross-project isolation:**
```
// Project A posts
relation="projectA.posts" && idx_title="Sample"

// Project B posts (separate namespace)  
relation="projectB.posts" && idx_title="Sample"
```

### Benefits

- **Multi-tenancy**: Different projects can use identical table names without conflicts
- **Query efficiency**: Relation filter provides fast project/table isolation
- **Scalability**: Clear separation enables horizontal scaling by project
- **Debuggability**: Easy identification of entity types and ownership

schemas are kept in XDG userdata. DDL will update toml  structured file that describes:

 - relations
 - indexed columns.
 - constraints
 - column types. (default values, etc.)

Schemat filename willbe derived from connection string. connection string should determine:

 - rpc/ws url for api.
 - privatekey keystore id.
 - appid
 - schemaid (toml with definitonss)

# How to use golem-sdk

creating entity

```python
from golem_base_sdk import GolemBaseCreate, Annotation

async def create_first_entity(client):
    # Create entity
    entity = GolemBaseCreate(
        data=b"Hello, Golem DB from Python!",
        btl=100,  # Expires after 100 blocks
        string_annotations=[
            Annotation(key="type", value="greeting"),
            Annotation(key="language", value="python")
        ],
        numeric_annotations=[
            Annotation(key="priority", value=1),
            Annotation(key="version", value=3)
        ]
    )
    
    # Send to blockchain
    receipts = await client.create_entities([entity])
    receipt = receipts[0]
    
    print(f"Entity created!")
    print(f"Entity key: {receipt.entity_key}")
    print(f"Expires at block: {receipt.expiration_block}")
    
    return receipt.entity_key
```


Query:

```python
import json

async def query_entities(client, entity_key):
    print(f"Entity metadata: {await client.get_entity_metadata(entity_key)}")
    print(f"Entity storage: {await client.get_storage_value(entity_key)}")

    # 1. Simple equality query for strings (use double quotes)
    greetings = await client.query_entities('type="greeting"')
    print(f"Found {len(greetings)} greeting entities")

    # 2. Processing results
    for result in greetings:
        entity_key = result.entity_key
        decoded = result.storage_value.decode("utf-8")
        try:
            data = json.loads(decoded)
            print(f"Entity: {entity_key}, Decoded JSON data {data}")
        except (json.JSONDecodeError, ValueError):
            print(f"Entity: {entity_key}, Decoded data {decoded}")

    # 3. Numeric equality (no quotes for numbers)
    print(f"version_one: {await client.query_entities('version=1')}")
    print(f"high_priority: {await client.query_entities('priority=5')}")

    # 4. Numeric comparison operators
    print(f"above_threshold: {await client.query_entities('priority>3')}")
    print(f"old_versions: {await client.query_entities('version<10')}")
    print(f"in_range: {await client.query_entities('score>=80')}")

    # 5. Combining conditions with AND (&&)
    print(f"specific: {await client.query_entities('type="message" && version=2')}")
    print(f"filtered: {await client.query_entities('status="active" && priority>3')}")

    # 6. Using OR (||) for multiple options
    print(f"messages: {await client.query_entities('type="message" || type="alert"')}")

    # 7. Complex queries with parentheses
    print(
        f"complex_query: {
            await client.query_entities(
                '(type="greeting" && version>2) || status="urgent"'
            )
        }"
    )

    # 8. Query by owner with variable
    owner = client.get_account_address()
    print(f"by_owner: {await client.query_entities(f'$owner="{owner}"')}")

    # Note: String values need double quotes: type="message"
    # Numbers don't need quotes: priority=5
    # Use && for AND operator, || for OR operator in complex queries

```

full example:

````python
# Full working example with all features
# Save as: golem_example.py

import os
import json
import time
import asyncio
import uuid
from dotenv import load_dotenv
from golem_base_sdk import (
    GolemBaseClient,
    GolemBaseCreate,
    GolemBaseUpdate,
    GolemBaseDelete,
    GenericBytes,
    Annotation
)

# Load environment variables
load_dotenv()

# Configuration
PRIVATE_KEY = os.getenv('PRIVATE_KEY', '0x0000000000000000000000000000000000000000000000000000000000000001')
RPC_URL = 'https://ethwarsaw.holesky.golemdb.io/rpc'
WS_URL = 'wss://ethwarsaw.holesky.golemdb.io/rpc/ws'

async def main():
    # === Initialize Client ===
    private_key_hex = PRIVATE_KEY.replace('0x', '')
    private_key_bytes = bytes.fromhex(private_key_hex)
    
    client = await GolemBaseClient.create(
        rpc_url=RPC_URL,
        ws_url=WS_URL,
        private_key=private_key_bytes
    )
    
    print("Connected to Golem DB!")
    print(f"Address: {client.get_account_address()}")
    
    # Set up real-time event watching
    await client.watch_logs(
        label="",
        create_callback=lambda create: print(f"WATCH-> Create: {create}"),
        update_callback=lambda update: print(f"WATCH-> Update: {update}"),
        delete_callback=lambda delete: print(f"WATCH-> Delete: {delete}"),
        extend_callback=lambda extend: print(f"WATCH-> Extend: {extend}"),
    )
    
    # === CREATE Operations ===
    print("\n=== CREATE Operations ===")
    
    # Create entity with unique ID
    entity_id = str(uuid.uuid4())
    data = {
        "message": "Hello from ETHWarsaw 2025!",
        "timestamp": int(time.time()),
        "author": "Python Developer"
    }
    
    entity = GolemBaseCreate(
        data=json.dumps(data).encode('utf-8'),
        btl=300,  # ~10 minutes (300 blocks at ~2 seconds each)
        string_annotations=[
            Annotation(key="type", value="message"),
            Annotation(key="event", value="ethwarsaw"),
            Annotation(key="id", value=entity_id)
        ],
        numeric_annotations=[
            Annotation(key="version", value=1),
            Annotation(key="timestamp", value=int(time.time()))
        ]
    )
    
    receipts = await client.create_entities([entity])
    entity_key = receipts[0].entity_key
    print(f"Created entity: {entity_key}")
    
    # === QUERY Operations ===
    print("\n=== QUERY Operations ===")
    
    # Query entity by annotations
    results = await client.query_entities(f'id = "{entity_id}" && version = 1')
    print(f"Found {len(results)} entities")
    
    for result in results:
        data = json.loads(result.storage_value.decode('utf-8'))
        print(f"  Entity: {data}")
    
    # === UPDATE Operations ===
    print("\n=== UPDATE Operations ===")
    
    updated_data = {
        "message": "Updated message from ETHWarsaw!",
        "updated": True,
        "updateTime": int(time.time())
    }
    
    update = GolemBaseUpdate(
        entity_key=entity_key,
        data=json.dumps(updated_data).encode('utf-8'),
        btl=600,  # ~20 minutes (600 blocks at ~2 seconds each)
        string_annotations=[
            Annotation(key="type", value="message"),
            Annotation(key="id", value=entity_id),
            Annotation(key="status", value="updated")
        ],
        numeric_annotations=[
            Annotation(key="version", value=2)
        ]
    )
    
    update_receipts = await client.update_entities([update])
    print(f"Updated entity: {update_receipts[0].entity_key}")
    
    # Query updated entity
    updated_results = await client.query_entities(f'id = "{entity_id}" && version = 2')
    for result in updated_results:
        data = json.loads(result.storage_value.decode("utf-8"))
        print(f"  Updated entity: {data}")

    # === DELETE Operations ===
    print("\n=== DELETE Operations ===")

    # Delete the entity
    delete_receipt = await client.delete_entities(
        [
            GolemBaseDelete(
                entity_key,  # Already a GenericBytes object
            )
        ]
    )
    print(f"Deleted entity: {delete_receipt[0]}")
    
    # Clean exit
    import sys
    await asyncio.sleep(2)  # Give time for events to be logged
    print("\nExample completed!")
    sys.exit(0)

if __name__ == "__main__":
    # Install requirements:
    # python3 -m venv venv
    # source venv/bin/activate
    # pip install golem-base-sdk==0.1.0 python-dotenv
    
    asyncio.run(main())
````

# Development

The `golemdb_sql` directory contains the core SQL-to-GolemDB translation layer implementing a PEP 249 compliant Python Database API 2.0 interface.

For detailed development setup, testing instructions, and API documentation, see: [`golemdb_sql/README.md`](../golemdb_sql/README.md)
