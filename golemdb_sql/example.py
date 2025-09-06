import golemdb_sql

# Connect to GolemBase database
conn = golemdb_sql.connect(
    rpc_url='https://ethwarsaw.holesky.golemdb.io/rpc',
    ws_url='wss://ethwarsaw.holesky.golemdb.io/rpc/ws',
    private_key='0x0000000000000000000000000000000000000000000000000000000000000001',
    app_id='myapp',
    schema_id='production'
)

# Create table schema first
cursor = conn.cursor()
cursor.execute("""
    CREATE TABLE users (
        id INTEGER PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        email VARCHAR(200) NOT NULL,
        active BOOLEAN DEFAULT TRUE,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
""")

# Insert data
cursor.execute(
    "INSERT INTO users (name, email, active) VALUES (%(name)s, %(email)s, %(active)s)",
    {'name': 'Alice Smith', 'email': 'alice@example.com', 'active': True}
)

# Execute queries  
cursor.execute("SELECT id, name, email FROM users WHERE active = %(active)s", {'active': True})

# Fetch results
for row in cursor:
    print(f"User {row[0]}: {row[1]} ({row[2]})")

# Close connection
cursor.close()
conn.close()

