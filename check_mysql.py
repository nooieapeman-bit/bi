import mysql.connector
import json

config = {
    'user': 'root',
    'password': 'ne@202509',
    'host': 'localhost',
    'port': 3306,
    'database': 'bi_data',
    'auth_plugin': 'mysql_native_password'
}

try:
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()
    
    print("Connected to MySQL!")
    
    # Check tables
    cursor.execute("SHOW TABLES")
    tables = cursor.fetchall()
    print(f"Tables ({len(tables)}):")
    for t in tables:
        print(f" - {t[0]}")
        
    # Check metadata
    cursor.execute("SELECT `key`, value FROM system_metadata")
    meta = cursor.fetchall()
    print(f"\nMetadata ({len(meta)}):")
    for k, v in meta:
        print(f"Key: {k}, Content Length: {len(v)}")
        
    conn.close()
    
except Exception as e:
    print(f"Verification Failed: {e}")
