
import sqlite3
import json
import os

DB_FILE = "backend/bi_data.db"

if not os.path.exists(DB_FILE):
    print(f"DB File {DB_FILE} does not exist.")
else:
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        
        # Check tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        print(f"Tables: {tables}")
        
        # Check metadata
        try:
            cursor.execute("SELECT key, value FROM system_metadata;")
            rows = cursor.fetchall()
            print(f"\nMetadata Entries ({len(rows)}):")
            for key, val in rows:
                parsed = json.loads(val)
                summary = f"Records: {len(parsed.get('reports', []))}" if 'reports' in parsed else "Schema Data"
                print(f"Key: {key}, Summary: {summary}")
        except Exception as e:
            print(f"Error querying metadata: {e}")
            
        conn.close()
    except Exception as e:
        print(f"DB Error: {e}")
