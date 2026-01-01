import mysql.connector
import json
import os

SCHEMA_FILE = "backend/bi_schema.json"

config = {
    'user': 'root',
    'password': 'ne@202509',
    'host': 'localhost',
    'port': 3306,
    'database': 'bi_data',
    'auth_plugin': 'mysql_native_password'
}

def update_db():
    try:
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()
        
        # 1. Drop Tables
        print("Dropping Dim_App and Dim_Region...")
        cursor.execute("DROP TABLE IF EXISTS Dim_App")
        cursor.execute("DROP TABLE IF EXISTS Dim_Region")
        
        # 2. Update Metadata
        if os.path.exists(SCHEMA_FILE):
            print(f"Reading new schema from {SCHEMA_FILE}...")
            with open(SCHEMA_FILE, "r") as f:
                data = json.load(f)
                
            print("Updating system_metadata...")
            cursor.execute("REPLACE INTO system_metadata (`key`, value) VALUES (%s, %s)", ("bi_schema", json.dumps(data)))
            conn.commit()
            print("Success!")
        else:
            print("Schema file not found!")

        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    update_db()
