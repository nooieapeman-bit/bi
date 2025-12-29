import requests
import json

BASE_URL = "http://localhost:8000"

def update_schema():
    # 1. Get current schema
    print("Fetching current schema...")
    try:
        res = requests.get(f"{BASE_URL}/api/schema")
        schema = res.json()
    except Exception as e:
        print(f"Error fetching schema: {e}")
        return

    # 2. Modify Schema
    print("Modifying schema...")
    
    # Helper to check if column exists
    def has_col(columns, name):
        return any(c['name'] == name for c in columns)

    # Columns to add to everyone (except App and Region themselves)
    common_cols = [
        {"name": "app_id", "type": "INTEGER", "foreign_key": "Dim_App.app_id", "description": "所属应用ID"},
        {"name": "region_id", "type": "INTEGER", "foreign_key": "Dim_Region.region_id", "description": "所属区域ID"}
    ]

    # Process Dimensions
    for table in schema['dimensions']:
        if table['name'] in ['Dim_App', 'Dim_Region']:
            continue
            
        # Add common cols if not exist
        for new_col in common_cols:
            if not has_col(table['columns'], new_col['name']):
                table['columns'].append(new_col)
        
        # Special handling for Dim_User: Add proxy_id and likely remove/demote global user_id if needed
        # User request: "user表应该有个代理id"
        if table['name'] == 'Dim_User':
            if not has_col(table['columns'], 'proxy_id'):
                # Add proxy_id at the beginning (conceptually) or just append
                # Using 0 index for visibility in UI often helps but appended is fine for JSON
                proxy_col = {"name": "proxy_id", "type": "TEXT", "description": "代理ID (App/Region独立)", "primary_key": False} 
                table['columns'].insert(1, proxy_col) # Insert after user_id or at top

    # Process Facts (They already had app/region in previous step, but ensuring consistency)
    for table in schema['facts']:
        for new_col in common_cols:
            if not has_col(table['columns'], new_col['name']):
                table['columns'].append(new_col)

    # 3. Save back to DB
    print("Saving updated schema...")
    res = requests.post(f"{BASE_URL}/api/schema", json=schema)
    if res.status_code == 200:
        print("Schema updated successfully!")
    else:
        print(f"Failed to save schema: {res.text}")

if __name__ == "__main__":
    update_schema()
