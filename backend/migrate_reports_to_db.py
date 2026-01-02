import json
import mysql.connector
import os

# MySQL Configuration
DB_CONFIG = {
    'user': 'root',
    'password': 'ne@202509',
    'host': 'localhost',
    'port': 3306,
    'database': 'bi_data',
    'auth_plugin': 'mysql_native_password'
}

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
REPORTS_FILE = os.path.join(BASE_DIR, "bi_reports.json")

def migrate_reports():
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    try:
        # 1. Create Table
        print("Creating bi_reports table...")
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS bi_reports (
            id VARCHAR(100) PRIMARY KEY,
            category VARCHAR(100),
            title VARCHAR(255),
            description TEXT,
            config JSON,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        )
        """)
        
        # 2. Read JSON
        if not os.path.exists(REPORTS_FILE):
            print("No reports file found.")
            return

        with open(REPORTS_FILE, "r") as f:
            data = json.load(f)
            reports = data.get("reports", [])
            
        print(f"Found {len(reports)} reports to migrate.")
        
        # 3. Insert Data
        for report in reports:
            r_id = report.get("id")
            cat = report.get("category")
            title = report.get("title")
            desc = report.get("description")
            config_json = json.dumps(report)
            
            # Upsert
            sql = """
            INSERT INTO bi_reports (id, category, title, description, config)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                category = VALUES(category),
                title = VALUES(title),
                description = VALUES(description),
                config = VALUES(config)
            """
            cursor.execute(sql, (r_id, cat, title, desc, config_json))
            
        conn.commit()
        print("Migration complete.")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    migrate_reports()
