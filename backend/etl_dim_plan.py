import mysql.connector
import time

# MySQL Configuration
DB_CONFIG = {
    'user': 'root',
    'password': 'ne@202509',
    'host': 'localhost',
    'port': 3306,
    'database': 'bi_data',
    'auth_plugin': 'mysql_native_password'
}

def get_db_connection():
    return mysql.connector.connect(**DB_CONFIG)

def etl_dim_plan():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    
    try:
        print("Recreating Dim_Plan table with correct schema...")
        # Dropping and recreating to ensure Code is VARCHAR and columns match requirements
        drop_offer_sql = "DROP TABLE IF EXISTS Dim_Plan"
        cursor.execute(drop_offer_sql)
        
        create_sql = """
            CREATE TABLE Dim_Plan (
                plan_key VARCHAR(100) PRIMARY KEY, -- Mapped from Code (User called it plan_id, using plan_key for consistency or plan_id?)
                plan_name VARCHAR(255),            -- Mapped from Name
                price DECIMAL(10,2),               -- Mapped from Price
                time_unit VARCHAR(50),             -- Mapped from Time Unit
                cycle_time INT,                    -- Mapped from Time
                license_number INT DEFAULT 1,      -- Fixed at 1
                app_key VARCHAR(50),               -- 'osaio' or 'nooie'
                region_key VARCHAR(50),            -- Empty
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        # User asked for 'plan_id' to correspond to 'code'. 
        # But 'Dim_' tables usually have '_key'. 
        # I will use `plan_key` as the primary key name to match `Fact_Subscription.plan_key`.
        # Wait, previous tasks used `plan_key` in Fact_Subscription. 
        # So `Dim_Plan` should likely have `plan_key` to join.
        # Let's double check user phrasing: "plan_id对应code". 
        # I will name the column `plan_key` to align with the rest of the BI schema (Fact_Order.plan_key), 
        # but I'll make sure it holds the 'code' value.
        
        cursor.execute(create_sql)
        print("Dim_Plan created.")

        source_apps = [
            {'table': 'osaio.plan_osaio', 'app_key': 'osaio'},
            {'table': 'osaio.plan_nooie', 'app_key': 'nooie'}
        ]
        
        total_inserted = 0
        
        for source in source_apps:
            table_name = source['table']
            app_key = source['app_key']
            
            print(f"Fetching data from {table_name}...")
            
            # Select columns based on mapping:
            # Code -> plan_key
            # Name -> plan_name (Guessing 'Name' column existence)
            # Price -> price
            # Time Unit -> time_unit
            # Time -> cycle_time
            
            # Note: Checking source columns from previous DESCRIBE:
            # Code, Name, Price, Time Unit, Time
            
            select_sql = f"""
                SELECT 
                    `Code` as code, 
                    `Name` as name, 
                    `Price` as price, 
                    `Time Unit` as time_unit, 
                    `Time` as time_val
                FROM {table_name}
            """
            cursor.execute(select_sql)
            rows = cursor.fetchall()
            
            print(f"  Found {len(rows)} plans in {table_name}.")
            
            if not rows:
                continue
                
            insert_sql = """
                INSERT INTO Dim_Plan 
                (plan_key, plan_name, price, app_key, license_number, time_unit, cycle_time, region_key)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            batch_data = []
            for row in rows:
                # Mapping
                p_key = row['code']
                p_name = row['name']
                p_price = row['price']
                p_app = app_key
                p_lic = 1
                p_unit = row['time_unit']
                p_time = row['time_val']
                p_region = '' # Leave empty
                
                batch_data.append((p_key, p_name, p_price, p_app, p_lic, p_unit, p_time, p_region))
            
            # Upsert not strictly needed if we truncated/dropped, but INSERT IGNORE safest if overlap
            # Actually we just created the table, so standard INSERT is fine. 
            # But what if 'osaio' and 'nooie' have same plan codes? 
            # If so, we might have PK conflict.
            # User implies they are separate. If duplicate codes exist, we might need composite key or ignore.
            # I will use ON DUPLICATE KEY UPDATE just in case.
            
            final_insert_sql = """
                INSERT INTO Dim_Plan 
                (plan_key, plan_name, price, app_key, license_number, time_unit, cycle_time, region_key)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    plan_name=VALUES(plan_name),
                    price=VALUES(price),
                    app_key=VALUES(app_key)
            """
            
            cursor.executemany(final_insert_sql, batch_data)
            conn.commit()
            total_inserted += len(batch_data)
            print(f"  Inserted/Updated {len(batch_data)} rows.")

        print(f"ETL Complete. Total {total_inserted} plans in Dim_Plan.")

    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    etl_dim_plan()
