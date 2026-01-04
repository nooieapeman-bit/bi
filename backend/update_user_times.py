import mysql.connector
from datetime import datetime
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

def get_source_table(app, region):
    if not app or not region:
        return None
    # Sanitize to avoid injection (though internal use)
    return f"osaio.orders_{app}_{region}"

def update_user_times(target_table="Dim_User"):
    conn = get_db_connection()
    read_cursor = conn.cursor()
    write_cursor = conn.cursor()

    try:
        # 1. Get Distinct App/Region Pairs active in target_table
        print(f"Discovering App/Region pairs from {target_table}...")
        read_cursor.execute(f"""
            SELECT DISTINCT app_key, region_key 
            FROM {target_table} 
            WHERE app_key IS NOT NULL AND region_key IS NOT NULL AND app_key != '' AND region_key != ''
        """)
        pairs = read_cursor.fetchall()
        print(f"Found {len(pairs)} pairs: {pairs}")

        total_updates = 0
        start_time = time.time()

        for app, region in pairs:
            source_table = get_source_table(app, region)
            print(f"\nProcessing {app.upper()} / {region.upper()} -> {source_table} for {target_table}")
            
            # Check if source table exists (rough check by try/except query)
            try:
                # 2. Aggregate Trial and Payment Times from Source
                print(f"  Aggregating data from {source_table}...")
                
                agg_sql = f"""
                    SELECT 
                        uid,
                        MIN(CASE WHEN amount = 0 THEN pay_time END) as first_trial,
                        MIN(CASE WHEN amount > 0 THEN pay_time END) as first_payment
                    FROM {source_table}
                    WHERE status = 1 
                      AND pay_time > 0
                    GROUP BY uid
                """
                
                read_cursor.execute(agg_sql)
                results = read_cursor.fetchall() # List of (uid, trial, pay)
                
                print(f"  Found {len(results)} users with activity. Prepare updates...")
                
                batch_updates = []
                for row in results:
                    uid, trial_ts, pay_ts = row
                    
                    # Convert TS to Datetime
                    trial_dt = datetime.utcfromtimestamp(trial_ts) if trial_ts else None
                    pay_dt = datetime.utcfromtimestamp(pay_ts) if pay_ts else None
                    
                    if trial_dt or pay_dt:
                        batch_updates.append((trial_dt, pay_dt, uid, app, region))
                
                # 3. Batch Update target_table
                update_sql = f"""
                    UPDATE {target_table} 
                    SET first_trial_time = %s, 
                        first_payment_time = %s 
                    WHERE uid = %s 
                      AND app_key = %s 
                      AND region_key = %s
                """
                
                batch_size = 5000
                count_local = 0
                
                for i in range(0, len(batch_updates), batch_size):
                    batch = batch_updates[i : i+batch_size]
                    write_cursor.executemany(update_sql, batch)
                    conn.commit()
                    count_local += len(batch)
                    print(f"    Updated {count_local} / {len(batch_updates)} users...")
                
                total_updates += count_local
                print(f"  Completed {source_table}. Updated {count_local} users.")

            except mysql.connector.Error as err:
                print(f"  Skipping {source_table} (Error: {err})")
                continue

        end_time = time.time()
        print("\n" + "="*50)
        print(f"ALL DONE for {target_table}. Total Updated: {total_updates} users.")
        print(f"Time Taken: {end_time - start_time:.2f}s")
        print("="*50)

    except Exception as e:
        print(f"Critical Error: {e}")
        conn.rollback()
    finally:
        read_cursor.close()
        write_cursor.close()
        conn.close()

if __name__ == "__main__":
    print("Updating Dim_User...")
    update_user_times("Dim_User")
    print("\nUpdating Dim_User_all...")
    update_user_times("Dim_User_all")
