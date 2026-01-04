import mysql.connector
from datetime import timedelta
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

def deduplicate_orders_final():
    conn = get_db_connection()
    read_cursor = conn.cursor(dictionary=True)
    write_cursor = conn.cursor()

    try:
        print("Fetching paying orders (status != 2) for deduplication...")
        query = """
            SELECT 
                order_uuid, order_id,
                plan_key, subscription_key, app_key, region_key, user_uid, device_id, cny_amount, 
                pay_time
            FROM Fact_Order
            WHERE cny_amount > 0
              AND (status != 2 OR status IS NULL)
            ORDER BY 
                plan_key, subscription_key, app_key, region_key, user_uid, device_id, cny_amount,
                pay_time DESC
        """
        read_cursor.execute(query)
        rows = read_cursor.fetchall()
        print(f"Fetched {len(rows)} rows. Processing...")

        to_mark_ids = []
        
        if not rows:
            print("No rows found.")
            return

        prev_row = None
        reference_row = None
        
        group_keys = ['plan_key', 'subscription_key', 'app_key', 'region_key', 'user_uid', 'device_id', 'cny_amount']
        
        for row in rows:
            curr_key_vals = tuple(row[k] for k in group_keys)
            
            if prev_row is None:
                prev_row = row
                reference_row = row
                continue
            
            prev_key_vals = tuple(prev_row[k] for k in group_keys)
            
            if curr_key_vals == prev_key_vals:
                # Same Group
                ref_time = reference_row['pay_time']
                curr_time = row['pay_time']
                ref_oid = reference_row['order_id']
                curr_oid = row['order_id']
                
                is_dupe = False
                
                if ref_time and curr_time and ref_oid is not None and curr_oid is not None:
                    time_diff = ref_time - curr_time
                    oid_diff = abs(ref_oid - curr_oid)
                    
                    # FINAL LOGIC: Time < 1h AND OrderID Gap < 10
                    if time_diff < timedelta(hours=1) and oid_diff < 10:
                        is_dupe = True
                
                if is_dupe:
                    to_mark_ids.append(row['order_uuid'])
                else:
                    reference_row = row
            else:
                reference_row = row
            
            prev_row = row
            
        print(f"Found {len(to_mark_ids)} duplicates matching strict criteria (Gap < 10).")
        
        if not to_mark_ids:
            return

        # EXECUTE UPDATES
        print(f"Executing updates for ALL {len(to_mark_ids)} rows...")
        
        update_sql = "UPDATE Fact_Order SET status = 2 WHERE order_uuid = %s"
        
        # Batch update
        batch_size = 2000 
        batch_data = [(i,) for i in to_mark_ids]
        
        start_ts = time.time()
        updated_count = 0
        
        for i in range(0, len(batch_data), batch_size):
            batch = batch_data[i : i+batch_size]
            write_cursor.executemany(update_sql, batch)
            conn.commit()
            updated_count += len(batch)
            print(f"  Marked {updated_count} rows...")
            
        end_ts = time.time()
        print(f"Deduplication Complete. Updated {updated_count} rows to status=2.")
        print(f"Time taken: {end_ts - start_ts:.2f} seconds")

    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
    finally:
        read_cursor.close()
        write_cursor.close()
        conn.close()

if __name__ == "__main__":
    deduplicate_orders_final()
