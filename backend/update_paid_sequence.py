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
    return f"osaio.orders_{app}_{region}"

def update_paid_sequence():
    conn = get_db_connection()
    read_cursor = conn.cursor(dictionary=True)
    write_cursor = conn.cursor()

    try:
        print("Fetching target orders from Fact_Order (2024-01-01 to 2024-02-10)...")
        
        # We need ALL orders in this range to update them.
        # Unlike debug (which grouped by sub key), here we iterate ALL orders 
        # because a subscription usually has multiple valid payments in history, 
        # but in this short window (40 days), a single subscription likely has 1 or 2 orders max.
        # But we must update EACH specific order row.
        
        query = """
            SELECT 
                order_uuid,
                subscription_key, 
                app_key, 
                region_key,
                pay_time
            FROM Fact_Order
            WHERE pay_time >= '2024-01-01 00:00:00'
              AND pay_time <= '2024-02-10 23:59:59'
              AND cny_amount > 0
              AND subscription_key IS NOT NULL
              AND subscription_key != ''
        """
        read_cursor.execute(query)
        targets = read_cursor.fetchall()
        print(f"Fetched {len(targets)} orders to process.")
        
        updates = []
        processed_count = 0
        cache_source_history = {} # cache history for subscription_key to avoid re-querying same sub
        
        start_ts = time.time()
        
        for target in targets:
            sub_key = target['subscription_key']
            uuid = target['order_uuid']
            target_time = target['pay_time']
            app = target['app_key']
            region = target['region_key']
            
            # Optimization: Cache source history for this sub_key
            # Since we iterate orders, if we have multiple orders for same sub in this window, we reuse history.
            
            if sub_key not in cache_source_history:
                source_table = get_source_table(app, region)
                if not source_table:
                    continue

                source_sql = f"""
                    SELECT id, pay_time
                    FROM {source_table}
                    WHERE subscribe_id = %s
                      AND status = 1
                      AND amount > 0
                      AND is_test = 0
                      AND pay_time != 0
                      AND pay_type NOT IN (0, 5)
                    ORDER BY pay_time ASC
                """
                # Use a specific new cursor for this loop query to avoid messing up outer loops? 
                # Actually we used read_cursor for outer, let's use a temp cursor or fetchall first.
                # read_cursor already fetchedall 'targets', so it's free. We can reuse it or use another.
                # But creating a new cursor inside loop is expensive.
                # Let's use a dedicated history_cursor outside.
                
                with conn.cursor(dictionary=True) as hist_cursor:
                    hist_cursor.execute(source_sql, (sub_key,))
                    history = hist_cursor.fetchall()
                    cache_source_history[sub_key] = history
            
            source_history = cache_source_history.get(sub_key)
            if not source_history:
                continue

            # Calculate Rank
            rank = -1
            
            for idx, src_row in enumerate(source_history):
                src_ts = src_row['pay_time']
                src_dt = datetime.utcfromtimestamp(src_ts)
                
                diff = abs((src_dt - target_time).total_seconds())
                
                if diff < 3600: # 1 hour tolerance
                    rank = idx + 1
                    break
            
            if rank != -1:
                updates.append((rank, uuid))
            
            processed_count += 1
            if processed_count % 1000 == 0:
                print(f"  Processed {processed_count} orders...")

        print(f"Calculated ranks for {len(updates)} orders. Executing DB updates...")
        
        update_sql = "UPDATE Fact_Order SET paid_sequence = %s WHERE order_uuid = %s"
        
        batch_size = 2000
        updated_db_count = 0
        
        for i in range(0, len(updates), batch_size):
            batch = updates[i : i+batch_size]
            write_cursor.executemany(update_sql, batch)
            conn.commit()
            updated_db_count += len(batch)
            print(f"  Updated matches {updated_db_count}...")
            
        end_ts = time.time()
        print(f"Update Complete. Updated {updated_db_count} orders.")
        print(f"Time taken: {end_ts - start_ts:.2f} seconds")

    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
    finally:
        read_cursor.close()
        write_cursor.close()
        conn.close()

if __name__ == "__main__":
    update_paid_sequence()
