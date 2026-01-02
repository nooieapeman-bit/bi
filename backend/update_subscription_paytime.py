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

def update_paytimes():
    conn = get_db_connection()
    read_cursor = conn.cursor(dictionary=True)
    write_cursor = conn.cursor()

    try:
        print("Fetching relevant orders (Osaio, EU, Dec 2025)...")
        # Find latest pay_time for each subscription_key in the specified range
        query = """
            SELECT subscription_key, MAX(pay_time) as latest_pay_time
            FROM Fact_Order
            WHERE app_key = 'osaio'
              AND region_key = 'eu'
              AND pay_time >= '2025-12-01 00:00:00'
              AND pay_time <= '2025-12-31 23:59:59'
              AND subscription_key IS NOT NULL
              AND subscription_key != ''
            GROUP BY subscription_key
        """
        read_cursor.execute(query)
        updates = read_cursor.fetchall()
        
        print(f"Found {len(updates)} subscriptions to update.")
        
        if not updates:
            print("No matching orders found.")
            return

        print("Updating Fact_Subscription...")
        
        update_sql = """
            UPDATE Fact_Subscription
            SET last_paytime = %s
            WHERE subscription_key = %s
        """
        
        # Batch update
        batch_size = 1000
        batch_data = []
        updated_count = 0
        
        start_ts = time.time()
        
        for row in updates:
            batch_data.append((row['latest_pay_time'], row['subscription_key']))
            
            if len(batch_data) >= batch_size:
                write_cursor.executemany(update_sql, batch_data)
                conn.commit()
                updated_count += len(batch_data)
                print(f"  Updated {updated_count} subscriptions...")
                batch_data = []
        
        if batch_data:
            write_cursor.executemany(update_sql, batch_data)
            conn.commit()
            updated_count += len(batch_data)
            
        end_ts = time.time()
        print(f"Update Complete. Total subscriptions updated: {updated_count}")
        print(f"Time taken: {end_ts - start_ts:.2f} seconds")

    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
    finally:
        read_cursor.close()
        write_cursor.close()
        conn.close()

if __name__ == "__main__":
    update_paytimes()
