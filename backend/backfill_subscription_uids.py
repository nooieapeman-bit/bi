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

def backfill_uids_refined():
    conn = get_db_connection()
    read_cursor = conn.cursor(dictionary=True)
    write_cursor = conn.cursor()

    try:
        # Step 1: Find subscriptions that are missing a user_uid
        print("Step 1: finding subscriptions with missing user_uid...")
        missing_query = """
            SELECT subscription_key 
            FROM Fact_Subscription 
            WHERE user_uid IS NULL OR user_uid = ''
        """
        read_cursor.execute(missing_query)
        missing_rows = read_cursor.fetchall()
        
        missing_keys = [row['subscription_key'] for row in missing_rows if row['subscription_key']]
        print(f"Found {len(missing_keys)} subscriptions missing user_uid.")
        
        if not missing_keys:
            print("No missing UIDs found. Exiting.")
            return

        # Step 2: Look up these keys in Fact_Order to find the user_uid
        print("Step 2: Looking up user_uids in Fact_Order...")
        
        # We process in chunks to avoid massive IN clauses if there are many
        chunk_size = 5000
        found_mappings = {} # key -> uid
        
        for i in range(0, len(missing_keys), chunk_size):
            chunk = missing_keys[i:i + chunk_size]
            format_strings = ','.join(['%s'] * len(chunk))
            
            lookup_query = f"""
                SELECT subscription_key, user_uid 
                FROM Fact_Order 
                WHERE subscription_key IN ({format_strings})
                  AND user_uid IS NOT NULL 
                  AND user_uid != ''
            """
            read_cursor.execute(lookup_query, tuple(chunk))
            results = read_cursor.fetchall()
            
            for row in results:
                # Store the first valid one we find (or overwrite, order doesn't strictly matter if 1-to-1)
                found_mappings[row['subscription_key']] = row['user_uid']
                
        print(f"Found {len(found_mappings)} matching user_uids in Fact_Order.")
        
        if not found_mappings:
            print("Could not find any matching UIDs in orders for these subscriptions.")
            return

        # Step 3: Update Fact_Subscription
        print("Step 3: Updating Fact_Subscription...")
        
        update_sql = """
            UPDATE Fact_Subscription
            SET user_uid = %s
            WHERE subscription_key = %s
        """
        
        batch_data = []
        updated_count = 0
        
        start_ts = time.time()
        
        for sub_key, uid in found_mappings.items():
            batch_data.append((uid, sub_key))
            
            if len(batch_data) >= 2000:
                write_cursor.executemany(update_sql, batch_data)
                conn.commit()
                updated_count += len(batch_data)
                print(f"  Updated {updated_count} rows...")
                batch_data = []
        
        if batch_data:
            write_cursor.executemany(update_sql, batch_data)
            conn.commit()
            updated_count += len(batch_data)
        
        end_ts = time.time()
        print(f"Backfill Complete. Updated {updated_count} subscriptions with UIDs.")
        print(f"Time taken: {end_ts - start_ts:.2f} seconds")

    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
    finally:
        read_cursor.close()
        write_cursor.close()
        conn.close()

if __name__ == "__main__":
    backfill_uids_refined()
