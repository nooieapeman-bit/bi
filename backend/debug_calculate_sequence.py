import mysql.connector
from datetime import datetime

# MySQL Configuration
DB_CONFIG = {
    'user': 'root',
    'password': 'ne@202509',
    'host': 'localhost',
    'port': 3306,
    'database': 'bi_data', # Primary DB
    'auth_plugin': 'mysql_native_password'
}

def get_db_connection():
    return mysql.connector.connect(**DB_CONFIG)

def get_source_table(app, region):
    # Map app/region to source table name in 'osaio' database
    # Assuming standard naming: osaio.orders_{app}_{region}
    # app: 'osaio', 'nooie'
    # region: 'eu', 'us'
    if not app or not region:
        return None
    return f"osaio.orders_{app}_{region}"

def debug_sequence():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    try:
        print("Fetching target orders from Fact_Order (2024-01-01 to 2024-02-10)...")
        
        # 1. Get Target Orders
        # Group by subscription_key to get the earliest one in the window for that sub
        query = """
            SELECT 
                subscription_key, 
                app_key, 
                region_key,
                MIN(pay_time) as first_pay_time
            FROM Fact_Order
            WHERE pay_time >= '2024-01-01 00:00:00'
              AND pay_time <= '2024-02-10 23:59:59'
              AND cny_amount > 0
              AND subscription_key IS NOT NULL
              AND subscription_key != ''
            GROUP BY subscription_key, app_key, region_key
            LIMIT 20
        """
        # LIMIT 20 for "Try a few" as requested
        
        cursor.execute(query)
        targets = cursor.fetchall()
        print(f"Fetched {len(targets)} distinct subscriptions to inspect.")
        
        for target in targets:
            sub_key = target['subscription_key']
            app = target['app_key']
            region = target['region_key']
            target_time = target['first_pay_time']
            
            source_table = get_source_table(app, region)
            if not source_table:
                print(f"Skipping {sub_key}: Unknown app/region ({app}/{region})")
                continue
            
            print(f"\nProcessing Subscription: {sub_key} ({source_table})")
            print(f"  Target Fact_Order Time: {target_time}")
            
            # 2. Query Source Table for Full History
            # Filters: status=1, amount>0, is_test=0, pay_time!=0, pay_type not in (0,5)
            # Match subscribe_id = sub_key
            
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
            
            cursor.execute(source_sql, (sub_key,))
            source_history = cursor.fetchall()
            
            if not source_history:
                print(f"  WARNING: No matching valid orders found in source table {source_table}!")
                continue
                
            # 3. Find Rank
            rank = -1
            matched_source_id = None
            
            for idx, src_row in enumerate(source_history):
                # How to match? 
                # Fact_Order.pay_time should match Source.pay_time (allowing for conversion drifts?)
                # Or Fact_Order.order_id matches Source.id? 
                # Let's check time match first. Fact_Order timestamp vs Source int/timestamp?
                # Source usually unix timestamp int? "pay_time != 0" implies int.
                # Let's convert source timestamp to datetime for comparison.
                
                src_ts = src_row['pay_time']
                src_dt = datetime.utcfromtimestamp(src_ts)
                
                # Check if this is our target order
                # Comparison might be tricky due to timezone or second precision.
                # User requested larger tolerance and UTC.
                
                diff = abs((src_dt - target_time).total_seconds())
                
                if diff < 3600: # Increased to 1 hour
                    rank = idx + 1 # 1-based index
                    matched_source_id = src_row['id']
                    # Re-calculate diff for display
                    print(f"  MATCH FOUND at Index {idx} (Seq {rank})!")
                    print(f"    Source ID: {matched_source_id}")
                    print(f"    Source Time: {src_dt} (TS: {src_ts})")
                    print(f"    Target Time: {target_time}")
                    print(f"    Time Diff: {diff:.2f}s")
                    break
            
            if rank == -1:
                 print("  NO MATCH found in source history for the target timestamp.")
                 print(f"    Target: {target_time}")
                 if source_history:
                     print(f"    Earliest Source: {datetime.fromtimestamp(source_history[0]['pay_time'])}")
                     print(f"    Latest Source: {datetime.fromtimestamp(source_history[-1]['pay_time'])}")
            else:
                print(f"  -> Calculated paid_sequence = {rank}")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    debug_sequence()
