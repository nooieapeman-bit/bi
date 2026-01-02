import mysql.connector
from datetime import datetime, timezone
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

def format_timestamp(ts):
    if not ts:
        return None
    try:
        # Handle 0 or unlikely small numbers if necessary, but standard check:
        ts_int = int(ts)
        if ts_int <= 0:
            return None
        return datetime.fromtimestamp(ts_int, timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    except:
        return None

def run_subscriptions_etl():
    read_conn = get_db_connection()
    write_conn = get_db_connection()
    
    read_cursor = read_conn.cursor(dictionary=True)
    write_cursor = write_conn.cursor()

    try:
        # 1. Truncate Target Table
        print("Truncating Fact_Subscription...")
        write_cursor.execute("TRUNCATE TABLE Fact_Subscription")
        write_conn.commit()

        # Source Configuration
        tasks = [
            {'table': 'osaio.`subscribe_nooie_us`', 'app': 'nooie', 'region': 'us'},
            {'table': 'osaio.`subscribe_nooie_eu`', 'app': 'nooie', 'region': 'eu'},
            {'table': 'osaio.`subscribe_osaio_us`', 'app': 'osaio', 'region': 'us'},
            {'table': 'osaio.`subscribe_osaio_eu`', 'app': 'osaio', 'region': 'eu'}
        ]

        total_inserted = 0
        
        # Track seen IDs to handle duplicates
        # key: subscribe_id, value: dict of source info for logging
        seen_ids = {}
        duplicates_found = []
        
        # Open log file for duplicates
        with open("duplicate_subscriptions.log", "w") as dup_log:
            dup_log.write("Type,SubscribeID,SourceTable,UserUID,PlanKey,StartTime\n")

            for task in tasks:
                table_name = task['table']
                app_key = task['app']
                region_key = task['region']
                
                print(f"Processing {table_name}...")
                
                # Query Source
                # 1. subscribe_id 对应 subscribe_key
                # 2. product_id 对应 plan_key
                # 3. initial_payment_time 对应 first_start_time
                # 4. cancel_time 对应 subscription_end_time
                # 5. next_billing_at 对应 next_billing_time
                # 6. status 对应 subscription_status
                # Plus uid for reference
                query = f"""
                    SELECT 
                        subscribe_id, 
                        product_id, 
                        uid, 
                        initial_payment_time, 
                        cancel_time, 
                        next_billing_at, 
                        status 
                    FROM {table_name}
                """
                read_cursor.execute(query)
                
                batch_size = 2000
                insert_sql = """
                    INSERT INTO Fact_Subscription 
                    (subscription_key, app_key, region_key, plan_key, user_uid, first_start_time, subscription_end_time, next_billing_time, subscription_status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                while True:
                    rows = read_cursor.fetchmany(batch_size)
                    if not rows:
                        break
                        
                    batch_data = []
                    for row in rows:
                        sub_id = row['subscribe_id']
                        if not sub_id:
                            continue # Skip empty IDs
                            
                        # Retrieve other fields for checking and mapping
                        plan_key = row['product_id']
                        user_uid = row['uid']
                        start_time_raw = row['initial_payment_time']
                        start_time_fmt = format_timestamp(start_time_raw)
                        
                        # Check for duplicates (INFO ONLY - Insert All)
                        if sub_id in seen_ids:
                            # Retrieve original info
                            orig = seen_ids[sub_id]
                            
                            # Log both original and duplicate
                            # ORIGINAL
                            dup_log.write(f"ORIGINAL,{sub_id},{orig['table']},{orig['uid']},{orig['plan']},{orig['start']}\n")
                            # DUPLICATE (Current)
                            dup_log.write(f"DUPLICATE,{sub_id},{table_name},{user_uid},{plan_key},{start_time_fmt}\n")
                            
                            duplicates_found.append({
                                'id': sub_id,
                                'original': orig,
                                'duplicate': {
                                    'table': table_name,
                                    'uid': user_uid,
                                    'plan': plan_key,
                                    'start': start_time_fmt
                                }
                            })
                            # CONTINUE INSERTION - Do not skip
                        else:
                            # Store info for future duplicate checking
                            seen_ids[sub_id] = {
                                'table': table_name,
                                'uid': user_uid,
                                'plan': plan_key,
                                'start': start_time_fmt
                            }

                        # Prepare insertion data
                        end_time_fmt = format_timestamp(row['cancel_time'])
                        next_bill_fmt = format_timestamp(row['next_billing_at'])
                        status = str(row['status']) if row['status'] is not None else None
                        
                        batch_data.append((
                            sub_id,
                            app_key, # New
                            region_key, # New
                            plan_key,
                            user_uid,
                            start_time_fmt,
                            end_time_fmt,
                            next_bill_fmt,
                            status
                        ))
                    
                    if batch_data:
                        write_cursor.executemany(insert_sql, batch_data)
                        write_conn.commit()
                        total_inserted += len(batch_data)
                        print(f"  Inserted {len(batch_data)} rows. Total: {total_inserted}")

        print(f"\nETL Complete. Total rows inserted into Fact_Subscription: {total_inserted}")
        
        if duplicates_found:
            print(f"\nDuplicate Subscriptions Found: {len(duplicates_found)}")
            print("See duplicate_subscriptions.log for details.")
            # Print sample to console
            print("\nSample Duplicates:")
            for item in duplicates_found[:5]:
                print("-" * 60)
                print(f"ID: {item['id']}")
                orig = item['original']
                dup = item['duplicate']
                print(f"  EXISTING: Table={orig['table']}, UID={orig['uid']}, Start={orig['start']}")
                print(f"  SKIPPED:  Table={dup['table']}, UID={dup['uid']}, Start={dup['start']}")
            print("-" * 60)
        else:
            print("\nNo duplicates found.")

    except Exception as e:
        print(f"ETL Error: {e}")
        write_conn.rollback()
    finally:
        read_cursor.close()
        write_cursor.close()
        read_conn.close()
        write_conn.close()

if __name__ == "__main__":
    start_time = time.time()
    run_subscriptions_etl()
    print(f"Execution time: {time.time() - start_time:.2f} seconds")
