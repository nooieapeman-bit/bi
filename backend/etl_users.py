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

def run_users_etl():
    read_conn = get_db_connection()
    write_conn = get_db_connection()
    
    read_cursor = read_conn.cursor(dictionary=True)
    write_cursor = write_conn.cursor()

    try:
        # 1. Truncate Target Table
        print("Truncating Dim_User...")
        write_cursor.execute("TRUNCATE TABLE Dim_User")
        write_conn.commit()

        # Source Configuration
        tasks = [
            {'table': 'osaio.`users-nooie-us`', 'app': 'nooie', 'region': 'us', 'app_id': 'nooie_us'},
            {'table': 'osaio.`users-nooie-eu`', 'app': 'nooie', 'region': 'eu', 'app_id': 'nooie_eu'},
            {'table': 'osaio.`users-osaio-us`', 'app': 'osaio', 'region': 'us', 'app_id': 'osaio_us'},
            {'table': 'osaio.`users-osaio-eu`', 'app': 'osaio', 'region': 'eu', 'app_id': 'osaio_eu'}
        ]

        total_inserted = 0

        # Track seen UIDs to handle duplicates across tables
        # key: uid, value: dict of source info
        seen_uids = {}
        duplicates_found = []
        
        # Open log file for duplicates
        with open("duplicate_uids.log", "w") as dup_log:
            dup_log.write("Type,UID,SourceTable,App,Region,Country,RegisterTime\n")

            for task in tasks:
                table_name = task['table']
                app_key = task['app']
                region_key = task['region']
                app_id = task['app_id']
                
                print(f"Processing {table_name}...")
                
                # Query Source
                query = f"SELECT uid, register_time, register_country FROM {table_name}"
                read_cursor.execute(query)
                
                batch_size = 2000
                insert_sql = """
                    INSERT INTO Dim_User (uid, app_key, region_key, country, join_date)
                    VALUES (%s, %s, %s, %s, %s)
                """
                
                while True:
                    rows = read_cursor.fetchmany(batch_size)
                    if not rows:
                        break
                        
                    batch_data = []
                    for row in rows:
                        uid = row['uid']
                        country = row['register_country']
                        reg_time = row['register_time']
                        
                        # Check for duplicates
                        if uid in seen_uids:
                            # Retrieve original info
                            orig = seen_uids[uid]
                            
                            # Log both original and current for comparison
                            # ORIGINAL
                            dup_log.write(f"ORIGINAL,{uid},{orig['table']},{orig['app']},{orig['region']},{orig['country']},{orig['time']}\n")
                            # DUPLICATE (Current)
                            dup_log.write(f"DUPLICATE,{uid},{table_name},{app_key},{region_key},{country},{reg_time}\n")
                            
                            duplicates_found.append({
                                'uid': uid,
                                'original': orig,
                                'duplicate': {
                                    'table': table_name,
                                    'app': app_key,
                                    'region': region_key,
                                    'country': country,
                                    'time': reg_time
                                }
                            })
                            continue
                        
                        # Store info for future duplicate checking
                        seen_uids[uid] = {
                            'table': table_name,
                            'app': app_key,
                            'region': region_key,
                            'country': country,
                            'time': reg_time
                        }

                        # Mapping
                        # Convert register_time (unix ts) to datetime
                        join_date = None
                        if reg_time:
                            try:
                                join_date = datetime.fromtimestamp(int(reg_time), timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
                            except:
                                join_date = None
                        
                        batch_data.append((
                            uid,
                            app_id, 
                            region_key,
                            country,
                            join_date
                        ))
                    
                    if batch_data:
                        write_cursor.executemany(insert_sql, batch_data)
                        write_conn.commit()
                        total_inserted += len(batch_data)
                        print(f"  Inserted {len(batch_data)} rows. Total: {total_inserted}")

        print(f"\nETL Complete. Total rows inserted into Dim_User: {total_inserted}")
        
        if duplicates_found:
            print("\nDuplicate UIDs and Conflicts:")
            for item in duplicates_found:
                print("-" * 60)
                print(f"UID: {item['uid']}")
                orig = item['original']
                dup = item['duplicate']
                print(f"  EXISTING: Table={orig['table']}, App={orig['app']}, Region={orig['region']}, Country={orig['country']}, Time={orig['time']}")
                print(f"  SKIPPED:  Table={dup['table']}, App={dup['app']}, Region={dup['region']}, Country={dup['country']}, Time={dup['time']}")
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
    run_users_etl()
    print(f"Execution time: {time.time() - start_time:.2f} seconds")
