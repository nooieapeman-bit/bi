import mysql.connector
import uuid
from datetime import datetime, timezone
from decimal import Decimal

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



def run_debug_etl():
    # Use two connections: one for reading, one for writing to avoid cursor conflicts
    read_conn = get_db_connection()
    write_conn = get_db_connection()
    
    read_cursor = read_conn.cursor(dictionary=True)
    write_cursor = write_conn.cursor()

    try:
        # 0. Truncate Target Table
        print("Truncating Fact_Order...")
        write_cursor.execute("TRUNCATE TABLE Fact_Order")
        write_conn.commit()

        # Source Configuration
        tasks = [
            {'table': 'osaio.orders_osaio_eu', 'region': 'eu'},
            {'table': 'osaio.orders_osaio_us', 'region': 'us'},
            {'table': 'osaio.orders_nooie_us', 'region': 'us'},
            {'table': 'osaio.orders_nooie_eu', 'region': 'eu'}
        ]

        start_ts = int(datetime(2024, 1, 1, tzinfo=timezone.utc).timestamp())
        end_ts = int(datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp())

        for task in tasks:
            table_name = task['table']
            region = task['region']
            
            # Construct info table name...
            info_table_name = table_name.replace('orders_', 'order_amount_info_')
            
            print(f"Processing {table_name} ({region}) JOIN {info_table_name} for range {start_ts}-{end_ts}...")
            
            # Query with JOIN and Filters
            # Filters: status = 1 AND pay_type NOT IN (0, 5)
            # time range on pay_time
            # LIMIT 10 for testing
            query = f"""
                SELECT 
                    o.*, 
                    info.amount_cny, 
                    info.transaction_fee_cny,
                    info.model_code
                FROM {table_name} o
                LEFT JOIN {info_table_name} info ON o.id = info.order_int_id
                WHERE o.pay_time >= {start_ts} 
                  AND o.pay_time < {end_ts} 
                  AND o.status = 1 
                  AND o.pay_type NOT IN (0, 5)
            """
            
            read_cursor.execute(query)
            
            batch_size = 1000
            count = 0
            
            while True:
                rows = read_cursor.fetchmany(batch_size)
                if not rows:
                    break
                    
                for row in rows:
                    target = {}
                    # ... (mapping code unchanged)
                    target['order_uuid'] =  uuid.uuid4().int & (1<<63)-1
                    target['subscription_key'] = row.get('subscribe_id')
                    target['order_id'] = row.get('id')
                    target['user_uid'] = row['uid']
                    target['plan_key'] = row['product_id']
                    target['quantity'] = 1
                    
                    raw_time = row.get('pay_time')
                    if raw_time:
                        target['pay_time'] = datetime.fromtimestamp(int(raw_time), timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
                    else:
                        target['pay_time'] = None

                    target['app_key'] = row['appid']
                    target['region_key'] = region
                    target['device_id'] = row.get('uuid')
                    target['amount'] = row.get('amount')
                    
                    # cny_amount calculation: amount_cny - transaction_fee_cny
                    # Handle possible None values
                    amt_cny = row.get('amount_cny') or 0
                    fee_cny = row.get('transaction_fee_cny') or 0
                    target['cny_amount'] = Decimal(str(amt_cny)) - Decimal(str(fee_cny))
                    
                    target['model_code'] = row.get('model_code')
                    target['credit_amount'] = 0.0
                    target['currency'] = 'CNY'
                    target['type'] = ''
                    target['sequence'] = 0
                    target['paid_sequence'] = 0
                    target['plan_p_type'] = ''
                    target['product_name'] = row.get('product_name')
                    target['description'] = row.get('description')

                    keys = ', '.join([f"`{k}`" for k in target.keys()])
                    values = ', '.join(['%s'] * len(target))
                    sql = f"INSERT INTO Fact_Order ({keys}) VALUES ({values})"
                    
                    try:
                        write_cursor.execute(sql, list(target.values()))
                    except Exception as e:
                        print(f"Error inserting {target['order_id']}: {e}")

                write_conn.commit()
                count += len(rows)
                print(f"Processed {count} rows from {table_name}...")
            
            print(f"Finished {table_name}.")

        print("All Done.")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        read_conn.close()
        write_conn.close()

if __name__ == "__main__":
    run_debug_etl()
