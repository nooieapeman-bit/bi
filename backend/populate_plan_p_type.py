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

def populate_plan_types():
    conn = get_db_connection()
    read_cursor = conn.cursor(dictionary=True)
    write_cursor = conn.cursor()

    try:
        print("Fetching orders with cny_amount > 0...")
        query = """
            SELECT order_uuid, product_name
            FROM Fact_Order
            WHERE cny_amount > 0
              AND product_name IS NOT NULL
        """
        read_cursor.execute(query)
        rows = read_cursor.fetchall()
        print(f"Fetched {len(rows)} rows. Processing...")
        
        updates = [] # List of (plan_p_type, order_uuid)
        unknown_products = set()
        
        for row in rows:
            p_name = row['product_name'].lower().strip()
            uuid = row['order_uuid']
            
            p_type = None
            
            # Logic Order matches User Request
            # 1. Monthly
            if 'monthly' in p_name:
                p_type = 'month'
            # 2. Yearly (annually OR yearly OR annual OR per year)
            elif 'annually' in p_name or 'yearly' in p_name or 'annual' in p_name or 'per year' in p_name:
                p_type = 'year'
            # 3. Half-year
            elif 'half-year' in p_name:
                p_type = 'half-year'
            else:
                # Unknown
                unknown_products.add(row['product_name'])
                continue
            
            if p_type:
                updates.append((p_type, uuid))
        
        # Execute Updates
        if updates:
            print(f"Identified {len(updates)} rows with known plan types. Updating DB...")
            update_sql = "UPDATE Fact_Order SET plan_p_type = %s WHERE order_uuid = %s"
            
            batch_size = 5000
            total_updated = 0
            start_ts = time.time()
            
            for i in range(0, len(updates), batch_size):
                batch = updates[i : i+batch_size]
                write_cursor.executemany(update_sql, batch)
                conn.commit()
                total_updated += len(batch)
                print(f"  Updated {total_updated} rows...")
            
            print(f"Update Complete. Time: {time.time() - start_ts:.2f}s")
        else:
            print("No known plan types identified.")

        # Report Unknowns
        if unknown_products:
            print("\n" + "="*50)
            print(f"FOUND {len(unknown_products)} UNKNOWN PRODUCT NAMES")
            print("="*50)
            for name in sorted(list(unknown_products)):
                print(name)
            print("="*50)
            print("Please review and provide classification for these.")
        else:
            print("\nAll product names were successfully classified!")

    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
    finally:
        read_cursor.close()
        write_cursor.close()
        conn.close()

if __name__ == "__main__":
    populate_plan_types()
