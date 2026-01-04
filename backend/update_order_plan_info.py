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

def update_order_plan_info():
    conn = get_db_connection()
    read_cursor = conn.cursor(dictionary=True)
    write_cursor = conn.cursor()

    try:
        # 1. Load Dim_Plan
        print("Loading Dim_Plan...")
        read_cursor.execute("SELECT plan_key, time_unit, cycle_time FROM Dim_Plan")
        plans = read_cursor.fetchall()
        
        plan_map = {}
        for p in plans:
            # key in DB is called plan_key
            plan_map[p['plan_key']] = {
                'type': p['time_unit'],
                'cycle': p['cycle_time']
            }
            
        print(f"Loaded {len(plan_map)} plans from Dim_Plan.")
        
        # 2. Fetch Orders
        print("Fetching target orders (cny_amount > 0)...")
        # Optimization: Only fetch unique plan_keys needed? 
        # No, we need to update each order_uuid.
        # But we can update by filtering?
        # "UPDATE Fact_Order SET ... WHERE plan_key = %s" ?
        # That would be much faster if we update by plan_key grouping!
        # Good idea.
        
        # Strategy:
        # Iterate through distinct plan_keys in Fact_Order (where cny_amount > 0)
        # If in plan_map -> Update ALL records with that plan_key
        # If not -> Add to missing list
        
        query = """
            SELECT DISTINCT plan_key
            FROM Fact_Order
            WHERE cny_amount > 0
        """
        read_cursor.execute(query)
        order_plan_keys = read_cursor.fetchall()
        print(f"Found {len(order_plan_keys)} distinct plan_keys in orders.")
        
        missing_keys = []
        updates_executed = 0
        
        start_ts = time.time()
        
        for row in order_plan_keys:
            pk = row['plan_key']
            
            if not pk:
                continue
                
            if pk in plan_map:
                info = plan_map[pk]
                new_type = info['type']
                new_cycle = info['cycle']
                
                # Perform Bulk Update for this plan_key
                update_sql = """
                    UPDATE Fact_Order 
                    SET plan_p_type = %s, plan_p_cycle = %s
                    WHERE plan_key = %s AND cny_amount > 0
                """
                write_cursor.execute(update_sql, (new_type, new_cycle, pk))
                # Note: write_cursor.rowcount gives affected rows
                updates_executed += 1
                if updates_executed % 50 == 0:
                    conn.commit()
                    print(f"  Processed {updates_executed} distinct plans...")
            else:
                missing_keys.append(pk)
        
        conn.commit()
        end_ts = time.time()
        
        print(f"Update Complete. Processed {updates_executed} distinct plans covering the orders.")
        print(f"Time taken: {end_ts - start_ts:.2f} seconds")
        
        if missing_keys:
            print("\n" + "="*50)
            print(f"MISSING PLAN KEYS ({len(missing_keys)} found in Orders but not Dim_Plan)")
            print("="*50)
            for k in sorted(missing_keys):
                print(k)
            print("="*50)
        else:
            print("\nAll plan keys matched successfully!")

    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
    finally:
        read_cursor.close()
        write_cursor.close()
        conn.close()

if __name__ == "__main__":
    update_order_plan_info()
