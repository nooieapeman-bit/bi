import mysql.connector
from datetime import datetime, timedelta
import math
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

def backfill_sequence_and_plan():
    conn = get_db_connection()
    read_cursor = conn.cursor(dictionary=True)
    write_cursor = conn.cursor()

    try:
        # 1. Load Subscription First Start Times
        print("Loading Subscription Start Times...")
        sub_map = {} # {subscription_key: first_start_time}
        
        # We need first_start_time from Fact_Subscription
        read_cursor.execute("SELECT subscription_key, first_start_time FROM Fact_Subscription")
        subs = read_cursor.fetchall()
        for s in subs:
            if s['subscription_key'] and s['first_start_time']:
                sub_map[s['subscription_key']] = s['first_start_time']
                
        print(f"Loaded {len(sub_map)} subscriptions.")

        # 2. Fetch All Paying Orders
        print("Fetching all paying orders (cny_amount > 0, status != 2)...")
        query = """
            SELECT 
                order_uuid, 
                subscription_key, 
                pay_time,
                paid_sequence,
                plan_p_type
            FROM Fact_Order
            WHERE cny_amount > 0
              AND (status != 2 OR status IS NULL)
            ORDER BY subscription_key, pay_time ASC
        """
        read_cursor.execute(query)
        all_orders = read_cursor.fetchall()
        print(f"Fetched {len(all_orders)} orders. Processing groups...")
        
        updates = [] # List of (new_seq, new_plan_type, order_uuid)
        
        current_sub_key = None
        group_orders = []
        
        processed_count = 0
        
        def process_group(orders, sub_start_time):
            if not orders:
                return

            if len(orders) == 1:
                order = orders[0]
                uuid = order['order_uuid']
                pay_time = order['pay_time']
                curr_seq = order['paid_sequence']
                curr_plan = order['plan_p_type']
                
                new_seq = curr_seq
                new_plan = curr_plan
                
                days_diff = -999
                if sub_start_time:
                    diff = pay_time - sub_start_time
                    days_diff = diff.days
                
                if sub_start_time:
                    if days_diff <= 32:
                        new_seq = 1
                    else:
                        # Gap > 32 days
                        # Formula: 2 + (Days - 32) // 365
                        new_seq = 2 + int((days_diff - 32) / 365)
                        if not new_plan:
                            new_plan = 'year'
                else: 
                     # No start time
                     if not curr_seq or curr_seq == 0:
                         new_seq = 1
            
                updates.append((new_seq, new_plan, uuid))
                
            else:
                first_order = orders[0]
                
                # --- 1. Update First Order ---
                f_uuid = first_order['order_uuid']
                f_pay_time = first_order['pay_time']
                f_seq = first_order['paid_sequence']
                f_plan = first_order['plan_p_type']
                
                new_f_seq = f_seq
                new_f_plan = f_plan
                
                days_diff = -999
                if sub_start_time:
                     diff = f_pay_time - sub_start_time
                     days_diff = diff.days
                
                # Logic A: If seq == 0
                if not f_seq or f_seq == 0:
                    if sub_start_time:
                         if days_diff <= 32:
                             new_f_seq = 1
                             found_gap = False
                             if len(orders) > 1:
                                 for i in range(1, len(orders)):
                                     next_o = orders[i]
                                     gap_sec = (next_o['pay_time'] - f_pay_time).total_seconds()
                                     if gap_sec < 3600: continue
                                     
                                     gap_days = (next_o['pay_time'] - f_pay_time).days
                                     if gap_days > 32:
                                         if not new_f_plan: new_f_plan = 'year'
                                     else:
                                         if not new_f_plan: new_f_plan = 'month'
                                     found_gap = True
                                     break
                         else: # Gap > 32 days
                             new_f_seq = 2 + int((days_diff - 32) / 365)
                             if not new_f_plan: new_f_plan = 'year'
                else:
                    if not new_f_plan and len(orders) > 1:
                         for i in range(1, len(orders)):
                             next_o = orders[i]
                             gap_sec = (next_o['pay_time'] - f_pay_time).total_seconds()
                             if gap_sec < 3600: continue
                             
                             gap_days = (next_o['pay_time'] - f_pay_time).days
                             if gap_days > 32:
                                 new_f_plan = 'year'
                             else:
                                 new_f_plan = 'month'
                             break
                
                updates.append((new_f_seq, new_f_plan, f_uuid))
                
                # --- 2. Update Subsequent Orders ---
                base_seq = new_f_seq if new_f_seq else 0
                base_plan = new_f_plan
                
                last_valid_time = f_pay_time
                current_running_seq = base_seq
                
                for i in range(1, len(orders)):
                    o = orders[i]
                    o_uuid = o['order_uuid']
                    o_time = o['pay_time']
                    
                    gap_sec = (o_time - last_valid_time).total_seconds()
                    
                    if gap_sec < 3600:
                        updates.append((current_running_seq, base_plan, o_uuid))
                    else:
                        current_running_seq += 1
                        updates.append((current_running_seq, base_plan, o_uuid))
                        last_valid_time = o_time

        # --- Main Loop ---
        for order in all_orders:
            sub = order['subscription_key']
            
            if sub != current_sub_key:
                if current_sub_key:
                    s_time = sub_map.get(current_sub_key)
                    process_group(group_orders, s_time)
                
                current_sub_key = sub
                group_orders = []
            
            group_orders.append(order)
            
        if current_sub_key and group_orders:
             s_time = sub_map.get(current_sub_key)
             process_group(group_orders, s_time)

        # 3. Execute Updates
        print(f"Calculated updates for {len(updates)} orders. Writing to DB...")
        
        update_sql = "UPDATE Fact_Order SET paid_sequence = %s, plan_p_type = %s WHERE order_uuid = %s"
        
        batch_size = 5000
        total_updates = 0
        start_ts = time.time()
        
        for i in range(0, len(updates), batch_size):
            batch = updates[i : i+batch_size]
            write_cursor.executemany(update_sql, batch)
            conn.commit()
            total_updates += len(batch)
            print(f"  Updated {total_updates} rows...")
            
        print(f"Backfill Complete. Updated {total_updates} rows.")
        print(f"Time: {time.time() - start_ts:.2f}s")

    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
    finally:
        read_cursor.close()
        write_cursor.close()
        conn.close()

if __name__ == "__main__":
    backfill_sequence_and_plan()
