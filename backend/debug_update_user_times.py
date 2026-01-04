import mysql.connector
from datetime import datetime

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

def debug_user_times():
    conn = get_db_connection()
    read_cursor = conn.cursor(dictionary=True) # Fetch users
    
    try:
        # 1. Fetch Sample Users
        print("Fetching sample users from Dim_User (Limit 20)...")
        query = """
            SELECT DISTINCT user_uid as uid, app_key, region_key
            FROM Fact_Order
            WHERE cny_amount > 0 
              AND user_uid IS NOT NULL
              AND app_key IS NOT NULL
              AND region_key IS NOT NULL
            LIMIT 20
        """
        read_cursor.execute(query)
        users = read_cursor.fetchall()
        print(f"Fetched {len(users)} users.")
        
        for user in users:
            uid = user['uid']
            app = user['app_key']
            region = user['region_key']
            
            source_table = get_source_table(app, region)
            if not source_table:
                continue
            
            print(f"\nProcessing User: {uid} ({app}/{region})")
            
            # Use separate cursor for source queries
            with conn.cursor(dictionary=True) as src_cursor:
                # 2. Find First Trial Time
                # status=1, amount=0, min(pay_time)
                # Note: pay_time > 0 usually? "pay_time != 0"
                trial_sql = f"""
                    SELECT min(pay_time) as ft
                    FROM {source_table}
                    WHERE uid = %s
                      AND status = 1
                      AND amount = 0
                      AND pay_time > 0
                """
                
                src_cursor.execute(trial_sql, (uid,))
                trial_res = src_cursor.fetchone()
                
                trial_ts = trial_res['ft'] if trial_res else None
                trial_dt = None
                if trial_ts:
                    trial_dt = datetime.utcfromtimestamp(trial_ts) 
                
                # 3. Find First Payment Time
                # status=1, amount > 0, min(pay_time)
                pay_sql = f"""
                    SELECT min(pay_time) as fp
                    FROM {source_table}
                    WHERE uid = %s
                      AND status = 1
                      AND amount > 0
                      AND pay_time > 0
                """
                
                src_cursor.execute(pay_sql, (uid,))
                pay_res = src_cursor.fetchone()
                
                pay_ts = pay_res['fp'] if pay_res else None
                pay_dt = None
                if pay_ts:
                    pay_dt = datetime.utcfromtimestamp(pay_ts)
                
                print(f"  First Trial:   {trial_dt} (TS: {trial_ts})")
                print(f"  First Payment: {pay_dt} (TS: {pay_ts})")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        read_cursor.close()
        conn.close()

if __name__ == "__main__":
    debug_user_times()
