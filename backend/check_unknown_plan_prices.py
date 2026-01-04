import mysql.connector

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

def check_prices():
    conn = get_db_connection()
    read_cursor = conn.cursor(dictionary=True)

    try:
        print("Fetching unclassified products and their prices...")
        query = """
            SELECT product_name, cny_amount
            FROM Fact_Order
            WHERE cny_amount > 0
              AND (plan_p_type IS NULL OR plan_p_type = '')
        """
        read_cursor.execute(query)
        rows = read_cursor.fetchall()
        
        # Aggregate data: Product Name -> Set of Prices
        price_map = {}
        for row in rows:
            name = row['product_name']
            if name:
                name = name.strip()
            else:
                name = "(Empty/Null)"
                
            price = row['cny_amount']
            
            if name not in price_map:
                price_map[name] = set()
            price_map[name].add(float(price)) # Use float for cleaner set display
            
        print("\n" + "="*80)
        print(f"{'Product Name':<40} | {'CNY Amount(s)':<40}")
        print("="*80)
        
        for name in sorted(price_map.keys()):
            prices = sorted(list(price_map[name]))
            price_str = ", ".join([f"{p:.2f}" for p in prices])
            print(f"{name:<40} | {price_str:<40}")
            
        print("="*80)
        print(f"Total unclassified entries found: {len(rows)}")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        read_cursor.close()
        conn.close()

if __name__ == "__main__":
    check_prices()
