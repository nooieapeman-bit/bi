import mysql.connector
import csv

DB_CONFIG = {
    'user': 'root',
    'password': 'ne@202509',
    'host': 'localhost',
    'port': 3306,
    'database': 'bi_data',
    'auth_plugin': 'mysql_native_password'
}

def check_missing_uids():
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    print("Checking for UIDs in Fact_Order that are missing in Dim_User AND have cny_amount > 0...")
    query = """
    SELECT f.user_uid, SUM(f.cny_amount) as total_amount, MAX(f.pay_time) as last_pay_time
    FROM Fact_Order f 
    LEFT JOIN Dim_User d ON f.user_uid = d.uid 
    WHERE d.uid IS NULL
      AND f.cny_amount > 0
    GROUP BY f.user_uid
    ORDER BY total_amount DESC
    """
    
    cursor.execute(query)
    results = cursor.fetchall()
    
    print(f"Count of Missing UIDs with Revenue: {len(results)}")
    
    if results:
        output_file = 'missing_paying_uids.log'
        with open(output_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['Missing_UID', 'Total_CNY_Amount', 'Last_Pay_Time'])
            for row in results:
                writer.writerow(row)
                print(f"Missing: {row[0]}, Amount: {row[1]}, Last Pay: {row[2]}")
        print(f"Missing paying UIDs saved to {output_file}")
    else:
        print("No missing paying UIDs found.")

    conn.close()

if __name__ == "__main__":
    check_missing_uids()
