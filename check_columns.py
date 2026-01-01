import mysql.connector

config = {
    'user': 'root',
    'password': 'ne@202509',
    'host': 'localhost',
    'port': 3306,
    'database': 'bi_data',
    'auth_plugin': 'mysql_native_password'
}

try:
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()
    
    print("Checking Fact_Order columns:")
    cursor.execute("DESCRIBE Fact_Order")
    cols = cursor.fetchall()
    
    found_uid = False
    for col in cols:
        print(f" - {col[0]} ({col[1]})")
        if col[0] == "user_uid":
            found_uid = True
            
    if found_uid:
        print("\nSUCCESS: Found 'user_uid' column!")
    else:
        print("\nFAILURE: 'user_uid' column missing!")

    conn.close()
except Exception as e:
    print(f"Error: {e}")
