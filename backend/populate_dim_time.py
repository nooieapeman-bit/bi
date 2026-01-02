import mysql.connector
from datetime import date, timedelta

# MySQL Configuration
DB_CONFIG = {
    'user': 'root',
    'password': 'ne@202509',
    'host': 'localhost',
    'port': 3306,
    'database': 'bi_data',
    'auth_plugin': 'mysql_native_password'
}

def populate_dim_time():
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    try:
        print("Truncating Dim_Time...")
        cursor.execute("TRUNCATE TABLE Dim_Time")
        conn.commit()

        start_date = date(2010, 1, 1)
        end_date = date(2040, 12, 31)
        delta = timedelta(days=1)
        
        print(f"Populating Dim_Time from {start_date} to {end_date}...")
        
        current_date = start_date
        batch_data = []
        batch_size = 1000
        
        while current_date <= end_date:
            time_key = current_date.strftime('%Y%m%d')
            date_str = current_date.strftime('%Y-%m-%d')
            year = current_date.year
            month = current_date.month
            day_of_week = current_date.strftime('%A')
            is_holiday = '0' # Default
            
            batch_data.append((time_key, date_str, year, month, day_of_week, is_holiday))
            
            if len(batch_data) >= batch_size:
                cursor.executemany(
                    "INSERT INTO Dim_Time (time_key, date, year, month, day_of_week, is_holiday) VALUES (%s, %s, %s, %s, %s, %s)",
                    batch_data
                )
                conn.commit()
                batch_data = []
            
            current_date += delta
            
        if batch_data:
             cursor.executemany(
                "INSERT INTO Dim_Time (time_key, date, year, month, day_of_week, is_holiday) VALUES (%s, %s, %s, %s, %s, %s)",
                batch_data
            )
             conn.commit()
             
        print("Done.")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    populate_dim_time()
