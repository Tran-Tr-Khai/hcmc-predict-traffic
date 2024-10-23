import pandas as pd
import mysql.connector
import json
from dotenv import load_dotenv
import os

class TrafficDataImporterToMySQL:
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database

    def create_connection(self):
        """Tạo kết nối đến cơ sở dữ liệu MySQL."""
        try:
            connection = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database,
                ssl_disabled=False
            )
            if connection.is_connected():
                print("Connected to MySQL")
                return connection
        except mysql.connector.Error as err:
            print(f"Error: {err}")
            return None

    def create_mysql_table(self):
        connection = self.create_connection()
        if connection:
            try:
                with connection.cursor() as cursor:
                    # Tạo bảng history_hcmc_traffic_data
                    create_history_table_query = """
                        CREATE TABLE IF NOT EXISTS history_hcmc_traffic_data (
                            Date VARCHAR(255),
                            Sensor INT,
                            Filename VARCHAR(255),
                            Count INT,
                            Timestamp VARCHAR(20)
                        );
                    """
                    cursor.execute(create_history_table_query)

                    # Tạo bảng real_hcmc_traffic_data
                    create_real_table_query = """
                        CREATE TABLE IF NOT EXISTS real_hcmc_traffic_data (
                            Date VARCHAR(255),
                            Sensor INT,
                            Filename VARCHAR(255),
                            Count INT,
                            Timestamp VARCHAR(20)
                        );
                    """
                    cursor.execute(create_real_table_query)

                connection.commit()
            except mysql.connector.Error as err:
                print(f"Error: {err}")
            finally:
                connection.close()

    def insert_data_to_mysql(self, df):
        connection = self.create_connection()
        if connection:
            try:
                with connection.cursor() as cursor:
                    # Xóa tất cả các dòng trong cả hai bảng
                    delete_history_query = "DELETE FROM history_hcmc_traffic_data"
                    delete_real_query = "DELETE FROM real_hcmc_traffic_data"
                    cursor.execute(delete_history_query)
                    cursor.execute(delete_real_query)

                    # Chia dữ liệu thành hai phần
                    real_data = df[df['Date'] == '2022-05-09']
                    history_data = df[df['Date'] != '2022-05-09']

                    # Nhập dữ liệu real
                    if not real_data.empty:
                        data_to_insert_real = [tuple(x) for x in real_data.values]
                        # Chia nhỏ dữ liệu real
                        for chunk in self.chunks(real_data, 1000):
                            insert_real_query = """
                                INSERT INTO real_hcmc_traffic_data (Date, Sensor, Filename, Count, Timestamp) 
                                VALUES (%s, %s, %s, %s, %s)
                            """
                            cursor.executemany(insert_real_query, [tuple(x) for x in chunk.values])

                    # Nhập dữ liệu history
                    if not history_data.empty:
                        data_to_insert_history = [tuple(x) for x in history_data.values]
                        # Chia nhỏ dữ liệu history
                        for chunk in self.chunks(history_data, 1000):
                            insert_history_query = """
                                INSERT INTO history_hcmc_traffic_data (Date, Sensor, Filename, Count, Timestamp) 
                                VALUES (%s, %s, %s, %s, %s)
                            """
                            cursor.executemany(insert_history_query, [tuple(x) for x in chunk.values])

                    connection.commit()
            except mysql.connector.Error as err:
                print(f"Error: {err}")
            finally:
                connection.close()

    @staticmethod
    def chunks(df, chunk_size):
        return [df[i:i + chunk_size] for i in range(0, len(df), chunk_size)]


def read_json_file(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        return json.load(file)

def create_dataframe(data):
    rows = []
    for date, sensors in data.items():
        for sensor, files in sensors.items():
            for filename, details in files.items():
                timestamp_str = f"{details['timestamp'][0]:02}:{details['timestamp'][1]:02}"
                rows.append([date, sensor, filename, details['count'], timestamp_str])
    return pd.DataFrame(rows, columns=['Date', 'Sensor', 'Filename', 'Count', 'Timestamp'])

def main():
    load_dotenv("D:\Project\hcmc_traffic_predict\.env")  # Tải biến môi trường từ file .env
    file_path = "D:\\Project\\hcmc_traffic_predict\\data\\raw\\hcmc-traffic-data.json"
    
    # Lấy thông tin kết nối từ biến môi trường
    host = os.getenv("MySQL_HOST")
    user = os.getenv("MySQL_USER")
    password = os.getenv("MySQL_PASSWORD")
    database = os.getenv("MYSQL_NAME")

    # Đọc dữ liệu từ tệp JSON
    data = read_json_file(file_path)
    
    # Tạo DataFrame từ dữ liệu
    df = create_dataframe(data)

    # Tạo đối tượng loader và thực hiện các thao tác
    importer = TrafficDataImporterToMySQL(host, user, password, database)
    importer.create_mysql_table()
    importer.insert_data_to_mysql(df)

    print("Dữ liệu đã được chèn vào bảng 'real_hcmc_traffic_data' và 'history_hcmc_traffic_data'.")

if __name__ == "__main__":
    main()