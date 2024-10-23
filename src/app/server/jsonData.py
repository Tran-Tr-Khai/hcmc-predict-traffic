import pandas as pd
import json
import mysql.connector

class MySQLConnector:
    def __init__(self, host, port, user, password, database):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.connection = None

    def connect(self):
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database,
            )
            print("Connected to MySQL database")
        except mysql.connector.Error as err:
            print(f"Error: {err}")

    def execute_query(self, query):
        if self.connection is not None and self.connection.is_connected():
            cursor = self.connection.cursor()
            cursor.execute(query)
            records = cursor.fetchall()
            column_names = [i[0] for i in cursor.description]  # Get column names for DataFrame
            cursor.close()
            return records, column_names
        else:
            print("No connection to MySQL database. Please connect first.")
            return None, None

    def query_to_dataframe(self, query):
        records, column_names = self.execute_query(query)
        if records:
            df = pd.DataFrame(records, columns=column_names)
            return df
        else:
            print("Error: Query returned no results.")
            return None

    def close(self):
        if self.connection is not None and self.connection.is_connected():
            self.connection.close()
            print("MySQL connection is closed")
        else:
            print("No connection to MySQL database.")



def fetch_data_from_db(host, port, user, password, database):
    connector = MySQLConnector(host, port, user, password, database)
    connector.connect()
    queries = {
        "nodes_df": "SELECT * FROM nodes_df",
        "traffic_data_streaming": "SELECT * FROM traffic_data_streaming",
        "traffic_data_final": "SELECT * FROM traffic_data_final"
    }

    data_frames = {}
    for key, query in queries.items():
        data_frames[key] = connector.query_to_dataframe(query)
    
    connector.close()
    return data_frames

# Sử dụng
host = 'localhost'
port = 3306
user = "root"
password = "Nokhai14442002"
database = "hcmc_traffic_db"

data_frames = fetch_data_from_db(host, port, user, password, database)

camera = data_frames["nodes_df"]
traffic = data_frames["traffic_data_streaming"]



# def initializeData():
#     camera = extract_data(json_file_path)
#     traffic = extract_data(hcm_traffic_json_file_path)

#     # Đọc dữ liệu từ file JSON
#     data_batch, data_on = traffic.read_json_file()

#     traffic_df = traffic.create_dataframe(data_batch)
#     traffic_df = traffic_df.drop("Filename",axis=1)
#     camera_df = camera.read_camera_json()
#     merged_df = pd.merge(camera_df, traffic_df, left_on='node_id', right_on='Sensor')
#     # merged_df[['h', 'min']] = merged_df['Timestamp'].str.split(':', expand=True)
#     # merged_df['Date'] = pd.to_datetime(merged_df['Date'], errors='coerce', format="%Y-%m-%d")
#     # merged_df = merged_df.dropna(subset=['Date'])
#     # merged_df['datetime'] = pd.to_datetime(merged_df['Date'].astype(str) + ' ' + merged_df['Timestamp'])
#     # merged_df['h'] = merged_df['h'].astype(int)
#     # merged_df['min'] = merged_df['min'].astype(int)
#     # merged_df['speed'] = 20
#     return (camera_df,merged_df)