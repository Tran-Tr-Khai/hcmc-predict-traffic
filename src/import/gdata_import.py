import pandas as pd
import mysql.connector
from mysql.connector import Error
import json
from dotenv import load_dotenv
import os

class gTrafficDataImporterToMySQL:
    def __init__(self, db_config):
        self.db_config = db_config
        self.connection = None
        self.cursor = None

    def connect_to_db(self):
        """Tạo kết nối đến cơ sở dữ liệu MySQL."""
        try:
            self.connection = mysql.connector.connect(**self.db_config)
            if self.connection.is_connected():
                db_Info = self.connection.get_server_info()
                print("Connected to MySQL Server version ", db_Info)
                self.cursor = self.connection.cursor()
        except Error as e:
            print("Error while connecting to MySQL", e)

    def create_tables(self, adjacency_columns_count, distance_columns_count):
        """Tạo các bảng trong cơ sở dữ liệu."""
        try:
            # Drop tables if they exist
            self.cursor.execute("DROP TABLE IF EXISTS nodes_df")
            self.cursor.execute("DROP TABLE IF EXISTS adjacency_matrix_df")
            self.cursor.execute("DROP TABLE IF EXISTS distance_matrix_df")

            # Create table nodes
            self.cursor.execute(""" 
            CREATE TABLE nodes_df (
                node_id VARCHAR(255) PRIMARY KEY,
                node_name VARCHAR(255),
                latitude FLOAT,
                longitude FLOAT
            )
            """)
            print("Table 'nodes_df' created successfully.")

            # Create table adjacency_matrix
            self.cursor.execute(""" 
            CREATE TABLE adjacency_matrix_df (
                sensor_id VARCHAR(255),
                {}
            )
            """.format(', '.join([f"s{i} INT" for i in range(adjacency_columns_count)])))
            print("Table 'adjacency_matrix_df' created successfully.")

            # Create table distance_matrix
            self.cursor.execute(""" 
            CREATE TABLE distance_matrix_df (
                sensor_id VARCHAR(255),
                {}
            )
            """.format(', '.join([f"s{i} INT" for i in range(distance_columns_count)])))
            print("Table 'distance_matrix_df' created successfully.")
        except Error as e:
            print("Error while creating tables: ", e)

    def insert_data(self, nodes_df, adjacency_df, distance_df):
        """Chèn dữ liệu vào các bảng."""
        try:
            # Delete existing data from table
            self.cursor.execute("DELETE FROM nodes_df")
            self.cursor.execute("DELETE FROM adjacency_matrix_df")
            self.cursor.execute("DELETE FROM distance_matrix_df")

            # Insert data into nodes table
            for index, row in nodes_df.iterrows():
                self.cursor.execute("INSERT INTO nodes_df (node_id, node_name, latitude, longitude) VALUES (%s, %s, %s, %s)", 
                                   (row['node_id'], row['node_name'], row['latitude'], row['longitude']))

            # Insert data into adjacency_matrix table
            for index, row in adjacency_df.iterrows():
                values = [int(row['sensor_id'])] + [int(x) for x in row[1:]]
                self.cursor.execute(f"INSERT INTO adjacency_matrix_df VALUES ({', '.join(['%s']*len(values))})", values)

            # Insert data into distance_matrix table
            for index, row in distance_df.iterrows():
                values = [int(row['sensor_id'])] + [int(x) for x in row[1:]]
                self.cursor.execute(f"INSERT INTO distance_matrix_df VALUES ({', '.join(['%s']*len(values))})", values)

            # Commit changes
            self.connection.commit()
            print("Insert thành công!")
        except Error as e:
            print("Lỗi khi insert dữ liệu: ", e)


def read_json_file(json_file_path):
    """Đọc dữ liệu từ file JSON."""
    with open(json_file_path, 'r', encoding='utf_8') as file:
        return json.load(file)

def load_data(data):
    """Chuyển đổi dữ liệu từ JSON thành DataFrame."""
    # Process nodes data
    nodes_data = []
    for i, ((lat, lon), name) in enumerate(data['camera-dictionary'].values()):
        node_data = {
            'node_id': str(i),  # Convert to string
            'node_name': name,
            'latitude': lat,
            'longitude': lon,   
        }
        nodes_data.append(node_data)

    nodes_df = pd.DataFrame(nodes_data)

    # Process adjacency matrix
    adjacency_df = pd.DataFrame(data['adjacency-matrix'])
    adjacency_df.columns = [f's{i}' for i in range(len(adjacency_df.columns))]
    adjacency_df.insert(0, 'sensor_id', range(len(adjacency_df)))

    # Process distance matrix
    distance_df = pd.DataFrame(data['distance-matrix'])
    distance_df.columns = [f's{i}' for i in range(len(distance_df.columns))]
    distance_df.insert(0, 'sensor_id', range(len(distance_df)))

    return nodes_df, adjacency_df, distance_df

def main():
    load_dotenv("D:\Project\hcmc_traffic_predict\.env")  # Tải biến môi trường từ file .env

    db_config = {
        'host': os.getenv("DB_HOST"),
        'user': os.getenv("DB_USER"),
        'password': os.getenv("DB_PASSWORD"), 
        'database': os.getenv("DB_NAME")
    }

    json_file_path = 'D:\\Project\\hcmc_traffic_predict\\data\\raw\\hcmc-clustered-graph.json'
    
    # Đọc dữ liệu từ file JSON
    data = read_json_file(json_file_path)
    
    # Tải dữ liệu thành DataFrames
    nodes_df, adjacency_df, distance_df = load_data(data)
    
    # Khởi tạo đối tượng TrafficDataLoader và thực hiện các thao tác
    importer = gTrafficDataImporterToMySQL(db_config)
    importer.connect_to_db()
    importer.create_tables(len(adjacency_df.columns) - 1, len(distance_df.columns) - 1)  # Tạo bảng trước khi chèn dữ liệu
    importer.insert_data(nodes_df, adjacency_df, distance_df)  # Chèn dữ liệu vào các bảng
    
    # Đóng kết nối
    if importer.connection.is_connected():
        importer.cursor.close()
        importer.connection.close()
        print("Kết nối MySQL đã đóng!")

if __name__ == "__main__":
    main()