import mysql.connector
import pandas as pd
from dotenv import load_dotenv 
import os
import torch
import dgl
import numpy as np 
import scipy.sparse as sp


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
        if self.connection is not None:
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
        if self.connection is not None:
            self.connection.close()
            print("MySQL connection is closed")
        else:
            print("No connection to MySQL database.")



def remove_missing_sensors(df, missing_sensors):
    columns_to_drop = ['s' + str(sensor) for sensor in missing_sensors]
    index_to_drop = [sensor for sensor in missing_sensors]
    df = df.drop(columns_to_drop, axis=1)
    df = df.drop(index_to_drop, axis=0)

    return df


def create_graph_from_matrices(adj_matrix, dist_matrix, present_sensors): 
    adj_tensor = torch.tensor(adj_matrix)
    dist_tensor = torch.tensor(dist_matrix, dtype=torch.float32)

    ## Tạo đồ thị từ danh sách cạnh
    src, dst = adj_tensor.nonzero(as_tuple=True)
    graph = dgl.graph((src, dst), num_nodes=adj_tensor.shape[0])

    # Thêm thuộc tính trọng số cho các cạnh từ distance_matrix
    edge_weights = dist_tensor[src, dst]
    graph.edata['weight'] = edge_weights

    # Map id sensor với node index
    sensor_id_to_node_idx_map = {}
    for i, sensor_id in enumerate(present_sensors):
        node_idx = i  
        sensor_id_to_node_idx_map[sensor_id] = node_idx

    # Tạo thuộc tính sensor_id cho node
    graph.ndata['sensor_id'] = torch.zeros(adj_tensor.shape[0], dtype=torch.int64)
    for sensor_id in present_sensors:
        node_idx = sensor_id_to_node_idx_map[sensor_id]
        graph.ndata['sensor_id'][node_idx] = sensor_id

    return graph


def positional_encoding(g, pos_enc_dim):
    """
        Graph positional encoding v/ Laplacian eigenvectors
    """
    # Laplacian
    A = g.adj_external(scipy_fmt='csr')
    in_degrees = g.in_degrees().float().clamp(min=1)  # Directly access in-degrees
    N = sp.diags(np.power(in_degrees.numpy(), -0.5), dtype=float)
    L = sp.eye(g.number_of_nodes()) - N * A * N

    # Eigenvectors with numpy
    EigVal, EigVec = np.linalg.eig(L.toarray())
    idx = EigVal.argsort() # increasing order
    EigVal, EigVec = EigVal[idx], np.real(EigVec[:,idx])
    lap_pos = torch.from_numpy(EigVec[:,1:pos_enc_dim+1]).float()

    # # Eigenvectors with scipy
    # EigVal, EigVec = sp.linalg.eigs(L, k=pos_enc_dim+1, which='SR')
    # EigVec = EigVec[:, EigVal.argsort()] # increasing order
    # g.ndata['pos_enc'] = torch.from_numpy(np.abs(EigVec[:,1:pos_enc_dim+1])).float()
    return lap_pos


def main(): 
    load_dotenv("D:\Project\hcmc_traffic_predict\.env")
    host = os.getenv("MySQL_HOST")
    port = 3306
    user = os.getenv("MySQL_USER")
    password = os.getenv("MySQL_PASSWORD") 
    database = os.getenv("MySQL_NAME")
    connector = MySQLConnector(host, port, user, password, database)
    # Kết nối đến cơ sở dữ liệu
    connector.connect()

    # Thực hiện truy vấn SQL và chuyển kết quả thành DataFrame
    query1 = "SELECT * FROM nodes_df"
    nodes_df = connector.query_to_dataframe(query1)
    nodes_df['node_id'] = nodes_df['node_id'].astype(int)
    print(nodes_df.head())

    query2 = "SELECT * FROM adjacency_matrix_df"  # Thay đổi truy vấn SQL tại đây
    adjacency_matrix_df = connector.query_to_dataframe(query2)
    print(adjacency_matrix_df.head()) 

    query3 = "SELECT * FROM distance_matrix_df"
    distance_matrix_df = connector.query_to_dataframe(query3)
    print(distance_matrix_df.head())
    # Đóng kết nối
    connector.close()
    


    # Xử lý dữ liệu
         
if __name__ == "__main__": 
    main()