import numpy as np
import pandas as pd
import psycopg2
from dotenv import load_dotenv
import os


class PgresSQLConnector:
    def __init__(self, host, port, user, password, database):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.connection = None

    def connect(self):
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database,
            )
            print("Connected to PostgresSQL database")
        except psycopg2.Error as err:
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
            print("No connection to PostgresSQL database. Please connect first.")
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
            print("PostgresSQL connection is closed")
        else:
            print("No connection to PostgresSQL database.")



def generate_sub_dfs(df, window_size=12):
    num_rows = len(df)
    sub_dfs = []
    for start in range(0, num_rows, window_size):
        end = start + window_size
        if end <= num_rows:
            sub_df = df[start:end].copy()
            sub_dfs.append(sub_df)
        else:
            break  # Dừng lại nếu không đủ 12 hàng
    return sub_dfs

# Hàm tạo dữ liệu đầu vào
def generate_graph_seq2seq_input_data(df, x_offsets, add_time_in_day=True, add_day_in_week=False):
    num_samples, num_nodes = df.shape
    data = np.expand_dims(df.values, axis=-1)
    data_list = [data]
    if add_time_in_day:
        time_ind = (df.index.values - df.index.values.astype("datetime64[D]")) / np.timedelta64(1, "D")
        time_in_day = np.tile(time_ind, [1, num_nodes, 1]).transpose((2, 1, 0))
        data_list.append(time_in_day)
    if add_day_in_week:
        day_in_week = np.zeros(shape=(num_samples, num_nodes, 7))
        day_in_week[np.arange(num_samples), :, df.index.dayofweek] = 1
        data_list.append(day_in_week)
    data = np.concatenate(data_list, axis=-1)
    x = []
    min_t = abs(min(x_offsets))
    max_t = num_samples
    for t in range(min_t, max_t):
        x_t = data[t + x_offsets, ...]
        x.append(x_t)
    x = np.stack(x, axis=0)
    return x




def main(): 
    # Tải data thực
    load_dotenv("D:\Project\hcmc_traffic_predict\.env")
    host = os.getenv("PG_HOST")
    port = 5432
    user = os.getenv("PG_USER")
    password = os.getenv("PG_PASSWORD") 
    database = os.getenv("PG_NAME")
    
    connector = PgresSQLConnector(host, port, user, password, database)
    connector.connect()

    query = "SELECT * FROM real_hcmc_traffic_data"
    df = connector.query_to_dataframe(query)
    df.rename(columns = {"index": "datetime"}, inplace = True)
    print(df.head())
    connector.close()

if __name__ == "__main__":
    main()
