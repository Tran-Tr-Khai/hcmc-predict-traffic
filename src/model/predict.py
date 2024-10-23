from STGTN import STGraphTransformers as stgtn
import torch
import dgl
import pandas as pd



def main(): 
    # Tải data thực
    load_dotenv("D:\Project\hcmc_traffic_predict\.env")
    host = os.getenv("MySQL_HOST")
    port = 3306
    user = os.getenv("MySQL_USER")
    password = os.getenv("MySQL_PASSWORD") 
    database = os.getenv("MySQL_NAME")
    
    connector = PgresSQLConnector(host, port, user, password, database)
    connector.connect()

    query = "SELECT * FROM real_hcmc_traffic_data"
    df = connector.query_to_dataframe(query)
    df.rename(columns = {"index": "datetime"}, inplace = True)
    print(df.head())
    connector.close()

    #




