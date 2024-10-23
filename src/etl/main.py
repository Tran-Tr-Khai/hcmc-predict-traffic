from dotenv import load_dotenv
import os
from extract import extract_data
from transform import DataTransformer
from load import LoadDataBatch

def main():
    load_dotenv("D:\Project\hcmc_traffic_predict\.env")
    MySQL_config = {
        "host": os.getenv("MySQL_HOST"),
        "user": os.getenv("MySQL_USER"),
        "password": os.getenv("MySQL_PASSWORD"),
        "database": os.getenv("MySQL_NAME")
    }


    host = os.getenv("PG_HOST")
    user = os.getenv("PG_USER")
    password =  os.getenv("PG_PASSWORD")
    database =  os.getenv("PG_NAME") 
    PG_uri = f'postgresql+psycopg2://{user}:{password}@{host}:5432/{database}'


    # Index
    tables = ["history_hcmc_traffic_data", "real_hcmc_traffic_data", "nodes_df"]
    
    # History
    # Extract data
    history_hcmc_traffic_data = extract_data(tables[0], MySQL_config)
    print(history_hcmc_traffic_data.head())
    # Transform data
    history_hcmc_traffic_data = DataTransformer(history_hcmc_traffic_data)
    history_hcmc_traffic_data.transform()
    print(history_hcmc_traffic_data.df.head())
    # Load data
    load_data = LoadDataBatch(history_hcmc_traffic_data.df, PG_uri)
    table_name = "history_hcmc_traffic_data"
    load_data.run(table_name)


    # Real
    # Extract data
    real_hcmc_traffic_data = extract_data(tables[1], MySQL_config)
    print(real_hcmc_traffic_data.head())
    # Transform data
    real_hcmc_traffic_data = DataTransformer(real_hcmc_traffic_data)
    real_hcmc_traffic_data.transform()
    print(real_hcmc_traffic_data.df.head())
    # Load data
    real_data = LoadDataBatch(real_hcmc_traffic_data.df, PG_uri)
    table_name = "real_hcmc_traffic_data"
    real_data.run(table_name)

if __name__ == "__main__": 
    main()