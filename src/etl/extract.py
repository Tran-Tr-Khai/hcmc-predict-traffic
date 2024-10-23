import mysql.connector
import pandas as pd 

def extract_data(table_name: str, db_config: dict) -> pd.DataFrame:
    query = f"SELECT * FROM {table_name}"

    try: 
        connection = mysql.connector.connect(**db_config)
        df = pd.read_sql(query, connection)
        return df
    except mysql.connector.Error as err:
        print(f"Error extracting{table_name}: {err}")
        return pd.DataFrame()   
    finally:
        if connection.is_connected(): 
            connection.close()