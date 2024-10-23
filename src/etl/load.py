from sqlalchemy import create_engine, text

class LoadDataBatch:
    def __init__(self, data_batch, db_uri):
        self.data_batch = data_batch
        self.db_uri = db_uri

    def load_data(self, df_chunk, table_name, if_exists_option):
        engine = create_engine(self.db_uri)
        # Tải dữ liệu vào PostgreSQL và tự động tạo bảng nếu chưa tồn tại
        df_chunk.to_sql(table_name, engine, if_exists=if_exists_option, index=True)
        print(f"Data loaded into {table_name}")

    def clear_table(self, table_name):
        engine = create_engine(self.db_uri)
        with engine.connect() as connection:
            # Kiểm tra xem bảng có tồn tại không
            result = connection.execute(
                text(f"SELECT to_regclass('{table_name}')")
            ).scalar()
            if result:
                connection.execute(text(f"DELETE FROM {table_name}"))
                print(f"Cleared data in table {table_name}")
            else:
                print(f"Table {table_name} does not exist. Skipping deletion.")

    def batching_data(self, data_batch, table_name):
        self.clear_table(table_name)
        self.load_data(data_batch, table_name, if_exists_option='replace')

    def run(self, table_name):
        # Thực hiện quá trình batching và tải dữ liệu
        self.batching_data(self.data_batch, table_name)
        print("Quá trình batching_data đã thành công")