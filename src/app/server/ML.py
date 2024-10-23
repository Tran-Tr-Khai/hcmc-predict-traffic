class DataFrameIterator:
    def __init__(self, df):
        self.df = df
        self.current_index = 0
    
    def get_next_batch(self):
        if self.current_index < len(self.df):
            current_time = self.df.iloc[self.current_index]['time']
            rows = self.df[self.df['time'] == current_time]
            self.current_index += len(rows)
            return rows
        else:
            return None  # Không còn dòng nào để trả về
        
def initializeData(df):
    iterator = DataFrameIterator(df)
    return iterator