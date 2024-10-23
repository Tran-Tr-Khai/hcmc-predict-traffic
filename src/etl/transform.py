import pandas as pd
import numpy as np


class DataTransformer:
    def __init__(self, df: pd.DataFrame):
        self.df = df

    def clean_data(self) -> pd.DataFrame:
        self.df = self.df.drop(['Filename'], axis=1)

        # Thay đổi Dtype
        self.df['Sensor'] = pd.to_numeric(self.df['Sensor'], errors='coerce').astype('int64')
        self.df['Count'] = pd.to_numeric(self.df['Count'], errors='coerce').astype('int64')
        self.df['Date'] = pd.to_datetime(self.df['Date'], errors='coerce', format="%Y-%m-%d")

        # Xóa các dòng không đúng format
        self.df = self.df.dropna(subset=['Date'])

        return self.df

    def preprocess_datetime(self):
        self.df['datetime'] = pd.to_datetime(self.df['Date'].astype(str) + ' ' + self.df['Timestamp'], errors='coerce')
        self.df.drop(['Date', 'Timestamp'], axis=1, inplace=True)
        self.df.dropna(subset=['datetime'], inplace=True)  # Xóa nếu datetime không hợp lệ
        self.df.sort_values(by=['Sensor', 'datetime'], inplace=True)
        return self.df

    def convert_df_to_pivot_df(self):
        self.df = self.df.pivot_table(index='datetime', columns='Sensor', values='Count')
        groups = []
        for _, date in self.df.groupby(self.df.index.date):
            date = date.interpolate(method='time', limit_direction='both')
            date = date.ffill().bfill() 
            groups.append(date)

        self.df = pd.concat(groups).sort_index()
        self.df = self.df.resample('5T').asfreq().round()
        self.df = self.df.between_time('07:30', '22:30')
        self.df = self.df.interpolate(method='time', limit_direction='both').round()
        self.df = DataTransformer.ensure_full_time_range(self.df)
        self.df['date_only'] = self.df.index.date
        self.df['date_of_week'] = self.df.index.dayofweek
        

    def execute_fill_missing_data(self, daily_times):
        groups = [self.fill_missing_data(group, daily_times) for _, group in self.df.groupby(self.df.index.dayofweek)]
        self.df = pd.concat(groups).sort_index()
        self.df = self.df.drop(['date_only', 'date_of_week'], axis=1)
        return self.df
    


    def transform(self) -> pd.DataFrame:
        self.clean_data()
        self.preprocess_datetime()
        daily_times = DataTransformer.day_of_week(self.df)
        #print(daily_times)
        self.convert_df_to_pivot_df()
        self.execute_fill_missing_data(daily_times)
        return self.df  
    
    
    # Tìm ngày có đủ thời gian gần ngày hiện tại bị thiếu thời gian
    @staticmethod
    def find_nearest_full_day(daily_times, day_of_week, min_or_max, time_compare):
        """Tìm ngày gần nhất có dữ liệu đầy đủ ở khoảng min hoặc max."""
        candidates = daily_times[daily_times['day_of_week'] == day_of_week]

        if min_or_max == 'start': #Bắt đầu
            candidates = candidates[candidates['min'].dt.time <= time_compare] 
        else: #Kết thúc 
            candidates = candidates[candidates['max'].dt.time >= time_compare]

        if not candidates.empty: #Ngày gần nhất
            return candidates.index[0]  
        return None

    # Hàm điền thiếu dữ liệu ở đầu ngày
    @staticmethod
    def fill_start_of_day(missing_range, full_data, min_time):
        """Điền các khoảng thiếu ở đầu ngày bằng ngày đầy đủ tương ứng."""
        for missing_time in missing_range.index[missing_range.index.time <= min_time.time()]:
            full_time = full_data.index[full_data.index.time == missing_time.time()]
            if not full_time.empty:
                missing_range.loc[missing_time] = full_data.loc[full_time[0]]
        return missing_range

    # Hàm điền thiếu dữ liệu ở cuối ngày
    @staticmethod
    def fill_end_of_day(missing_range, full_data, max_time):
        """Điền các khoảng thiếu ở cuối ngày bằng ngày đầy đủ tương ứng."""
        for missing_time in missing_range.index[missing_range.index.time >= max_time.time()]:
            full_time = full_data.index[full_data.index.time == missing_time.time()]
            if not full_time.empty:
                missing_range.loc[missing_time] = full_data.loc[full_time[0]]
        return missing_range

    @staticmethod
    def fill_missing_data(group, daily_times):
        for date, row in daily_times.iterrows():
            if date in group.index.date:
                min_time = row['min']
                max_time = row['max']

                # Lọc khoảng thời gian thiếu của ngày hiện tại
                missing_range = group[(group.index.date == date) & ((group.index < min_time) | (group.index > max_time))]
                print(f"Đang xử lý ngày: {date}, min_time: {min_time}, max_time: {max_time}")
                
                # Kiểm tra khoảng đầu ngày bị thiếu
                start_missing_range = group[(group.index.date == date) & (group.index < min_time)]
                end_missing_range = group[(group.index.date == date) & (group.index > max_time)]

                # Điền dữ liệu cho đầu ngày
                nearest_start_day = DataTransformer.find_nearest_full_day(daily_times, row['day_of_week'], 'start', pd.to_datetime('07:30').time())
                if nearest_start_day:
                    full_data_start = group[group.index.date == nearest_start_day]
                    filled_start = DataTransformer.fill_start_of_day(start_missing_range, full_data_start, min_time)
                    group.update(filled_start)  # Cập nhật group

                # Cập nhật lại missing_range sau khi đã điền đầu ngày
                missing_range = group[(group.index.date == date) & ((group.index < min_time) | (group.index > max_time))]

                # Điền dữ liệu cho cuối ngày
                nearest_end_day = DataTransformer.find_nearest_full_day(daily_times, row['day_of_week'], 'end', pd.to_datetime('22:30').time())
                if nearest_end_day:
                    full_data_end = group[group.index.date == nearest_end_day]
                    filled_end = DataTransformer.fill_end_of_day(end_missing_range, full_data_end, max_time)
                    group.update(filled_end)  # Cập nhật group

        return group

    @staticmethod
    def day_of_week(df: pd.DataFrame) -> pd.DataFrame:
        df['date_only'] = df['datetime'].dt.date
        daily_times = df.groupby('date_only')['datetime'].agg(['min', 'max']).reset_index()
        daily_times['day_of_week'] = pd.to_datetime(daily_times['date_only']).dt.dayofweek
        daily_times.set_index('date_only', inplace=True)
        return daily_times
    

    @staticmethod
    def ensure_full_time_range(group, start='07:30', end='22:30', freq='5T'):
        """Tạo đủ khung giờ từ 07:30 đến 22:30 cho mỗi ngày."""
        all_dates = group.index.normalize().unique()  # Lấy tất cả các ngày
        full_index = pd.date_range(start=start, end=end, freq=freq)  # Khung giờ đầy đủ

        # Tạo DataFrame với tất cả các ngày và khung giờ đầy đủ
        expanded_index = pd.MultiIndex.from_product([all_dates, full_index.time], names=['date', 'time'])
        expanded_index = pd.to_datetime([f"{date} {time}" for date, time in expanded_index])  # Kết hợp ngày và giờ thành datetime index

        # Gộp index mở rộng với group ban đầu
        group = group.reindex(expanded_index, fill_value=np.nan)
        return group
