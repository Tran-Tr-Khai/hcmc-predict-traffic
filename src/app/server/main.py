from flask import Flask, request, jsonify, render_template
import jsonData
import ML
from datetime import datetime
#from tensorflow.keras.models import load_model

app = Flask(__name__)

df = jsonData.fetch_data_from_db(host = 'localhost', port = 3306, user = "root",
                                                       password = "Nokhai14442002", database = "hcmc_traffic_db")
camera_df = df["nodes_df"]
traffic_df = df["traffic_data_streaming"]
final_df = df["traffic_data_final"]

traffic_df_iterator = ML.initializeData(final_df)

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/api/data', methods=['GET'])
def get_data():
    json_result = camera_df.to_json(orient='records')
    return jsonify(json_result)

@app.route('/api/get-count', methods=['GET'])   
def get_count_data():
    date = request.args.get('date')
    time = request.args.get('time')

    if not date or not time:
        return jsonify({'message': 'Missing date or time parameter'}), 400

    # Convert date and time to a datetime object for comparison
    datetime_str = f"{date} {time}"
    request_datetime = datetime.strptime(datetime_str, "%Y-%m-%d %H:%M")

    # Filter the dataframe based on the given datetime
    filtered_rows = final_df[final_df['datetime'] == request_datetime]

    if not filtered_rows.empty:
        return jsonify(filtered_rows.to_json(orient='records'))
    else:
        return jsonify({'message': 'No data available for the given datetime'}), 404
    
if __name__ == '__main__':
    app.run(debug=True)
