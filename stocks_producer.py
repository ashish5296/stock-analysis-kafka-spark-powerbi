import requests
from kafka import KafkaProducer
import json
from datetime import datetime

# Polygon.io API key
api_key = "XjYt_dkqf8RGZoPqstKOCrTozpC5CtsI"

# Stock symbol for Nvidia
symbol = "NVDA"

# Date range for the stock data
start_date = "2025-05-01"
end_date = "2025-05-25"

# API URL to fetch the stock data from Polygon.io
url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/{start_date}/{end_date}?apiKey={api_key}"

# Make a request to Polygon.io API
response = requests.get(url)
data = response.json()

# Check if the response contains results
if 'results' in data:
    # Initialize Kafka producer
    producer = KafkaProducer(bootstrap_servers='localhost:9092',
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    # Iterate through the data for each day
    for item in data['results']:
        # Get stock data: open, close, high, low values
        timestamp = item['t']  # Timestamp (in milliseconds)
        open_price = item['o']  # Open price
        close_price = item['c']  # Close price
        high_price = item['h']  # High price
        low_price = item['l']  # Low price
        
        # Convert timestamp to a readable date format
        date_str = datetime.fromtimestamp(timestamp / 1000).strftime('%Y-%m-%d')

        # Classify the stock movement based on open and close prices
        if close_price > open_price:
            movement_class = 1  # Up
        else:
            movement_class = 0  # Down
         

        # Prepare the message for Kafka
        message = {
            "date": date_str,
            "symbol": symbol,
            "open": open_price,
            "close": close_price,
            "high": high_price,
            "low": low_price,
            "movement_class": movement_class  # Adding classifier: 1 for up, 0 for down
        }

        # Send the message to Kafka
        producer.send('my-stock-data', value=message)
        print(f"Sent to Kafka: {message}")
else:
    print("No data found or error with the API request")

producer.flush()
producer.close()
