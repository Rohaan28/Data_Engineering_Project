import yfinance as yf
import pandas as pd
from pymongo import MongoClient
import time
from kafka import KafkaProducer
import json

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017")
db = client["DE_new"]  # Replace "your_database_name" with your desired database name
collection = db["col_1"]  # Replace "your_collection_name" with your desired collection name

# Kafka configuration
kafka_bootstrap_servers = "localhost:9092"
kafka_topic = "Topic_1_Trade_Data"  # Replace "your_topic_name" with your desired Kafka topic name

# Create Kafka producer
kafka_producer = KafkaProducer(bootstrap_servers=kafka_bootstrap_servers,
                                    api_version=(0,11,5),
                                    value_serializer=lambda v: json.dumps(v).encode('utf-8'))
# Main loop to continuously update the database
while True:
    # Fetch data using yfinance
    ticker = "UBER"  # Replace with your desired ticker symbol
    data = yf.download(ticker, period="1d", interval="1m")

    # Convert data to a DataFrame
    df = pd.DataFrame(data)

    # Convert DataFrame to JSON format
    data_json = df.to_dict(orient="records")

    # Send data to Kafka topic
    kafka_producer.send(kafka_topic, value=data_json)

    # Insert data into MongoDB
    collection.insert_many(data_json)
    
    # Wait for 5 seconds before fetching new data
    time.sleep(2)

print("Execution stopped by user")
