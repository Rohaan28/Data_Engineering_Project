import yfinance as yf
import pandas as pd
from datetime import datetime, date
import time
from pymongo import MongoClient
from mongo_handler import insert_data, delete_collection
from gcp_handler import upload_to_gcs, delete_blob
from kafka_handler import send_message
from visual import update_chart

# MongoDB configuration
mongo_host = "localhost"
mongo_port = 27017
mongo_db_name = "DE_new"

# GCS configuration
gcs_bucket_name = "trading_data_new"
credentials_path = "/Users/Rohaan/Desktop/DE/Data_Engineering_Project/key.json"

# Kafka configuration
kafka_bootstrap_servers = "localhost:9092"
kafka_topic = "Topic_1_Trade_Data"

# Ticker configuration
ticker = "UBER"

# Connect to MongoDB
client = MongoClient(f"mongodb://{mongo_host}:{mongo_port}")
db = client[mongo_db_name]

# Main loop to continuously update the database and upload to GCP
while True:
    # Delete existing collections in MongoDB
    existing_collections = db.list_collection_names()
    for collection_name in existing_collections:
        delete_collection(mongo_host, mongo_port, mongo_db_name, collection_name)

    # Generate a unique name for the new collection in MongoDB
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    collection_name = f"col_{timestamp}"

    # Fetch data using yfinance
    data = yf.download(ticker, period="1d", interval="1m")
    df = pd.DataFrame(data)
    
    if df.empty:
        print("No data available")
        time.sleep(2)
        continue

    # Set the index as the date column
    #df['Date'] = df.index.time
    df['Date'] = pd.to_datetime(df.index)
    # Upload data to MongoDB
    insert_data(mongo_host, mongo_port, mongo_db_name, collection_name, df.to_dict(orient="records"))

    # Generate a unique name for the file in GCS
    gcp_timestamp = date.today().strftime("%Y%m%d")
    gcp_file_name = f"data_{gcp_timestamp}.csv"

    # Upload data to GCP and delete existing data for the current day
    upload_to_gcs(credentials_path, gcs_bucket_name, gcp_file_name, df.to_csv(index=False))

    # Send data to Kafka topic
    send_message(kafka_bootstrap_servers, kafka_topic, df.to_dict(orient="records"))

    # Update the candlestick chart
    update_chart(df)

    time.sleep(2)  # Pause for 2 seconds before updating the chart again

print("Execution stopped by user")
