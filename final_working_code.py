import yfinance as yf
import pandas as pd
from pymongo import MongoClient
from google.cloud import storage
import os
import time
from kafka import KafkaProducer
import json
from datetime import datetime, date
from google.oauth2 import service_account


# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017")
db = client["DE_new"]

# GCS configuration
gcs_bucket_name = "trading_data_new"  # Replace with your GCS bucket name
# Specify the path to the service account key file
key_path = "/Users/Rohaan/Desktop/DE/Data_Engineering_Project/key.json"
# Create credentials
credentials = service_account.Credentials.from_service_account_file(key_path)
# Use credentials for authentication
gcs_client = storage.Client(credentials=credentials)


# Set GCP credentials environment variable
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/Rohaan/Desktop/DE/Data_Engineering_Project/key.json"

# Kafka configuration
kafka_bootstrap_servers = "localhost:9092"
kafka_topic = "Topic_1_Trade_Data"

# Create Kafka producer
kafka_producer = KafkaProducer(
    bootstrap_servers=kafka_bootstrap_servers,
    api_version=(0, 11, 5),
    value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
)

# Main loop to continuously update the database and upload to GCP
while True:
    # Delete existing collections in MongoDB
    existing_collections = db.list_collection_names()
    for collection_name in existing_collections:
        db[collection_name].drop()

    # Generate a unique name for the new collection in MongoDB
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    collection_name = f"col_{timestamp}"
    collection = db[collection_name]

    # Fetch data using yfinance
    ticker = "UBER"  # Replace with your desired ticker symbol
    data = yf.download(ticker, period="1d", interval="1m")
    df = pd.DataFrame(data)

    # Upload data to MongoDB
    collection.insert_many(df.to_dict(orient="records"))

    # Generate a unique name for the file in GCS
    gcp_timestamp = date.today().strftime("%Y%m%d")
    gcp_file_name = f"data_{gcp_timestamp}.csv"

    # Upload data to GCP and delete existing data for the current day
    bucket = gcs_client.bucket(gcs_bucket_name)
    blob = bucket.blob(gcp_file_name)

    if blob.exists():
        # Delete existing data for the current day
        blob.delete()

    # Upload the new data to GCP
    blob.upload_from_string(df.to_csv(index=False), content_type="text/csv")

    # Send data to Kafka topic
    kafka_producer.send(kafka_topic, value="Data received".encode('utf-8'))

    time.sleep(2)

print("Execution stopped by user")
