import yfinance as yf
import pandas as pd
from datetime import datetime
import time
from mongo_handler import insert_data_to_mongodb, delete_existing_collections
from gcp_handler import upload_data_to_gcp
from kafka_handler import send_data_to_kafka_topic

# Main loop to continuously update the database and upload to GCP
while True:
    # Delete existing collections in MongoDB
    delete_existing_collections()

    # Generate a unique name for the new collection in MongoDB
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    collection_name = f"col_{timestamp}"

    # Fetch data using yfinance
    ticker = "UBER"  # Replace with your desired ticker symbol
    data = yf.download(ticker, period="1d", interval="1m")
    df = pd.DataFrame(data)

    # Insert data into MongoDB
    insert_data_to_mongodb(df, collection_name)

    # Upload data to GCP
    upload_data_to_gcp(df)

    # Send data to Kafka topic
    send_data_to_kafka_topic()

    time.sleep(2)
