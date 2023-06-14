import yfinance as yf
import pandas as pd
from pymongo import MongoClient
import time

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017")
db = client["your_database_name"]  # Replace "your_database_name" with your desired database name
collection = db["your_collection_name"]  # Replace "your_collection_name" with your desired collection name

# Main loop to continuously update the database
stop_flag = False
while not stop_flag:
    # Fetch data using yfinance
    ticker = "UBER"  # Replace with your desired ticker symbol
    data = yf.download(ticker, period="1d", interval="1m")

    # Convert data to a DataFrame
    df = pd.DataFrame(data)

    # Convert DataFrame to JSON format
    data_json = df.to_dict(orient="records")

    # Insert data into MongoDB
    collection.insert_many(data_json)

    # Check if any key is pressed to stop the execution
    if input("Press Enter to continue, or type 'stop' and press Enter to stop: ").lower() == "stop":
        stop_flag = True
 
    # Wait for 5 seconds before fetching new data
    time.sleep(5)

print("Execution stopped by user")

