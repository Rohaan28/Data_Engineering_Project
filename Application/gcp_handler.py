from google.cloud import storage
from google.oauth2 import service_account
import os
from datetime import date

# GCS configuration
gcs_bucket_name = "trading_data_new"  # Replace with your GCS bucket name
# Specify the path to the service account key file
key_path = "/Users/Rohaan/Desktop/DE/Data_Engineering_Project/key.json"
# Create credentials
credentials = service_account.Credentials.from_service_account_file(key_path)
# Use credentials for authentication
gcs_client = storage.Client(credentials=credentials)

def upload_data_to_gcp(df):
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
