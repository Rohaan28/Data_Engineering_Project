from google.cloud import storage

def upload_to_gcs(credentials_path, bucket_name, file_name, content):
    client = storage.Client.from_service_account_json(credentials_path)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    blob.upload_from_string(content, content_type="text/csv")

def delete_blob(credentials_path, bucket_name, file_name):
    client = storage.Client.from_service_account_json(credentials_path)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    if blob.exists():
        blob.delete()
