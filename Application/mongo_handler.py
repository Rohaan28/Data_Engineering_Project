from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017")
db = client["DE_new"]

def delete_existing_collections():
    # Delete existing collections in MongoDB
    existing_collections = db.list_collection_names()
    for collection_name in existing_collections:
        db[collection_name].drop()

def insert_data_to_mongodb(df, collection_name):
    # Insert data into MongoDB
    collection = db[collection_name]
    collection.insert_many(df.to_dict(orient="records"))
