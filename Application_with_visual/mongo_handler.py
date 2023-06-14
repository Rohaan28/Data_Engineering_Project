from pymongo import MongoClient

def insert_data(host, port, database, collection_name, data):
    client = MongoClient(host, port)
    db = client[database]
    collection = db[collection_name]
    collection.insert_many(data)

def delete_collection(host, port, database, collection_name):
    client = MongoClient(host, port)
    db = client[database]
    collection = db[collection_name]
    collection.drop()
