from pymongo import MongoClient

def connect_to_mongodb():
    # Connect to MongoDB
    try:
        client = MongoClient('mongodb://localhost:27017/')
        return client
    except Exception as e:
        print(f"Error connecting to MongoDB: {e}")
        return None

def store_data(channel_data, db_name):
    # Connect to MongoDB
    client = connect_to_mongodb()
    if client is None:
        return
    db = client[db_name]
    # Use the channel name as the collection name
    collection_name = channel_data['Channel_Name']['Channel_Name']
    collection = db[collection_name]
    # Store data
    try:
        collection.insert_one(channel_data)
    except Exception as e:
        print(f"Error inserting data into MongoDB: {e}")
