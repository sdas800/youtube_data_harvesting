import psycopg2
import pandas as pd
from database import mongodb

def connect_to_postgres(server_name, db_name, user, password):
    # Connect to PostgreSQL
    try:
        conn = psycopg2.connect(host=server_name, dbname=db_name, user=user, password=password)
        return conn
    except Exception as e:
        print(f"Error connecting to PostgreSQL: {e}")
        return None

def migrate_data(db_name, collection_name, server_name, postgres_table_name, user, password):
    # Fetch data from MongoDB
    data = mongodb.connect_to_mongodb(db_name, collection_name)

    # Connect to PostgreSQL
    conn = connect_to_postgres(server_name, db_name, user, password)
    if conn is None:
        return
    cur = conn.cursor()

    # Iterate over each document in data
    for doc in data:
        # Extract the fields from the document
        channel_name = doc['Channel_Name']['Channel_Name']
        channel_id = doc['Channel_Name']['Channel_Id']
        subscription_count = doc['Channel_Name']['Subscription_Count']
        channel_views = doc['Channel_Name']['Channel_Views']
        channel_description = doc['Channel_Name']['Channel_Description']
        playlist_id = doc['Channel_Name']['Playlist_Id']

        # Insert the data into the PostgreSQL table
        try:
            cur.execute(
                "INSERT INTO {} VALUES (%s, %s, %s, %s, %s, %s)".format(postgres_table_name),
                (channel_name, channel_id, subscription_count, channel_views, channel_description, playlist_id)
            )
        except Exception as e:
            print(f"Error executing query: {e}")

    # Commit the changes and close the connection
    conn.commit()
    cur.close()
    conn.close()

def search_data(search_query, server_name, db_name, user, password):
    # Connect to SQL Server
    conn = connect_to_postgres(server_name, db_name, user, password)
    if conn is None:
        return []
    # Execute search query
    try:
        df = pd.read_sql(search_query, conn)
        return df.to_dict('records')
    except Exception as e:
        print(f"Error executing query: {e}")
        return []
