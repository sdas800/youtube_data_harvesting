import os
import logging
import streamlit as st
import pandas as pd
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from pymongo import MongoClient
import psycopg2
from sqlalchemy import create_engine, types

# Set up logging
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)

"""
Retrieves the YouTube API key from the environment.

The YouTube API key is required to authenticate requests to the YouTube Data API.
This key is stored as an environment variable and retrieved using the `os.getenv()` function.
"""
api_key = os.getenv("YOUTUBE_API")
youtube = build("youtube", "v3", developerKey=api_key)


# Fetch data from YouTube
def get_playlist_items(playlist_id):
    next_page_token = None
    while True:
        playlist_response = youtube.playlistItems().list(
            part="snippet,contentDetails",
            playlistId=playlist_id,
            maxResults=50,
            pageToken=next_page_token
        ).execute()
        playlist_items = playlist_response.get("items", [])
        next_page_token = playlist_response.get("nextPageToken")
        if not next_page_token:
            break
    return playlist_items


def get_video_info(video_id: str):
    """
    Fetches detailed information about a YouTube video using the YouTube Data API.
    
    Args:
        video_id (str): The ID of the YouTube video to fetch information for.
    
    Returns:
        dict: A dictionary containing various information about the video, including its title, description, tags, publish date, view count, like count, favorite count, comment count, duration, thumbnail URL, and caption status. The dictionary also includes the comments for the video, obtained by calling the `get_comments` function.
    """
    video_response = youtube.videos().list(
        part="snippet,statistics,contentDetails",
        id=video_id
    ).execute()

    video_items = video_response.get("items", [])
    video_information = None
    if video_items:
        video_item = video_items[0]
        video_information = {
            "Video_Id": video_id,
            "Video_Name": video_item["snippet"]["title"],
            "Video_Description": video_item["snippet"].get("description"),
            "Tags": video_item["snippet"].get("tags"),
            "PublishedAt": video_item["snippet"]["publishedAt"],
            "View_Count": video_item["statistics"].get("viewCount"),
            "Like_Count": video_item["statistics"].get("likeCount"),
            "Favorite_Count": video_item["statistics"].get("favoriteCount"),
            "Comment_Count": video_item["statistics"].get("commentCount"),
            "Duration": video_item["contentDetails"].get("duration"),
            "Thumbnail": video_item["snippet"]["thumbnails"]["default"]["url"],
            "Caption_Status": video_item["contentDetails"].get("caption"),
            "Comments": get_comments(video_id)
        }
    return video_information


def get_comments(video_id):
    comments = {}
    try:
        comments_response = youtube.commentThreads().list(
            part="snippet,replies",
            videoId=video_id,
            maxResults=20,
        ).execute()
        for comment in comments_response.get("items", []):
            comment_info = {
                "Comment_Id": comment["snippet"]["topLevelComment"]["id"],
                "Comment_Text": comment["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                "Comment_Author": comment["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                "Comment_PublishedAt": comment["snippet"]["topLevelComment"]["snippet"]["publishedAt"]
            }
            comment_id = comment["snippet"]["topLevelComment"]["id"]
            comments[comment_id] = comment_info
    except HttpError as e:
        print(f"Error retrieving comments for video {video_id}: {e}")
    return comments


@st.cache(allow_output_mutation=True)
def get_channel_data(channel_id):
    if not channel_id:
        raise ValueError("Channel ID is required")

    channel_data = {
        "About_Channel": {},
        "Playlists": {},
        "Videos": {}
    }

    try:
        # Fetch channel details
        channel_response = youtube.channels().list(
            id=channel_id,
            part="snippet,statistics"
        ).execute()
        channel_items = channel_response.get("items", [])
        if channel_items:
            channel_data["About_Channel"]["Channel_Name"] = channel_items[0]["snippet"]["title"]
            channel_data["About_Channel"]["Channel_Id"] = channel_id
            channel_data["About_Channel"]["Subscription_Count"] = channel_items[0]["statistics"].get("subscriberCount")
            channel_data["About_Channel"]["Channel_Views"] = channel_items[0]["statistics"].get("viewCount")
    except HttpError as E:
        print(f"Error fetching channel data: {E}")

    try:
        # Fetch playlists
        playlists_response = youtube.playlists().list(
            part="snippet",
            channelId=channel_id,
            maxResults=50
        ).execute()
        playlists = playlists_response.get("items", [])
        for playlist in playlists:
            playlist_id = playlist["id"]
            playlist_title = playlist["snippet"]["title"]
            playlist_data = {
                "Playlist_ID": playlist_id,
                "Playlist_Title": playlist_title,
                "Videos": {}
            }
            playlist_items = get_playlist_items(playlist_id)
            for item in playlist_items:
                video_id = item["snippet"]["resourceId"]["videoId"]
                video_title = item["snippet"]["title"]
                video_data = get_video_info(video_id)
                playlist_data["Videos"][video_title] = video_data
            channel_data["Playlists"][playlist_title] = playlist_data
    except HttpError as E:
        print(f"Error fetching playlist data: {E}")

    try:
        # Fetch videos not in any playlist
        all_videos_response = youtube.search().list(
            channelId=channel_id,
            part="snippet",
            maxResults=50,
            type="video"
        ).execute()
        for video in all_videos_response.get("items", []):
            video_id = video["id"]["videoId"]
            video_title = video["snippet"]["title"]
            if video_id not in channel_data["Videos"]:
                video_data = get_video_info(video_id)
                channel_data["Videos"][video_title] = video_data
    except HttpError as E:
        print(f"Error fetching videos data: {E}")

    return channel_data


# Connect to MongoDB
def connect_to_mongodb():
    try:
        MongoClient('mongodb://localhost:27017/')
        return client
    except Exception as E:
        logger.error(f"Error connecting to MongoDB: {E}")
        return None


# Connect to PostgreSQL
def connect_to_postgres():
    try:
        conn = psycopg2.connect(host='localhost', dbname='youtube_data', user='postgres', password='8001', port=5432)
        return conn
    except Exception as E:
        logger.error(f"Error connecting to PostgreSQL: {E}")
        return None


# Fetch data from MongoDB
def get_mongodb_data(selected_db_name, selected_collection_name):
    client = connect_to_mongodb()
    if client:
        try:
            db = client[selected_db_name]
            collection = db[selected_collection_name]
            data = list(collection.find())
            return data
        except Exception as E:
            logger.error(f"Error fetching data from MongoDB: {E}")


# Store data in MongoDB
def store_data_mongodb(channel_data, db_name, channel_id):
    client = connect_to_mongodb()
    if client is None:
        return False, "Failed to connect to MongoDB"

    db = client[db_name]
    if not channel_data or not isinstance(channel_data, dict):
        return False, "Invalid channel data"

    collection_name = channel_data.get('About_Channel', {}).get('Channel_Name')
    if not collection_name:
        return False, "Invalid collection name"

    try:
        collection = db[collection_name]
        existing_document = collection.find_one({"_id": channel_id})
        if existing_document:
            collection.update_one({"_id": channel_id}, {"$set": {"channel_data": channel_data}})
            return True, "Data updated successfully"
        else:
            document = {"_id": channel_id, "channel_data": channel_data}
            collection.insert_one(document)
            return True, "Data inserted successfully"
    except Exception as E:
        logger.error(f"Error storing data in MongoDB: {E}")
        return False, "Error storing data in MongoDB"


def migrate_data(selected_db_name, selected_collection_name):
    try:
        # Fetch data from MongoDB
        client = connect_to_mongodb()
        if client is None:
            return

        collection = client[selected_db_name][selected_collection_name]
        channel_data = collection.find_one()

        if not channel_data:
            st.error("No channel data found in the provided collection")
            return

        # Connect to PostgreSQL
        conn = connect_to_postgres()
        if conn is None:
            return

        about_channel = channel_data.get("About_Channel", {})

        # Check if channel data already exists
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM channel_data WHERE channel_name = %s",
                           (about_channel.get("Channel_Name"),))
            result = cursor.fetchone()
            if result[0] > 0:
                st.write("Channel data already exists in PostgreSQL. Migrating updated channel data")
                cursor.execute("DELETE FROM channel_data WHERE channel_name = %s", (about_channel.get("Channel_Name"),))

        # Insert channel data into PostgreSQL
        with conn.cursor() as cursor:
            cursor.execute(
                "INSERT INTO channel_data (channel_name, channel_id, subscription_count, channel_views) VALUES (%s, %s, %s, %s)",
                (
                    about_channel.get("Channel_Name"),
                    about_channel.get("Channel_Id"),
                    about_channel.get("Subscription_Count"),
                    about_channel.get("Channel_Views")
                )
            )

        # Insert playlist and video data into PostgreSQL
        for playlist_title, playlist_data in channel_data.get("Playlists", {}).items():
            playlist_id = playlist_data["Playlist_ID"]  # Get the playlist ID
            for video_title, video_data in playlist_data.get("Videos", {}).items():
                video_id = video_data["Video_Id"]
                video_name = video_data["Video_Name"]
                video_description = video_data["Video_Description"]
                published_date = video_data["Published_date"]
                view_count = int(video_data["View_Count"] or 0)
                like_count = int(video_data["Like_Count"] or 0)
                dislike_count = int(video_data["Dislike_Count"] or 0)
                favorite_count = int(video_data["Favorite_Count"] or 0)
                comment_count = int(video_data["Comment_Count"] or 0)
                duration = video_data["Duration"]
                thumbnail = video_data["Thumbnail"]
                caption_status = video_data["Caption_Status"]

                # Insert playlist data into PostgreSQL
                cursor.execute(
                    "INSERT INTO playlist (playlist_id, playlist_title, video_title, video_id, video_name, video_description, published_date, view_count, like_count, dislike_count, favorite_count, comment_count, duration, thumbnail, caption_status) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (
                        playlist_id,  # Include playlist ID
                        playlist_title,
                        video_title,
                        video_id,
                        video_name,
                        video_description,
                        published_date,
                        view_count,
                        like_count,
                        dislike_count,
                        favorite_count,
                        comment_count,
                        duration,
                        thumbnail,
                        caption_status
                    )
                )

                # Insert comments data into PostgreSQL
                comments = video_data.get("Comments", {})
                for comment_id, comment in comments.items():
                    comment_text = comment["Comment_Text"]
                    comment_author = comment["Comment_Author"]
                    comment_published_date = comment["Comment_PublishedAt"]

                    cursor.execute(
                        "INSERT INTO comments (video_id, comment_id, comment_text, comment_author, comment_published_date) VALUES (%s, %s, %s, %s, %s)",
                        (
                            video_id,
                            comment_id,
                            comment_text,
                            comment_author,
                            comment_published_date
                        )
                    )

        # Commit all changes
        conn.commit()
        st.write("Data migrated to PostgreSQL")

    except Exception as e:
        st.error(f"Error migrating data to PostgreSQL: {e}")


def search_data(search_query):
    result = []
    # Connect to SQL Server 
    conn = connect_to_postgres()
    if conn is None:
        return result
    # Execute search query
    try:
        with conn.cursor() as cursor:
            cursor.execute(search_query)
            result = cursor.fetchall()
    except Exception as e:
        print(f"Error executing query: {e}")
    finally:
        conn.close()
    return result

    # ====================== Streamlit Application ===================== #


# Configure the page
st.set_page_config(
    page_title="YouTube Dashboard",
    page_icon="random",
    layout="wide",
)

# Set the title of the app
st.title('YouTube Channel Dashboard')

# Create columns for layout
col1, col2, col3, col4 = st.columns(4)

# Initialize channel_data in session_state
if 'channel_data' not in st.session_state:
    st.session_state['channel_data'] = None

# Column 1: YouTube Channel Details
with col1:
    st.header("YouTube Channel Info")
    st.subheader("Enter the Channel ID:")

    # Use the stored 'channel_ids' as the options for the text input
    channel_id = st.text_input("Channel ID", key="channel_id")

    if st.button("Fetch Channel Data"):
        try:
            st.session_state['channel_data'] = get_channel_data(channel_id)
            if st.session_state['channel_data'] is not None:
                st.success('Data fetched successfully')
                st.write(st.session_state['channel_data'])
        except Exception as e:
            st.error(f"Error fetching data: {e}")

# Column 2: Store Data in MongoDB
with col2:
    st.header("Store Data in MongoDB")
    st.subheader("Enter the Database Name:")
    db_name = st.text_input("Database Name", key="db_name")

    if st.button("Store Data in MongoDB"):
        try:
            success, message = store_data_mongodb(st.session_state['channel_data'], db_name, channel_id)
            if success:
                st.success('Data stored in MongoDB successfully.')
            else:
                st.error(message)
        except Exception as e:
            st.error(f"Error storing data in MongoDB: {e}")

# Column 3: Export to SQL Server
with col3:
    st.header("Export to SQL Server")
    st.subheader("Select the Database:")
    # Get MongoDB databases
    client = connect_to_mongodb()
    db_names = client.list_database_names()
    selected_db_name = st.selectbox('Select MongoDB database', db_names)

    # Get collections in the selected database
    db = client[selected_db_name]
    selected_collection_name = st.selectbox('Select MongoDB collection', (db.list_collection_names()))

    # Fetch and display data from the selected collection
    data = get_mongodb_data(selected_db_name, selected_collection_name)

    # Export data to PostgreSQL
    if st.button("Export Data to SQL Server"):
        try:
            migrate_data(selected_db_name, selected_collection_name)
            st.success('Data migrated to PostgreSQL database successfully.')
        except Exception as e:
            st.error(f"Error migrating data to PostgreSQL database: {e}")

# Column 4: Analyze the Collected Data
with col4:
    st.header("Analyze the Collected Data")
    st.subheader("Select the Analysis Type:")
    analysis_type = st.selectbox(
        "Analysis Type",
        ("Videos and Corresponding Channels",
         "Channels with Most Videos",
         "Top 10 Most Viewed Videos",
         "Number of Comments per Video",
         "Videos with Most Likes",
         "Total Likes and Dislikes per Video",
         "Total Views per Channel",
         "Channels with Videos Published in 2023",
         "Average Duration of Videos per Channel",
         "Videos with Most Comments")
    )

    if st.button("Analyze Data"):
        try:
            # Define SQL queries for each analysis type
            if analysis_type == "Videos and Corresponding Channels":
                search_query = "SELECT video_name, channel_name FROM playlist JOIN channel_data ON playlist.channel_id = channel_data.channel_id;"
            elif analysis_type == "Channels with Most Videos":
                search_query = "SELECT channel_name, COUNT(*) AS num_videos FROM playlist JOIN channel_data ON playlist.channel_id = channel_data.channel_id GROUP BY channel_name ORDER BY num_videos DESC LIMIT 10;"
            elif analysis_type == "Top 10 Most Viewed Videos":
                search_query = "SELECT video_name, view_count FROM playlist ORDER BY view_count DESC LIMIT 10;"
            elif analysis_type == "Number of Comments per Video":
                search_query = "SELECT video_name, COUNT(*) AS num_comments FROM comments GROUP BY video_id ORDER BY num_comments DESC;"
            elif analysis_type == "Videos with Most Likes":
                search_query = "SELECT Video_Name, Like_Count FROM playlist ORDER BY Like_Count DESC LIMIT 10;"
            elif analysis_type == "Total Likes and Dislikes per Video":
                search_query = "SELECT Video_Name, Like_Count, Dislike_Count FROM playlist;"
            elif analysis_type == "Total Views per Channel":
                search_query = "SELECT Channel_Name, SUM(View_Count) AS Total_Views FROM playlist JOIN channel_data ON playlist.channel_id = channel_data.channel_id GROUP BY channel_name;"
            elif analysis_type == "Channels with Videos Published in 2023":
                search_query = "SELECT channel_name FROM playlist JOIN channel_data ON playlist.channel_id = channel_data.channel_id WHERE EXTRACT(YEAR FROM published_date) = 2023 GROUP BY channel_name;"
            elif analysis_type == "Average Duration of Videos per Channel":
                search_query = "SELECT channel_name, AVG(duration) AS avg_duration FROM playlist JOIN channel_data ON playlist.channel_id = channel_data.channel_id GROUP BY channel_name;"
            elif analysis_type == "Videos with Most Comments":
                search_query = "SELECT video_name, COUNT(*) AS num_comments FROM comments GROUP BY video_id ORDER BY num_comments DESC LIMIT 10;"
            else:
                st.warning("Please select an analysis type.")
            # Execute the search query
            analysis_result = search_data(search_query)
            # Display the analysis result
            st.write(analysis_result)
        except Exception as e:
            st.error(f"Error analyzing data: {e}")
