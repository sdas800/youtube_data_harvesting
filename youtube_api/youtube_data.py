import googleapiclient
from googleapiclient.discovery import build

api_key="AIzaSyBzZ9Eh0M9U_J-T8WbP3ph_mSPzfn1jEoM"
youtube = googleapiclient.discovery.build("youtube", "v3", developerKey=api_key)

def get_data(channel_id):
    # Get the data from the YouTube API
    try:
        channel_response = youtube.channels().list(
            id=channel_id,
            part='snippet,statistics,contentDetails'
        ).execute()
    except Exception as e:
        print(f"Error fetching channel data: {e}")
        return {}

    # Get playlist information
    playlist_response = youtube.playlists().list(
        part='snippet,contentDetails',
        channelId=channel_id,
        maxResults=50
    ).execute()

    data = {
        "Channel_Name": {
            "Channel_Name": channel_response['items'][0]['snippet']['title'],
            "Channel_Id": channel_id,
            "Subscription_Count": channel_response['items'][0]['statistics']['subscriberCount'],
            "Channel_Views": channel_response['items'][0]['statistics']['viewCount'],
            "Channel_Description": channel_response['items'][0]['snippet']['description'],
            "Playlist_Id": playlist_response['items'][0]['id']
        }
    }

    for item in playlist_response['items']:
        playlist_id = item['id']
        playlist_videos = youtube.playlistItems().list(
            playlistId=playlist_id,
            part='snippet',
            maxResults=50
        ).execute()

        # Get video and comment information
        for video in playlist_videos['items']:
            video_id = video['snippet']['resourceId']['videoId']
            video_response = youtube.videos().list(
                part='snippet,statistics,contentDetails',
                id=video_id
            ).execute()

            if video_response['items']:
                video_information = {
                    "Video_Id": video_id,
                    "Video_Name": video_response['items'][0]['snippet']['title'],
                    "Video_Description": video_response['items'][0]['snippet']['description'],
                    "Tags": video_response['items'][0]['snippet'].get('tags'),
                    "PublishedAt": video_response['items'][0]['snippet']['publishedAt'],
                    "View_Count": video_response['items'][0]['statistics']['viewCount'],
                    "Like_Count": video_response['items'][0]['statistics']['likeCount'],
                    "Favorite_Count": video_response['items'][0]['statistics']['favoriteCount'],
                    "Comment_Count": video_response['items'][0]['statistics'].get('commentCount'),
                    "Duration": video_response['items'][0]['contentDetails']['duration'],
                    "Thumbnail": video_response['items'][0]['snippet']['thumbnails']['default']['url'],
                    "Caption_Status": video_response['items'][0]['contentDetails']['caption'],
                    "Comments": {}
                }
                try:
                  comments_response = youtube.commentThreads().list(
                      part='snippet,replies',
                      videoId=video_id
                      ).execute()
                  for comment in comments_response['items']:
                      comment_information = {
                          "Comment_Id": comment['snippet']['topLevelComment']['id'],
                          "Comment_Text": comment['snippet']['topLevelComment']['snippet']['textDisplay'],
                          "Comment_Author": comment['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                          "Comment_PublishedAt": comment['snippet']['topLevelComment']['snippet']['publishedAt']
                        }
                      video_information['Comments'][comment_information['Comment_Id']] = comment_information
                except googleapiclient.errors.HttpError:
                    print(f"Comments are disabled for video: {video_id}")



    data[video_id] = video_information

    return data
