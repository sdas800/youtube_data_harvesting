import streamlit as st
from youtube_api import youtube_data
from database import mongodb, sql_db
from utils import helpers

# Set page configuration
st.set_page_config(
    page_title="YouTube Dashboard",
    page_icon=":video_camera:",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# Set the title of the app
st.title('YouTube Channel Dashboard')

# Create columns for layoute
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.header("YouTube Channel Details")                       
    st.subheader("Enter the Channel ID: ")
    channel_id = st.text_input("Channel ID", key="channel_id")
    if st.button("Fetch Channel Data"):
        try:
            channel_data = youtube_data.get_data(channel_id)
            st.write('Data fetched successfully', channel_data)
        except Exception as e:
            st.write(f"Error fetching data: {e}")

with col2:
    st.header("Store Data in MongoDB")
    st.subheader("Enter the Database Name: ")
    db_name = st.text_input("Database Name", key="db_name")
    if st.button("Store Data in MongoDB"):
        try:
            mongodb.store_data(channel_data, db_name)
            st.write('Data stored in MongoDB successfully.')
        except Exception as e:
            st.write(f"Error storing data in MongoDB: {e}")

with col3:
    st.header("Export to SQL Server")
    st.subheader("Enter the Server Name: ")
    server_name = st.text_input("Server Name", key="server_name")
    st.subheader("Enter the Database Name: ")
    db_name = st.text_input("Database Name", key="sql_db_name")
    if st.button("Export Data to SQL Server"):
        try:
            sql_db.migrate_data(db_name, server_name)
            st.write('Data migrated to SQL database successfully.')
        except Exception as e:
            st.write(f"Error migrating data to SQL database: {e}")

with col4:
    st.header("Analyze the Collected Data")
    st.subheader("Select the Analysis Type: ")
    analysis_type = st.selectbox("Analysis Type", ("Option 1", "Option 2", "Option 3"), key="analysis_type")
    if st.button("Analyze Data"):
        sql_db.search_data
        # Add your data analysis code here


# Display the results
st.header("Results")
st.write("Results will be displayed here.")
