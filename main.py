import streamlit as st
import pandas as pd
from database_handler import DatabaseHandler
from csv_processor import PlaylistCSVProcessor
import os

def main():
    st.set_page_config(page_title="Spotify Playlist Curator Database", layout="wide")
    
    st.title("Spotify Playlist Curator Database")
    
    # Initialize database
    db = DatabaseHandler()
    csv_processor = PlaylistCSVProcessor()
    
    # File uploader
    uploaded_file = st.file_uploader("Upload CSV file", type=['csv'])
    if uploaded_file is not None:
        # Process the uploaded file
        df = csv_processor.process_csv(uploaded_file)
        
        # Import to database
        stats = db.process_csv_import(uploaded_file.name, df)
        
        st.success(f"""
            CSV import completed:
            - Processed: {stats['processed']} records
            - Added: {stats['added']} new playlists
            - Updated: {stats['updated']} existing playlists
        """)
    
    # Display current database contents
    st.subheader("Stored Playlists")
    
    # Get all playlists
    playlists_df = db.get_all_playlists()
    
    # Add filters
    col1, col2 = st.columns(2)
    with col1:
        min_followers = st.number_input("Minimum Followers", min_value=0, value=0)
    with col2:
        email_filter = st.checkbox("Show only playlists with email contacts")
    
    # Apply filters
    filtered_df = playlists_df[playlists_df['follower_count'] >= min_followers]
    if email_filter:
        filtered_df = filtered_df[filtered_df['email'].notna()]
    
    # Display stats
    st.metric("Total Playlists", len(filtered_df))
    
    # Display table
    st.dataframe(
        filtered_df[[
            'playlist_name', 'curator_name', 'email',
            'follower_count', 'track_count', 'playlist_url'
        ]].style.format({
            'follower_count': '{:,.0f}',
            'track_count': '{:,.0f}'
        })
    )
    
    # Export functionality
    if st.button("Export to CSV"):
        filtered_df.to_csv("exported_playlists.csv", index=False)
        st.success("Data exported to 'exported_playlists.csv'")

if __name__ == "__main__":
    main()