import streamlit as st
import pandas as pd
from database_handler import DatabaseHandler
from csv_processor import PlaylistCSVProcessor
from data_visualizer import PlaylistVisualizer
from batch_processor import BatchProcessor
from playlist_scheduler import PlaylistUpdateScheduler
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import os
import tempfile
from datetime import datetime
import re
import time
import io


class PlaylistCuratorFinder:
    def __init__(self, client_id, client_secret):
        """Initialize with your Spotify API credentials"""
        self.sp = spotipy.Spotify(
            auth_manager=SpotifyClientCredentials(
                client_id=client_id,
                client_secret=client_secret
            )
        )
        
        # Initialize contact pattern matches
        self.patterns = {
            'email': r'[\w\.-]+@[\w\.-]+\.\w+',
            'instagram': r'(?:instagram\.com/|ig:?|instagram:?\s?)(@?\w+)',
            'discord': r'(?:discord\.gg/|discord:?\s?)(\w+)',
            'submission_form': r'https?://(?:www\.)?\w+\.\w+/\S*(?:submit|submission|contact)',
            'email_obfuscated': r'[\w\.-]+\s*(?:\[at\]|\(at\)|@)\s*[\w\.-]+\.\w+'
        }

    def extract_contacts(self, text):
        """Extract all contact information from text"""
        if not text:
            return {}
        
        contacts = {}
        for contact_type, pattern in self.patterns.items():
            matches = re.findall(pattern, text, re.IGNORECASE)
            if matches:
                contacts[contact_type] = matches[0] if len(matches) == 1 else matches
                
        return contacts

    def search_playlists(self, query, limit=50):
        """Search for playlists matching the query"""
        results = self.sp.search(q=query, type='playlist', limit=limit)
        return results['playlists']['items']

    def get_curator_info(self, playlist):
        """Extract curator information from a playlist"""
        try:
            full_playlist = self.sp.playlist(playlist['id'])
            description = full_playlist.get('description', '')
            
            contacts = self.extract_contacts(description)
            
            tracks_preview = self.sp.playlist_tracks(
                playlist['id'], 
                limit=5,
                fields='items(track(artists,id))'
            )
            
            genres = set()
            for item in tracks_preview['items']:
                if item['track'] and item['track']['artists']:
                    artist_info = self.sp.artist(item['track']['artists'][0]['id'])
                    genres.update(artist_info.get('genres', []))
            
            return {
                'playlist_id': playlist['id'],
                'playlist_name': playlist['name'],
                'curator_name': playlist['owner']['display_name'],
                'curator_id': playlist['owner']['id'],
                'playlist_url': playlist['external_urls']['spotify'],
                'follower_count': full_playlist['followers']['total'],
                'track_count': playlist['tracks']['total'],
                'email': contacts.get('email'),
                'instagram': contacts.get('instagram'),
                'discord': contacts.get('discord'),
                'submission_form': contacts.get('submission_form'),
                'email_obfuscated': contacts.get('email_obfuscated'),
                'description': description,
                'genres': list(genres)[:5]
            }
        except Exception as e:
            st.error(f"Error getting playlist info: {str(e)}")
            return None

def main():
    st.set_page_config(page_title="Spotify Playlist Curator Database", layout="wide")
    
    # Initialize components
    db = DatabaseHandler()
    csv_processor = PlaylistCSVProcessor()
    visualizer = PlaylistVisualizer()
    batch_processor = BatchProcessor(db, csv_processor)
    
    # Sidebar navigation
    page = st.sidebar.selectbox(
        "Navigation",
        ["Dashboard", "Playlist Search", "Data Import", "Batch Processing", "Scheduler", "Export"]
    )
    
    if page == "Dashboard":
        show_dashboard(db, visualizer)
    elif page == "Playlist Search":
        show_playlist_search(db)
    elif page == "Data Import":
        show_import_page(db, csv_processor)
    elif page == "Batch Processing":
        show_batch_processing(batch_processor)
    elif page == "Scheduler":
        show_scheduler_page(db)
    else:
        show_export_page(db)

def show_dashboard(db, visualizer):
    st.title("Playlist Curator Dashboard")
    
    # Get current data
    df = db.get_all_playlists()
    
    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Playlists", len(df))
    with col2:
        st.metric("Total Followers", f"{df['follower_count'].sum():,}")
    with col3:
        st.metric("Avg Followers", f"{df['follower_count'].mean():,.0f}")
    with col4:
        st.metric("With Email", f"{df['email'].notna().sum()}")
    
    # Visualizations
    st.subheader("Analytics")
    
    # Two columns for visualizations
    col1, col2 = st.columns(2)
    
    with col1:
        st.plotly_chart(visualizer.create_follower_distribution(df), use_container_width=True)
        st.plotly_chart(visualizer.create_email_presence_pie(df), use_container_width=True)
        st.plotly_chart(visualizer.create_curator_rankings(df), use_container_width=True)
    
    with col2:
        st.plotly_chart(visualizer.create_track_count_scatter(df), use_container_width=True)
        st.plotly_chart(visualizer.create_engagement_scatter(df), use_container_width=True)
        st.plotly_chart(visualizer.create_growth_heatmap(df), use_container_width=True)
    
    # Data table with filters
    st.subheader("Playlist Data")
    
    # Filters
    col1, col2, col3 = st.columns(3)
    with col1:
        min_followers = st.number_input("Minimum Followers", min_value=0, value=0)
    with col2:
        email_filter = st.checkbox("Show only playlists with email contacts")
    with col3:
        search_term = st.text_input("Search playlists/curators")
    
    # Apply filters
    filtered_df = df[df['follower_count'] >= min_followers]
    if email_filter:
        filtered_df = filtered_df[filtered_df['email'].notna()]
    if search_term:
        filtered_df = filtered_df[
            filtered_df['playlist_name'].str.contains(search_term, case=False, na=False) |
            filtered_df['curator_name'].str.contains(search_term, case=False, na=False)
        ]
    
    # Display filtered data
    st.dataframe(filtered_df)

def show_playlist_search(db):
    st.title("Playlist Curator Search")
    
    # Get API credentials
    client_id = st.secrets.get("SPOTIFY_CLIENT_ID") or os.getenv("SPOTIFY_CLIENT_ID")
    client_secret = st.secrets.get("SPOTIFY_CLIENT_SECRET") or os.getenv("SPOTIFY_CLIENT_SECRET")
    
    if not client_id or not client_secret:
        st.error("Spotify API credentials not found. Please set them in secrets or environment variables.")
        return
    
    finder = PlaylistCuratorFinder(client_id, client_secret)
    
    # Search interface
    search_term = st.text_input("Enter search term")
    
    predefined_queries = [
        'submit music', 'submissions', 'contact', 'booking',
        'demo', 'music promotion', 'playlist curator',
        'accepting submissions', 'submit tracks', 'playlist submission'
    ]
    
    selected_queries = st.multiselect(
        "Or select from common search terms",
        predefined_queries
    )
    
    if search_term or selected_queries:
        queries = [search_term] if search_term else [] + selected_queries
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        all_results = []
        for i, query in enumerate(queries):
            status_text.text(f"Searching for '{query}'...")
            progress = (i + 1) / len(queries)
            progress_bar.progress(progress)
            
            try:
                playlists = finder.search_playlists(query)
                for playlist in playlists:
                    curator_info = finder.get_curator_info(playlist)
                    if curator_info:
                        result = db.add_or_update_playlist(curator_info)
                        all_results.append({
                            'playlist': curator_info['playlist_name'],
                            'curator': curator_info['curator_name'],
                            'status': result
                        })
                        
                time.sleep(1)  # Respect rate limits
                
            except Exception as e:
                st.error(f"Error searching for '{query}': {str(e)}")
        
        # Show results
        if all_results:
            st.success("Search completed!")
            results_df = pd.DataFrame(all_results)
            st.dataframe(results_df)
            
            # Summary stats
            added = len([r for r in all_results if r['status'] == 'added'])
            updated = len([r for r in all_results if r['status'] == 'updated'])
            skipped = len([r for r in all_results if r['status'] == 'skipped'])
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("New Playlists", added)
            with col2:
                st.metric("Updated", updated)
            with col3:
                st.metric("Skipped", skipped)

def show_import_page(db, csv_processor):
    st.title("Import Data")
    
    uploaded_file = st.file_uploader("Upload CSV file", type=['csv'])
    if uploaded_file is not None:
        # Save the uploaded file temporarily
        with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as tmp_file:
            tmp_file.write(uploaded_file.getvalue())
            tmp_path = tmp_file.name
        
        # Process the file
        try:
            df = csv_processor.process_csv(tmp_path)
            
            # Preview the data
            st.subheader("Data Preview")
            st.dataframe(df.head())
            
            if st.button("Confirm Import"):
                stats = db.process_csv_import(uploaded_file.name, df)
                
                st.success(f"""
                    Import completed:
                    - Processed: {stats['processed']} records
                    - Added: {stats['added']} new playlists
                    - Updated: {stats['updated']} existing playlists
                """)
            
        except Exception as e:
            st.error(f"Error processing file: {str(e)}")
            
        finally:
            os.unlink(tmp_path)

def show_batch_processing(batch_processor):
    st.title("Batch Processing")
    
    uploaded_files = st.file_uploader(
        "Upload CSV files", 
        type=['csv'],
        accept_multiple_files=True
    )
    
    if uploaded_files:
        st.write(f"Selected {len(uploaded_files)} files")
        
        if st.button("Process Files"):
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            # Create temporary directory for processing
            with tempfile.TemporaryDirectory() as tmp_dir:
                # Save uploaded files
                for i, file in enumerate(uploaded_files):
                    progress = (i + 1) / len(uploaded_files)
                    status_text.text(f"Saving files... ({i+1}/{len(uploaded_files)})")
                    progress_bar.progress(progress)
                    
                    with open(os.path.join(tmp_dir, file.name), 'wb') as f:
                        f.write(file.getvalue())
                
                # Process the directory
                status_text.text("Processing files...")
                results = batch_processor.process_directory(tmp_dir)
                
                # Show results
                st.success(f"""
                    Batch processing completed:
                    - Processed {results['processed_files']} of {results['total_files']} files
                    - Added {results['added_records']} new playlists
                    - Updated {results['updated_records']} existing playlists
                    - Failed files: {len(results['failed_files'])}
                """)
                
                if results['failed_files']:
                    st.error("Failed files:")
                    for file_name, error in results['failed_files']:
                        st.write(f"- {file_name}: {error}")

def show_scheduler_page(db):
    st.title("Update Scheduler")
    
    # Initialize scheduler if needed
    if 'scheduler' not in st.session_state:
        st.session_state.scheduler = PlaylistUpdateScheduler(
            db,
            os.getenv("SPOTIFY_CLIENT_ID"),
            os.getenv("SPOTIFY_CLIENT_SECRET")
        )
    
    scheduler = st.session_state.scheduler
    
    # Scheduler controls
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("Start Scheduler"):
            if scheduler.start():
                st.success("Scheduler started successfully")
            else:
                st.warning("Scheduler is already running")
    
    with col2:
        if st.button("Stop Scheduler"):
            scheduler.stop()
            st.success("Scheduler stopped")
    
    # Update status
    status = scheduler.get_update_status()
    
    st.subheader("Update Status")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Total Playlists", status['total_playlists'])
    with col2:
        st.metric("Recently Updated", status['recently_updated'])
    with col3:
        st.metric("Last Check", status['last_check'].strftime('%Y-%m-%d %H:%M:%S'))

def show_export_page(db):
    st.title("Export Data")
    
    # Get current data
    df = db.get_all_playlists()
    
    # Export options
    export_format = st.selectbox(
        "Export Format",
        ["CSV", "Excel", "JSON"]
    )
    
    # Filters
    st.subheader("Export Filters")
    col1, col2 = st.columns(2)
    
    with col1:
        min_followers = st.number_input("Minimum Followers", min_value=0, value=0)
        email_only = st.checkbox("Only playlists with email contacts")
    
    with col2:
        min_tracks = st.number_input("Minimum Tracks", min_value=0, value=0)
                                     
def show_export_page(db):
    st.title("Export Data")
    
    # Get current data
    df = db.get_all_playlists()
    
    # Export options
    export_format = st.selectbox(
        "Export Format",
        ["CSV", "Excel", "JSON"]
    )
    
    # Filters
    st.subheader("Export Filters")
    col1, col2 = st.columns(2)
    
    with col1:
        min_followers = st.number_input("Minimum Followers", min_value=0, value=0)
        email_only = st.checkbox("Only playlists with email contacts")
    
    with col2:
        min_tracks = st.number_input("Minimum Tracks", min_value=0, value=0)
        active_only = st.checkbox("Only recently updated playlists")
    
    # Apply filters
    filtered_df = df[
        (df['follower_count'] >= min_followers) &
        (df['track_count'] >= min_tracks)
    ]
    
    if email_only:
        filtered_df = filtered_df[filtered_df['email'].notna()]
    
    if active_only:
        recent_date = datetime.now() - pd.Timedelta(days=30)
        filtered_df = filtered_df[pd.to_datetime(filtered_df['last_updated']) >= recent_date]
    
    # Show preview
    st.subheader(f"Preview ({len(filtered_df)} playlists)")
    st.dataframe(filtered_df.head())
    
    # Export functionality using st.download_button
    if len(filtered_df) > 0:
        try:
            filename = f"playlist_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            if export_format == "CSV":
                csv = filtered_df.to_csv(index=False)
                st.download_button(
                    label="Download CSV",
                    data=csv,
                    file_name=f"{filename}.csv",
                    mime="text/csv"
                )
            
            elif export_format == "Excel":
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    filtered_df.to_excel(writer, index=False)
                excel_data = output.getvalue()
                st.download_button(
                    label="Download Excel",
                    data=excel_data,
                    file_name=f"{filename}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
            
            else:  # JSON
                json_str = filtered_df.to_json(orient='records')
                st.download_button(
                    label="Download JSON",
                    data=json_str,
                    file_name=f"{filename}.json",
                    mime="application/json"
                )
                
        except Exception as e:
            st.error(f"Error preparing export: {str(e)}")
    else:
        st.warning("No data to export with current filters")
if __name__ == "__main__":
    main()