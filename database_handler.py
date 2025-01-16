import sqlite3
from datetime import datetime
import pandas as pd

class DatabaseHandler:
    def __init__(self, db_name='playlists.db'):
        """Initialize database connection and create tables if they don't exist"""
        self.db_name = db_name
        self.conn = sqlite3.connect(db_name)
        self.create_tables()
    
    def create_tables(self):
        """Create necessary tables if they don't exist"""
        cursor = self.conn.cursor()
        
        # Create playlists table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS playlists (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                playlist_id TEXT UNIQUE,
                playlist_name TEXT,
                curator_name TEXT,
                curator_id TEXT,
                playlist_url TEXT,
                follower_count INTEGER,
                track_count INTEGER,
                email TEXT,
                description TEXT,
                last_updated TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Create a table for tracking CSV imports
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS import_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                filename TEXT,
                import_date TIMESTAMP,
                records_processed INTEGER,
                records_added INTEGER,
                records_updated INTEGER
            )
        ''')
        
        self.conn.commit()
    
    def add_or_update_playlist(self, playlist_data):
        """Add or update a playlist in the database"""
        cursor = self.conn.cursor()
        
        # Extract the Spotify playlist ID from the URL
        playlist_id = playlist_data['playlist_url'].split('/')[-1]
        
        # Check if playlist exists
        cursor.execute('SELECT id FROM playlists WHERE playlist_id = ?', (playlist_id,))
        existing_playlist = cursor.fetchone()
        
        if existing_playlist:
            # Update existing playlist
            cursor.execute('''
                UPDATE playlists 
                SET playlist_name = ?,
                    curator_name = ?,
                    curator_id = ?,
                    follower_count = ?,
                    track_count = ?,
                    email = ?,
                    description = ?,
                    last_updated = ?
                WHERE playlist_id = ?
            ''', (
                playlist_data['playlist_name'],
                playlist_data['curator_name'],
                playlist_data['curator_id'],
                playlist_data['follower_count'],
                playlist_data['track_count'],
                playlist_data.get('email', None),
                playlist_data.get('description', None),
                datetime.now(),
                playlist_id
            ))
            return 'updated'
        else:
            # Insert new playlist
            cursor.execute('''
                INSERT INTO playlists (
                    playlist_id, playlist_name, curator_name, curator_id,
                    playlist_url, follower_count, track_count, email,
                    description, last_updated
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                playlist_id,
                playlist_data['playlist_name'],
                playlist_data['curator_name'],
                playlist_data['curator_id'],
                playlist_data['playlist_url'],
                playlist_data['follower_count'],
                playlist_data['track_count'],
                playlist_data.get('email', None),
                playlist_data.get('description', None),
                datetime.now()
            ))
            return 'added'
        
    def process_csv_import(self, filename, df):
        """Process CSV import and track statistics"""
        records_processed = len(df)
        records_added = 0
        records_updated = 0
        
        for _, row in df.iterrows():
            result = self.add_or_update_playlist(row)
            if result == 'added':
                records_added += 1
            else:
                records_updated += 1
        
        # Record import statistics
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT INTO import_history (
                filename, import_date, records_processed,
                records_added, records_updated
            ) VALUES (?, ?, ?, ?, ?)
        ''', (
            filename,
            datetime.now(),
            records_processed,
            records_added,
            records_updated
        ))
        
        self.conn.commit()
        return {
            'processed': records_processed,
            'added': records_added,
            'updated': records_updated
        }
    
    def get_all_playlists(self):
        """Retrieve all playlists from database"""
        query = '''
            SELECT playlist_name, curator_name, curator_id, playlist_url,
                   follower_count, track_count, email, description,
                   last_updated
            FROM playlists
            ORDER BY follower_count DESC
        '''
        return pd.read_sql_query(query, self.conn)
    
    def close(self):
        """Close database connection"""
        self.conn.close()