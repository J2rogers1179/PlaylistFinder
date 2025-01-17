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
                playlist_id TEXT,
                curator_id TEXT UNIQUE,
                playlist_name TEXT,
                curator_name TEXT,
                playlist_url TEXT,
                follower_count INTEGER,
                track_count INTEGER,
                email TEXT,
                instagram TEXT,
                discord TEXT,
                submission_form TEXT,
                email_obfuscated TEXT,
                description TEXT,
                genres TEXT,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
    
    # Check if genres column exists, add it if it doesn't
        cursor.execute("PRAGMA table_info(playlists)")
        columns = [info[1] for info in cursor.fetchall()]
        if 'genres' not in columns:
            cursor.execute('ALTER TABLE playlists ADD COLUMN genres TEXT')
    
    # Create import history table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS import_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                filename TEXT,
                import_date TIMESTAMP,
                records_processed INTEGER,
                records_added INTEGER,
                records_skipped INTEGER
            )
        ''')
    
    # Create merge history table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS merge_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                primary_id INTEGER,
                secondary_id INTEGER,
                merge_date TIMESTAMP,
                action TEXT,
                FOREIGN KEY (primary_id) REFERENCES playlists (id),
                FOREIGN KEY (secondary_id) REFERENCES playlists (id)
            )
        ''')
    
        self.conn.commit()
        
    
    def add_or_update_playlist(self, playlist_data):
        """Add playlist if curator_id doesn't exist, update if it does"""
        cursor = self.conn.cursor()
        
        try:
            # Check if curator_id exists
            cursor.execute('SELECT id FROM playlists WHERE curator_id = ?', 
                         (playlist_data['curator_id'],))
            existing_playlist = cursor.fetchone()
            
            if existing_playlist:
                # Update existing playlist
                cursor.execute('''
                    UPDATE playlists 
                    SET playlist_name = ?,
                        curator_name = ?,
                        playlist_url = ?,
                        follower_count = ?,
                        track_count = ?,
                        email = ?,
                        instagram = ?,
                        discord = ?,
                        submission_form = ?,
                        email_obfuscated = ?,
                        description = ?,
                        genres = ?,
                        last_updated = ?
                    WHERE curator_id = ?
                ''', (
                    playlist_data['playlist_name'],
                    playlist_data['curator_name'],
                    playlist_data['playlist_url'],
                    playlist_data['follower_count'],
                    playlist_data['track_count'],
                    playlist_data.get('email'),
                    playlist_data.get('instagram'),
                    playlist_data.get('discord'),
                    playlist_data.get('submission_form'),
                    playlist_data.get('email_obfuscated'),
                    playlist_data.get('description'),
                    ','.join(playlist_data.get('genres', [])),
                    datetime.now(),
                    playlist_data['curator_id']
                ))
                return 'updated'
            else:
                # Insert new playlist
                cursor.execute('''
                    INSERT INTO playlists (
                        curator_id, playlist_name, curator_name, playlist_url,
                        follower_count, track_count, email, instagram, discord,
                        submission_form, email_obfuscated, description, genres,
                        last_updated
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    playlist_data['curator_id'],
                    playlist_data['playlist_name'],
                    playlist_data['curator_name'],
                    playlist_data['playlist_url'],
                    playlist_data['follower_count'],
                    playlist_data['track_count'],
                    playlist_data.get('email'),
                    playlist_data.get('instagram'),
                    playlist_data.get('discord'),
                    playlist_data.get('submission_form'),
                    playlist_data.get('email_obfuscated'),
                    playlist_data.get('description'),
                    ','.join(playlist_data.get('genres', [])),
                    datetime.now()
                ))
                return 'added'
                
        except sqlite3.IntegrityError:
            return 'skipped'
            
        finally:
            self.conn.commit()
    
    def process_csv_import(self, filename, df):
        """Process CSV import and track statistics"""
        records_processed = len(df)
        records_added = 0
        records_skipped = 0
        
        for _, row in df.iterrows():
            result = self.add_or_update_playlist(row.to_dict())
            if result == 'added':
                records_added += 1
            elif result == 'skipped':
                records_skipped += 1
        
        # Record import statistics
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT INTO import_history (
                filename, import_date, records_processed,
                records_added, records_skipped
            ) VALUES (?, ?, ?, ?, ?)
        ''', (
            filename,
            datetime.now(),
            records_processed,
            records_added,
            records_skipped
        ))
        
        self.conn.commit()
        return {
            'processed': records_processed,
            'added': records_added,
            'skipped': records_skipped
        }
    
    def get_all_playlists(self):
        """Retrieve all playlists from database"""
        query = '''
            SELECT * FROM playlists
            ORDER BY follower_count DESC
        '''
        df = pd.read_sql_query(query, self.conn)
    
        # Only process genres if the column exists
        if 'genres' in df.columns:
            df['genres'] = df['genres'].apply(lambda x: x.split(',') if x else [])
        else:
        # Add empty genres list if column doesn't exist
            df['genres'] = df.apply(lambda x: [], axis=1)
    
        return df
    
    def get_import_history(self):
        """Retrieve import history"""
        query = '''
            SELECT * FROM import_history
            ORDER BY import_date DESC
        '''
        return pd.read_sql_query(query, self.conn)
    
    def get_stats(self):
        """Get database statistics"""
        cursor = self.conn.cursor()
        
        stats = {}
        
        # Total playlists
        cursor.execute('SELECT COUNT(*) FROM playlists')
        stats['total_playlists'] = cursor.fetchone()[0]
        
        # Playlists with contact info
        cursor.execute('''
            SELECT COUNT(*) FROM playlists 
            WHERE email IS NOT NULL 
            OR instagram IS NOT NULL 
            OR discord IS NOT NULL 
            OR submission_form IS NOT NULL
        ''')
        stats['playlists_with_contacts'] = cursor.fetchone()[0]
        
        # Total followers
        cursor.execute('SELECT SUM(follower_count) FROM playlists')
        stats['total_followers'] = cursor.fetchone()[0] or 0
        
        # Average tracks per playlist
        cursor.execute('SELECT AVG(track_count) FROM playlists')
        stats['avg_tracks'] = round(cursor.fetchone()[0] or 0, 2)
        
        return stats
    
    def close(self):
        """Close database connection"""
        self.conn.close()