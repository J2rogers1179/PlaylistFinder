import pandas as pd
import re
from pathlib import Path

class PlaylistCSVProcessor:
    @staticmethod
    def extract_email(text):
        """Extract email from text using regex"""
        if pd.isna(text):
            return None
        email_pattern = r'[\w\.-]+@[\w\.-]+\.\w+'
        matches = re.findall(email_pattern, str(text))
        return matches[0] if matches else None
    
    def process_csv(self, file_path):
        """Process CSV file and prepare data for database import"""
        # Read CSV file
        df = pd.read_csv(file_path)
        
        # Clean column names
        df.columns = df.columns.str.strip().str.lower()
        
        # Extract emails from description if not already present
        if 'email' not in df.columns:
            df['email'] = df['description'].apply(self.extract_email)
        
        # Convert follower_count and track_count to numeric
        df['follower_count'] = pd.to_numeric(df['follower_count'], errors='coerce').fillna(0)
        df['track_count'] = pd.to_numeric(df['track_count'], errors='coerce').fillna(0)
        
        return df