import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import csv
import re
from datetime import datetime
import time
import hashlib

# Hardcoded credentials (replace with your actual credentials)
SPOTIFY_CLIENT_ID = ""  # Replace with your actual ID
SPOTIFY_CLIENT_SECRET = ""  # Replace with your actual secret

class EmailCuratorFinder:
    def __init__(self):
        self.sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(
            client_id=SPOTIFY_CLIENT_ID,
            client_secret=SPOTIFY_CLIENT_SECRET
        ))
        self.seen_playlists = set()
        self.seen_emails = set()

    def search_email_playlists(self):
        """Search playlists containing email patterns"""
        email_domains = [
            '@gmail.com',
            '@yahoo.com',
            '@hotmail.com',
            '@outlook.com',
            '@protonmail.com'
        ]
        
        all_playlists = []
        
        for domain in email_domains:
            try:
                # Search for exact domain match
                results = self.sp.search(q=domain, type='playlist', limit=50)
                if results and 'playlists' in results:
                    all_playlists.extend(results['playlists'].get('items', []))
                time.sleep(0.5)

                # Search for "at" version
                at_version = domain.replace('@', 'at ')
                results_at = self.sp.search(q=at_version, type='playlist', limit=50)
                if results_at and 'playlists' in results_at:
                    all_playlists.extend(results_at['playlists'].get('items', []))
                time.sleep(0.5)
                
            except Exception as e:
                print(f"Error searching {domain}: {str(e)}")
        
        return self._deduplicate_playlists(all_playlists)

    def _deduplicate_playlists(self, playlists):
        """Safe deduplication with None checks"""
        unique = []
        for p in playlists:
            if p and p.get('id') and p['id'] not in self.seen_playlists:
                unique.append(p)
                self.seen_playlists.add(p['id'])
        return unique
  
  
  
    def extract_emails(self, text):
        """Find all email variations in text"""
        if not text:
            return []
            
        # Normalize common obfuscations
        text = text.lower()
        replacements = [
            ('[dot]', '.'),
            ('(dot)', '.'),
           ('[at]', '@'),
            ('(at)', '@'),
            (' at ', '@'),
            (' ', '')
        ]
        
        for old, new in replacements:
            text = text.replace(old, new)
            
        # Find emails using comprehensive regex
        pattern = r'\b[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}\b'
        matches = re.findall(pattern, text)
        return [email for email in matches if email not in self.seen_emails]

    def process_playlists(self, playlists):
        results = []
        for playlist in playlists:
            self.seen_playlists.add(playlist['id'])
            
            try:
                full = self.sp.playlist(playlist['id'])
                desc = full.get('description', '')
                
                emails = self.extract_emails(desc)
                if not emails:
                    continue
                
                self.seen_emails.update(emails)
                
                results.append({
                    'Playlist Name': playlist['name'],
                    'Owner': playlist['owner']['display_name'],
                    'Email(s)': ', '.join(emails),
                    'Followers': full['followers']['total'],
                    'URL': playlist['external_urls']['spotify'],
                    'Description': desc[:500]
                })
                
            except Exception as e:
                print(f"Error processing {playlist['name']}: {str(e)}")
        
        return results

    def save_to_csv(self, data):
        filename = f"spotify_emails_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=[
                'Playlist Name', 'Owner', 'Email(s)', 
                'Followers', 'URL', 'Description'
            ])
            writer.writeheader()
            writer.writerows(data)
        return filename

if __name__ == "__main__":
    finder = EmailCuratorFinder()
    
    print("Searching for email-containing playlists...")
    playlists = finder.search_email_playlists()
    
    print(f"Found {len(playlists)} potential playlists")
    
    print("Processing playlists...")
    results = finder.process_playlists(playlists)
    
    if results:
        filename = finder.save_to_csv(results)
        print(f"Found {len(results)} playlists with emails!")
        print(f"Total unique emails collected: {len(finder.seen_emails)}")
        print(f"Saved to: {filename}")
    else:
        print("No emails found in any playlists")