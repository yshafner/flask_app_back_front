import requests
import json
import csv
import time
import os
import sys
import argparse
import atexit
import signal
from typing import Callable, Optional, Union
import os
import jwt
import datetime
from tqdm import tqdm
from supabase import create_client
import math
import requests
import json
import time
import os
import random

# empty the apple tracks json
with open('apple_tracks.json', 'w') as f:
    json.dump([], f)
# Apple Music JWT Configuration
TEAM_ID = 'J72YS4R6L2'
KEY_ID = 'DP4VTL282Q'
PRIVATE_KEY_PATH = 'auth.p8'  # Path to your private key file

# Current JWT token for Apple Music
JWT_TOKEN = None

# Apple Music API Base URL
BASE_URL = "https://api.music.apple.com/v1/catalog/us"

# API Request Headers (will be updated with token)
headers = {
    "Authorization": f"Bearer {JWT_TOKEN}"
}

class Stopwatch:
    def __init__(self, interval_hours: Union[int, float] = 24):
        """
        Initialize a stopwatch to run tasks at specified intervals.
        
        Args:
            interval_hours: Number of hours between task executions (default: 24)
        """
        self.interval_hours = interval_hours
        self.interval_seconds = interval_hours * 3600
        self.start_time = None
        self.running = False
        
    def start(self):
        """Start the stopwatch"""
        self.start_time = time.time()
        self.running = True
        
    def reset(self):
        """Reset the stopwatch"""
        self.start_time = time.time()
        
    def elapsed_time(self) -> float:
        """
        Get elapsed time since stopwatch started
        
        Returns:
            float: Elapsed time in seconds
        """
        if not self.running:
            return 0
        return time.time() - self.start_time
    
    def elapsed_formatted(self) -> str:
        """
        Get elapsed time in a human-readable format (HH:MM:SS)
        
        Returns:
            str: Formatted elapsed time
        """
        elapsed = self.elapsed_time()
        hours, remainder = divmod(int(elapsed), 3600)
        minutes, seconds = divmod(remainder, 60)
        return f"{hours:02}:{minutes:02}:{seconds:02}"
    
    def time_until_next_run(self) -> float:
        """
        Calculate time remaining until next scheduled run
        
        Returns:
            float: Time in seconds until next run
        """
        if not self.running:
            return self.interval_seconds
            
        elapsed = self.elapsed_time()
        return max(0, self.interval_seconds - elapsed)
    
    def is_time_to_run(self) -> bool:
        """
        Check if it's time to run the scheduled task
        
        Returns:
            bool: True if it's time to run the task
        """
        if not self.running:
            return False
        return self.elapsed_time() >= self.interval_seconds


class ScheduledTaskRunner:
    def __init__(self, task_function: Callable, interval_hours: Union[int, float] = 24):
        """
        Initialize a task runner with specified interval
        
        Args:
            task_function: Function to be executed periodically
            interval_hours: Number of hours between executions (default: 24)
        """
        self.task_function = task_function
        self.stopwatch = Stopwatch(interval_hours)
        self.run_count = 0
        
    def _get_next_run_time(self):
        """Calculate and format the next scheduled run time"""
        next_time = datetime.datetime.now() + datetime.timedelta(seconds=self.stopwatch.time_until_next_run())
        return next_time.strftime("%Y-%m-%d %H:%M:%S")
    
    def cleanup(self):
        """Clean up resources and flush output before exit"""
        sys.stdout.write("\n")  # Move to a new line
        print(f"Task scheduler shutdown. Completed {self.run_count} task runs.")
        sys.stdout.flush()  # Ensure all output is flushed
    
    def signal_handler(self, sig, frame):
        """Handle termination signals gracefully"""
        print("\nReceived termination signal. Shutting down...")
        self.cleanup()
        sys.exit(0)
        
    def start(self):
        """Start the task runner"""
        # Register cleanup handlers
        atexit.register(self.cleanup)
        signal.signal(signal.SIGINT, self.signal_handler)
        if hasattr(signal, 'SIGTERM'):
            signal.signal(signal.SIGTERM, self.signal_handler)
        
        # Clear any previous output
        self._clear_console()
            
        self.stopwatch.start()
        print(f"Task scheduler started. Will run every {self.stopwatch.interval_hours} hours.")
        print(f"First run scheduled for: {self._get_next_run_time()}")
        print("Press Ctrl+C to stop the scheduler.")
        sys.stdout.flush()
        
        try:
            while True:
                # Update progress display
                self._update_progress_display()
                
                # Check if it's time to run
                if self.stopwatch.is_time_to_run():
                    self._execute_task()
                    self.stopwatch.reset()
                    self.run_count += 1
                    
                    # Clear console after task execution
                    self._clear_console()
                    print(f"Task scheduler restarted. Will run every {self.stopwatch.interval_hours} hours.")
                    print(f"Next run scheduled for: {self._get_next_run_time()}")
                    print(f"Previous runs completed: {self.run_count}")
                    print("Press Ctrl+C to stop the scheduler.")
                    sys.stdout.flush()
                
                # Small sleep to prevent high CPU usage
                time.sleep(1)
                
        except KeyboardInterrupt:
            # Will be handled by signal_handler
            pass
        except Exception as e:
            print(f"\nUnexpected error: {str(e)}")
            self.cleanup()
            raise
    
    def _execute_task(self):
        """Execute the scheduled task and handle any exceptions"""
        # Clear the console before executing the task
        self._clear_console()
        
        print(f"[{datetime.datetime.now()}] Executing scheduled task (Run #{self.run_count + 1})...")
        sys.stdout.flush()
        
        try:
            self.task_function()
            print(f"[{datetime.datetime.now()}] Task completed successfully.")
            sys.stdout.flush()
        except Exception as e:
            print(f"[{datetime.datetime.now()}] Error executing task: {str(e)}")
            sys.stdout.flush()
            
    def _clear_console(self):
        """Clear the console completely"""
        if os.name == 'nt':  # Windows
            os.system('cls')
        else:  # Unix/Linux/Mac
            os.system('clear')
    
    def _update_progress_display(self):
        """Update the progress display on the same line"""
        remaining = self.stopwatch.time_until_next_run()
        hours, remainder = divmod(int(remaining), 3600)
        minutes, seconds = divmod(remainder, 60)
        
        # Calculate a simple progress bar (20 characters wide)
        progress_pct = 1 - (remaining / self.stopwatch.interval_seconds)
        bar_width = 20
        filled_width = int(bar_width * progress_pct)
        bar = "█" * filled_width + "░" * (bar_width - filled_width)
        
        # Include more info in the status line
        current_time = datetime.datetime.now().strftime("%H:%M:%S")
        status = f"\r[{current_time}] Elapsed: {self.stopwatch.elapsed_formatted()} | Next run in: {hours:02}:{minutes:02}:{seconds:02} | [{bar}] {progress_pct:.1%} | Runs: {self.run_count}"
        
        # Get terminal width if possible to avoid line wrapping issues
        try:
            terminal_width = os.get_terminal_size().columns
            if len(status) > terminal_width:
                status = status[:terminal_width-3] + "..."
        except (AttributeError, OSError):
            pass  # Use default status line if terminal size is unavailable
        
        # Print with carriage return and flush to update the same line
        sys.stdout.write(status)
        sys.stdout.flush()







def generate_apple_jwt():
    """
    Generate a new JWT token for Apple Music API.
    """
    try:
        # Read the private key
        with open(PRIVATE_KEY_PATH, 'r') as file:
            private_key = file.read()

        # Set token expiration (4 hours from now)
        time_now = datetime.datetime.now()
        expiration_time = time_now + datetime.timedelta(hours=4)

        # Prepare the token payload
        payload = {
            'iss': TEAM_ID,
            'iat': int(time_now.timestamp()),
            'exp': int(expiration_time.timestamp())
        }

        # Create the JWT token
        token = jwt.encode(
            payload=payload,
            key=private_key,
            algorithm='ES256',
            headers={
                'kid': KEY_ID,
                'typ': 'JWT'
            }
        )
        
        print(f"Generated new Apple Music JWT token, expires at {expiration_time}")
        return token
        
    except Exception as e:
        print(f"Error generating Apple Music JWT: {str(e)}")
        return None

def make_api_request(url, method="GET", params=None, data=None, max_retries=5):
    """
    Make a request to Apple Music API with automatic token renewal and rate limit handling
    
    Args:
        url: The full API URL
        method: HTTP method (GET/POST)
        params: URL parameters
        data: Request body for POST
        max_retries: Maximum number of retry attempts
        
    Returns:
        Response object or None if all retries failed
    """
    global JWT_TOKEN, headers
    
    # Initialize token if needed
    if JWT_TOKEN is None:
        JWT_TOKEN = generate_apple_jwt()
        if JWT_TOKEN is None:
            print("Failed to generate Apple Music JWT token")
            return None
        headers["Authorization"] = f"Bearer {JWT_TOKEN}"
    
    retry_count = 0
    
    while retry_count <= max_retries:
        try:
            # Make the request
            if method.upper() == "GET":
                response = requests.get(url, headers=headers, params=params)
            elif method.upper() == "POST":
                response = requests.post(url, headers=headers, params=params, json=data)
            else:
                print(f"Unsupported HTTP method: {method}")
                return None
            
            # Handle 401 (Unauthorized) - expired token
            if response.status_code == 401:
                print("Apple Music API returned 401 - token has expired, generating new token")
                
                # Generate a new token
                new_token = generate_apple_jwt()
                if new_token is None:
                    print("Failed to generate new Apple Music JWT token after 401 error")
                    return None
                
                # Update token
                JWT_TOKEN = new_token
                headers["Authorization"] = f"Bearer {JWT_TOKEN}"
                
                retry_count += 1
                print(f"Retrying with new token (attempt {retry_count}/{max_retries})")
                continue  # Retry immediately with new token
            
            # Handle 429 (Too Many Requests) - rate limiting
            elif response.status_code == 429:
                retry_count += 1
                
                # Get retry delay from response header or use default
                retry_after = response.headers.get('Retry-After', '30')
                retry_delay = int(retry_after) if retry_after.isdigit() else 30
                
                print(f"Rate limited (429). Waiting for {retry_delay} seconds. Attempt {retry_count}/{max_retries}")
                time.sleep(retry_delay)
                continue  # Retry after delay
            
            # Return response if successful
            if response.status_code == 200:
                return response
            
            # Handle other errors
            print(f"API error: {response.status_code} - {response.text}")
            retry_count += 1
            
            if retry_count <= max_retries:
                print(f"Retrying in 5 seconds (attempt {retry_count}/{max_retries})")
                time.sleep(5)
            
        except Exception as e:
            print(f"Request error: {str(e)}")
            retry_count += 1
            
            if retry_count <= max_retries:
                print(f"Retrying in 5 seconds (attempt {retry_count}/{max_retries})")
                time.sleep(5)
    
    print(f"Failed after {max_retries} attempts")
    return None

def get_tracks_alternative_method(playlist_id, playlist_info, max_retries=5):
    """Alternative method to get tracks using the include=tracks parameter"""
    all_tracks = []
    playlist_title = playlist_info["name"]
    playlist_image_url = playlist_info["artwork_url"]
    playlist_url = playlist_info["url"]
    
    # Try the relationship endpoint
    url = f"{BASE_URL}/playlists/{playlist_id}?include=tracks"
    
    response = make_api_request(url)
    if not response:
        return []
    
    try:
        data = response.json()
        
        if 'included' not in data:
            print("No 'included' field in response from alternative method")
            return []
            
        # Extract tracks from the included field
        tracks = [item for item in data.get('included', []) if item.get('type') == 'songs']
        
        for track in tracks:
            attrs = track.get('attributes', {})
            
            # Extract artists - some tracks have multiple artists
            artists = []
            if 'artistName' in attrs:
                # Handle cases where multiple artists are separated by comma, &, or featuring
                artist_text = attrs['artistName']
                for separator in [', ', ' & ', ' and ', ' feat. ', ' featuring ', ' with ', ' x ']:
                    if separator in artist_text:
                        artists = [a.strip() for a in artist_text.split(separator)]
                        break
                
                # If no separators were found, add the single artist
                if not artists:
                    artists = [artist_text]
            
            # Create the track entry in the desired format
            track_entry = {
                "song_name": attrs.get('name', 'Unknown Song'),
                "artist_name": artists,
                "song_url": f"https://music.apple.com/us/song/{track.get('id', '')}",
                "playlist_url": playlist_url,
                "playlist_title": playlist_title,
                "playlist_image_url": playlist_image_url
            }
            
            all_tracks.append(track_entry)
        
        return all_tracks
    
    except Exception as e:
        print(f"Error in alternative method: {str(e)}")
        return []

def try_different_storefronts(playlist_id, playlist_info, max_retries=5):
    """Try different storefronts to see if the playlist is region-restricted"""
    storefronts = ["us", "gb", "jp", "de", "fr"]
    playlist_url = playlist_info["url"]
    playlist_title = playlist_info["name"]
    playlist_image_url = playlist_info["artwork_url"]
    
    for storefront in storefronts:
        all_tracks = []
        url = f"https://api.music.apple.com/v1/catalog/{storefront}/playlists/{playlist_id}/tracks"
        
        print(f"Trying storefront: {storefront}")
        
        response = make_api_request(url)
        if not response:
            continue
        
        try:
            data = response.json()
            tracks_batch = data.get('data', [])
            
            if not tracks_batch:
                continue
                
            # Process each track
            for track in tracks_batch:
                attrs = track['attributes']
                
                # Extract artists
                artists = []
                if 'artistName' in attrs:
                    artist_text = attrs['artistName']
                    for separator in [', ', ' & ', ' and ', ' feat. ', ' featuring ', ' with ', ' x ']:
                        if separator in artist_text:
                            artists = [a.strip() for a in artist_text.split(separator)]
                            break
                    
                    if not artists:
                        artists = [artist_text]
                
                track_entry = {
                    "song_name": attrs.get('name', 'Unknown Song'),
                    "artist_name": artists,
                    "song_url": f"https://music.apple.com/us/song/{track.get('id', '')}",
                    "playlist_url": playlist_url,
                    "playlist_title": playlist_title,
                    "playlist_image_url": playlist_image_url
                }
                
                all_tracks.append(track_entry)
            
            # If we found tracks, return them
            if all_tracks:
                print(f"Found {len(all_tracks)} tracks in storefront {storefront}")
                return all_tracks
            
        except Exception as e:
            print(f"Error in storefront {storefront}: {str(e)}")
    
    return []

def get_playlist_tracks(playlist_info, max_retries=5):
    """Get all tracks from a playlist with automatic token renewal and retry logic"""
    all_tracks = []
    offset = 0
    limit = 100  # Maximum allowed by Apple Music API
    
    # Extract playlist details from the JSON
    playlist_id = playlist_info["playlist_id"]
    playlist_title = playlist_info["name"]
    playlist_url = playlist_info["url"]
    playlist_image_url = playlist_info["artwork_url"]
    
    # First try the standard method with pagination
    while True:
        # Build URL with pagination parameters
        url = f"{BASE_URL}/playlists/{playlist_id}/tracks?limit={limit}&offset={offset}"
        
        response = make_api_request(url)
        
        # If the request failed completely or we got a 404
        if not response or response.status_code == 404:
            break
        
        try:
            # Parse the response
            data = response.json()
            tracks_batch = data.get('data', [])
            
            # If no tracks in batch, break out of the loop
            if not tracks_batch:
                break
            
            # Process each track to match the desired JSON structure
            for track in tracks_batch:
                attrs = track['attributes']
                
                # Extract artists - some tracks have multiple artists
                artists = []
                if 'artistName' in attrs:
                    # Handle cases where multiple artists are separated by comma, &, or featuring
                    artist_text = attrs['artistName']
                    for separator in [', ', ' & ', ' and ', ' feat. ', ' featuring ', ' with ', ' x ']:
                        if separator in artist_text:
                            artists = [a.strip() for a in artist_text.split(separator)]
                            break
                    
                    # If no separators were found, add the single artist
                    if not artists:
                        artists = [artist_text]
                
                # Create the track entry in the desired format
                track_entry = {
                    "song_name": attrs.get('name', 'Unknown Song'),
                    "artist_name": artists,
                    "song_url": f"https://music.apple.com/us/song/{track.get('id', '')}",
                    "playlist_url": playlist_url,
                    "playlist_title": playlist_title,
                    "playlist_image_url": playlist_image_url
                }
                
                all_tracks.append(track_entry)
            
        except Exception as e:
            print(f"Error while processing tracks: {str(e)}")
            break
            
        # Update offset for next batch
        offset += limit
        
        # If we received fewer tracks than the limit, we've reached the end
        if len(tracks_batch) < limit:
            break
    
    # If we didn't get any tracks with the standard method, try alternatives
    if not all_tracks:
        print(f"Standard method failed for playlist {playlist_id}. Trying alternative method...")
        all_tracks = get_tracks_alternative_method(playlist_id, playlist_info)
        
        # If that didn't work, try different storefronts
        if not all_tracks:
            print(f"Alternative method failed. Trying different storefronts...")
            all_tracks = try_different_storefronts(playlist_id, playlist_info)
    
    return all_tracks

def append_to_json_file(file_path, new_data):
    """Append new data to an existing JSON file with proper formatting"""
    if not new_data:
        return 0  # No tracks to append
    
    try:
        # If file doesn't exist or is empty, create it with proper formatting
        if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
            with open(file_path, 'w', encoding='utf-8') as f:
                # Create a new formatted JSON file
                json.dump(new_data, f, ensure_ascii=False, indent=4)
            return len(new_data)
        
        # File exists, try to load it
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                existing_data = json.load(f)
                if not isinstance(existing_data, list):
                    raise ValueError("JSON file does not contain an array")
        except (json.JSONDecodeError, ValueError) as e:
            print(f"Error reading JSON file: {str(e)}. Creating a new file.")
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(new_data, f, ensure_ascii=False, indent=4)
            return len(new_data)
        
        # Append new data and rewrite the file with proper formatting
        existing_data.extend(new_data)
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(existing_data, f, ensure_ascii=False, indent=4)
        
        return len(new_data)
    
    except Exception as e:
        print(f"Unexpected error: {str(e)}. Attempting to save data safely.")
        try:
            # Fallback: Save just the new data if everything else fails
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(new_data, f, ensure_ascii=False, indent=4)
            return len(new_data)
        except Exception as e2:
            print(f"Critical error: {str(e2)}. Could not save data.")
            return 0

def process_playlist_json_file(file_path):
    """Process all playlist data from a JSON file and save all tracks at the end"""
    # Generate an initial token at startup
    global JWT_TOKEN, headers
    JWT_TOKEN = generate_apple_jwt()
    if JWT_TOKEN:
        headers["Authorization"] = f"Bearer {JWT_TOKEN}"
        print("Initial JWT token generated successfully")
    else:
        print("Failed to generate initial JWT token, will try again as needed")
    
    # Read playlist data from JSON file
    with open(file_path, 'r', encoding='utf-8') as f:
        playlists = json.load(f)
    
    total_playlists = len(playlists)
    print(f"Total playlists: {total_playlists}")
    
    output_file = 'apple_tracks.json'
    all_tracks = []  # Collect all tracks here
    
    # Process each playlist with progress tracking
    for i, playlist_info in enumerate(playlists, 1):
        playlist_id = playlist_info["playlist_id"]
        print(f"Fetching: {i} / {total_playlists} - Playlist: {playlist_info['name']} (ID: {playlist_id})")
        
        # Get tracks for this playlist
        playlist_tracks = get_playlist_tracks(playlist_info)
        tracks_count = len(playlist_tracks)
        
        if tracks_count > 0:
            # Add tracks to our collection (not writing to file yet)
            all_tracks.extend(playlist_tracks)
            print(f"Found {tracks_count} tracks in playlist {playlist_id}")
            print(f"Total tracks collected: {len(all_tracks)}")
        else:
            print(f"No tracks found in playlist {playlist_id}")
        
        # Add a small delay between playlists to avoid rate limiting
        if i < total_playlists:
            time.sleep(0.1)
    
    # Now write all tracks to the file once, with proper formatting
    if all_tracks:
        try:
            # Check if file exists and has content
            if os.path.exists(output_file) and os.path.getsize(output_file) > 0:
                try:
                    # Try to load existing tracks
                    with open(output_file, 'r', encoding='utf-8') as f:
                        existing_tracks = json.load(f)
                    
                    # If existing tracks is a list, extend it
                    if isinstance(existing_tracks, list):
                        existing_tracks.extend(all_tracks)
                        all_tracks = existing_tracks
                    
                    print(f"Added {len(all_tracks) - len(existing_tracks)} new tracks to existing {len(existing_tracks)} tracks.")
                except (json.JSONDecodeError, ValueError) as e:
                    print(f"Error reading existing file: {str(e)}. Creating new file.")
            
            # Write all tracks with proper formatting
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(all_tracks, f, ensure_ascii=False, indent=4)
                
            print(f"\nCompleted processing all playlists!")
            print(f"Total tracks saved: {len(all_tracks)}")
            print(f"Output saved to {output_file}")
        except Exception as e:
            print(f"Error saving tracks: {str(e)}")
    else:
        print("No tracks found in any playlist.")





def update_database_apple():
    print(f"Starting Apple Music database update at {time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Load your JSON data
    with open('apple_tracks.json', 'r', encoding='utf-8') as f:
        songs_data = json.load(f)
    
    # Initialize Supabase client for Apple Music
    apple_url = "https://ouqcpjuvtfyxdktjqevg.supabase.co"
    apple_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im91cWNwanV2dGZ5eGRrdGpxZXZnIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc0MTU4MTc1OSwiZXhwIjoyMDU3MTU3NzU5fQ.ms4F7hTZfujVGxObxELJOZv-GtCBENJe8sn2WqyM0z0"
    apple_supabase = create_client(apple_url, apple_key)
    
    # Check which table is currently active
    try:
        config = apple_supabase.table("db_config").select("*").eq("id", 2).execute()  # Using id: 2 for Apple Music
        if not config.data:
            # If no config data, create it (fallback)
            apple_supabase.table("db_config").insert({"id": 2, "active_table": "song_apple_green"}).execute()
            active_table = "song_apple_green"
        else:
            active_table = config.data[0]["active_table"]
        
        # Determine which table to update
        inactive_table = "song_apple_blue" if active_table == "song_apple_green" else "song_apple_green"
        
        print(f"Current active Apple Music table: {active_table}")
        print(f"Will update inactive table: {inactive_table}")
        
        # Clear the inactive table in batches instead of all at once
        clear_table_in_batches(apple_supabase, inactive_table)
        
        # Process in batches
        batch_size = 1000
        total_songs = len(songs_data)
        total_batches = math.ceil(total_songs / batch_size)
        
        print(f"Starting upload of {total_songs} Apple Music songs in {total_batches} batches to {inactive_table}...")
        
        for i in range(0, total_songs, batch_size):
            current_batch = i // batch_size + 1
            batch = songs_data[i:i+batch_size]
            
            print(f"Processing batch {current_batch}/{total_batches} ({len(batch)} songs)...")
            
            # Format data for PostgreSQL
            formatted_batch = []
            for song in batch:
                formatted_batch.append({
                    "song_name": song["song_name"],
                    "artist_names": song["artist_name"],
                    "song_url": song["song_url"],
                    "playlist_url": song["playlist_url"],
                    "playlist_title": song["playlist_title"],
                    "playlist_image_url": song["playlist_image_url"]
                })
            
            # Insert batch
            try:
                response = apple_supabase.table(inactive_table).insert(formatted_batch).execute()
                
                songs_uploaded = i + len(batch)
                percent_complete = (songs_uploaded / total_songs) * 100
                
                print(f"Batch {current_batch}/{total_batches} complete: {songs_uploaded}/{total_songs} songs uploaded ({percent_complete:.1f}%)")
            except Exception as e:
                print(f"Error inserting batch {current_batch}: {e}")
                # Continue with next batch
        
        # Now switch the active table
        print(f"Switching active Apple Music table from {active_table} to {inactive_table}...")
        apple_supabase.table("db_config").update({"active_table": inactive_table}).eq("id", 2).execute()
        
        # Create a status file that our Flask app can check
        with open("apple_db_updated.txt", "w") as f:
            f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"active_table={inactive_table}\n")
        
        print(f"Apple Music database update complete. Active table is now: {inactive_table}")
        
    except Exception as e:
        print(f"Error in Apple Music database update: {e}")
        print("Update failed. Please check logs and try again.")
    
    print(f"Apple Music upload process finished at {time.strftime('%Y-%m-%d %H:%M:%S')}")

def clear_table_in_batches(supabase_client, table_name, batch_size=1000):
    """Clear a table in batches to avoid timeout errors"""
    print(f"Clearing {table_name} table in batches...")
    total_deleted = 0
    
    while True:
        try:
            # Get a batch of records to delete
            response = supabase_client.table(table_name).select("id").limit(batch_size).execute()
            
            if not response.data or len(response.data) == 0:
                print(f"No more records to delete in {table_name}")
                break
                
            ids_to_delete = [record["id"] for record in response.data]
            batch_count = len(ids_to_delete)
            
            # Delete this batch
            supabase_client.table(table_name).delete().in_("id", ids_to_delete).execute()
            
            total_deleted += batch_count
            print(f"Deleted batch of {batch_count} records from {table_name}. Total deleted: {total_deleted}")
            
            # If we got fewer records than the batch size, we're done
            if batch_count < batch_size:
                break
                
        except Exception as e:
            print(f"Error during batch deletion: {e}")
            # If there's an error, try a smaller batch size
            if batch_size > 100:
                batch_size = batch_size // 2
                print(f"Reducing batch size to {batch_size} and retrying...")
            else:
                print(f"Unable to delete records even with small batch size. Aborting.")
                break
    
    print(f"Finished clearing {table_name} table. Total records deleted: {total_deleted}")
    return total_deleted


def run_main_task():
    """Function that runs your main code"""
    print("Running task at:", datetime.datetime.now())
    sys.stdout.flush()
    
    input_file = 'apple_playlist_urls.json'
    
    # Process all playlists from the JSON file
    process_playlist_json_file(input_file)
    
    
    json_file_path = "apple_tracks.json"
    if os.path.exists(json_file_path):
        update_database_apple()
        print("Database updated successfully...")
    else:
        print("Data file not found")
    
    # Add a small delay to ensure output is visible before console clear
    time.sleep(3)
    print("\nTask execution completed. Console will be cleared in 3 seconds...")
    sys.stdout.flush()
    time.sleep(3)



def get_curator_playlists(storefront, curator_id, jwt_token, max_retries=3, limit=500):
    base_url = f"https://api.music.apple.com/v1/catalog/{storefront}/apple-curators/{curator_id}/playlists"
    headers = {
        "Authorization": f"Bearer {jwt_token}"
    }
    
    all_playlists = []
    next_url = base_url
    
    while next_url and len(all_playlists) < limit:
        # Make request to current URL with retry logic
        retries = 0
        success = False
        
        while not success and retries <= max_retries:
            response = requests.get(next_url, headers=headers)
            
            if response.status_code == 200:
                success = True
            elif response.status_code == 429:
                retries += 1
                if retries <= max_retries:
                    wait_time = 30 + random.randint(0, 10)  # 30-40 second wait
                    print(f"Rate limited (429). Retry {retries}/{max_retries} after {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    print(f"Failed after {max_retries} retries. Moving to next URL.")
                    break
            else:
                print(f"Error fetching playlists for curator {curator_id}: {response.status_code}")
                break
        
        if not success:
            break
            
        data = response.json()
        
        # Extract playlist info
        if 'data' in data:
            for playlist in data['data']:
                if len(all_playlists) >= limit:
                    break
                    
                playlist_info = {
                    'curator_id': curator_id,
                    'playlist_id': playlist['id'],
                    'name': playlist['attributes']['name'] if 'name' in playlist['attributes'] else '',
                    'url': playlist['attributes']['url'] if 'url' in playlist['attributes'] else '',
                }
                
                # Extract artwork URL if available
                if 'artwork' in playlist['attributes'] and 'url' in playlist['attributes']['artwork']:
                    artwork_url = playlist['attributes']['artwork']['url']
                    # Replace {w} and {h} with actual dimensions
                    artwork_url = artwork_url.replace('{w}', '1000').replace('{h}', '1000')
                    playlist_info['artwork_url'] = artwork_url
                else:
                    playlist_info['artwork_url'] = ''
                
                all_playlists.append(playlist_info)
                
                # Show progress
                if len(all_playlists) % 10 == 0:
                    print(f"Fetched {len(all_playlists)} / {limit} playlists for curator {curator_id}")
        
        # Check if there's a next batch
        if 'next' in data and data['next'] and len(all_playlists) < limit:
            next_url = f"https://api.music.apple.com{data['next']}"
        else:
            next_url = None
        
        # Add a small delay to avoid rate limiting
        time.sleep(0.5)
    
    print(f"Completed fetching {len(all_playlists)} total playlists for curator {curator_id}")
    return all_playlists

def save_playlists_to_json(playlists, output_file):
    """
    Save or append playlists to a JSON file.
    
    Parameters:
    - playlists: List of playlist data to save
    - output_file: Filename to save to
    """
    # Load existing data if file exists
    existing_data = []
    if os.path.exists(output_file):
        with open(output_file, 'r', encoding='utf-8') as f:
            try:
                existing_data = json.load(f)
            except json.JSONDecodeError:
                # If the file is empty or invalid JSON, start fresh
                existing_data = []
    
    # Append new playlists
    existing_data.extend(playlists)
    
    # Save all playlists to the JSON file
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(existing_data, f, ensure_ascii=False, indent=2)
    
    return len(existing_data)


    
    
if __name__ == "__main__":
    #empty the file
    with open('apple_playlist_urls.json', 'w') as f:
        json.dump([], f)
    
    jwt_token = generate_apple_jwt()
    if not jwt_token:
        print("Failed to generate JWT token. Exiting.")
        sys.exit(1)
    
    print(f"Generated new JWT token successfully")
    
    # Store front
    storefront = "us"
    
    # Output JSON file
    output_file = "apple_music_playlists.json"
    
    # List of curator IDs
    curator_ids = [
        '976439587',  # Worldwide
        '976439553',  # Urbano Latino
        '1532467784',  # Up Next
        '1564180390',  # Spatial Audio
        '976439585',  # Soul/Funk
        '1558257257',  # Sleep
        '976439554',  # Rock
        '976439552',  # Reggae
        '1531543191',  # Apple Music Radio
        '976439551',  # R&B
        '976439548',  # Pop
        '976439549',  # Pop Latino
        '1558257035',  # Party
        '976439547',  # Oldies
        '1558257146',  # Motivation
        '976439543',  # Metal
        '976439544',  # Música Mexicana
        '1649426593',  # Apple Music Live
        '1531542847',  # Latin
        '976439538',  # Kids
        '988658197',  # K-Pop
        '976439542',  # Jazz
        '976439541',  # Indie
        '1526756058',  # Hits
        '976439539',  # Hip-Hop
        '979231690',  # Hard Rock
        '1558256909',  # Fitness
        '976439586',  # Film, TV & Stage
        '1558256919',  # Feel Good
        '1555173397',  # Family
        '1558256771',  # Essentials
        '976439536',  # Electronic
        '1441811365',  # DJ Mixes
        '1554938339',  # Decades
        '976439535',  # Dance
        '976439534',  # Country
        '976439532',  # Classical
        '976439531',  # Classic Rock
        '1558256251',  # Chill
        '976439528',  # Blues
        '1554941247',  # Behind the Songs
        '976439527',  # Americana
        '976439526',  # Alternative
        '1747003654',  # Afrobeats
        '1526866189',  # '80s
        '1526866635',  # 2000s
        '1482068485'   # Christian
    ]
    # Process each curator and save after each one
    for i, curator_id in enumerate(curator_ids):
        print(f"\nProcessing curator {i+1}/{len(curator_ids)}: {curator_id}")
        
        # Get playlists for this curator
        playlists = get_curator_playlists(storefront, curator_id, jwt_token, 800) # 500 is the limit
        
        # Save/append to JSON file after each curator
        total_saved = save_playlists_to_json(playlists, output_file)
        print(f"Saved curator {curator_id} playlists. Total playlists in file: {total_saved}")
        
        # Add a delay between curator requests to avoid rate limiting
        time.sleep(1)
    
    print(f"\nAll done! All playlists have been saved to {output_file}")
    
    
    input_file = 'apple_playlist_urls.json'
    
    process_playlist_json_file(input_file)

    json_file_path = "apple_tracks.json"
    if os.path.exists(json_file_path):
        update_database_apple()
        print("Database updated successfully...")
    else:
        print("Data file not found")
    
    
    
    parser = argparse.ArgumentParser(description="Run a task at specified intervals")
    parser.add_argument(
        "--hours", 
        type=float, 
        default=24.0,
        help="Interval in hours between task executions (default: 24.0)"
    )
    parser.add_argument(
        "--run-now", 
        action="store_true",
        help="Run the task immediately on startup"
    )
    parser.add_argument(
        "--reset", 
        action="store_true",
        help="Clear the console and reset any previous executions"
    )
    
    args = parser.parse_args()
    
    # Clear the console if requested
    if args.reset:
        if os.name == 'nt':  # Windows
            os.system('cls')
        else:  # Unix/Linux/Mac
            os.system('clear')
    
    # Create scheduler with specified interval
    scheduler = ScheduledTaskRunner(run_main_task, args.hours)
    
    # Run task immediately if requested
    if args.run_now:
        print(f"[{datetime.datetime.now()}] Running initial task...")
        sys.stdout.flush()  # Ensure the message is displayed immediately
        run_main_task()
        scheduler.run_count += 1
        print(f"[{datetime.datetime.now()}] Initial task completed.")
        sys.stdout.flush()
    
    # Start the scheduler
    print(f"[{datetime.datetime.now()}] Starting task scheduler...")
    sys.stdout.flush()
    scheduler.start()