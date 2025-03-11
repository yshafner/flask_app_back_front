from flask import Flask, request, jsonify, render_template, send_from_directory
import requests
import base64
import json
import time
import os
import jwt
from datetime import datetime, timedelta
from dotenv import load_dotenv
import logging
from supabase import create_client

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("app.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Initialize Supabase clients for both projects
# Spotify Supabase client
SPOTIFY_SUPABASE_URL = "https://cdcpztcchbrddkuqrtlq.supabase.co"
SPOTIFY_SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImNkY3B6dGNjaGJyZGRrdXFydGxxIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc0MTU3OTgwOCwiZXhwIjoyMDU3MTU1ODA4fQ.CciL70S5vvGgmIEIYTp9tGBur6CbrnuBaxq7PoiAvjU"

# Apple Music Supabase client
APPLE_SUPABASE_URL = "https://ouqcpjuvtfyxdktjqevg.supabase.co"
APPLE_SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im91cWNwanV2dGZ5eGRrdGpxZXZnIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc0MTU4MTc1OSwiZXhwIjoyMDU3MTU3NzU5fQ.ms4F7hTZfujVGxObxELJOZv-GtCBENJe8sn2WqyM0z0"

spotify_supabase = create_client(SPOTIFY_SUPABASE_URL, SPOTIFY_SUPABASE_KEY)
apple_supabase = create_client(APPLE_SUPABASE_URL, APPLE_SUPABASE_KEY)

app = Flask(__name__, static_folder='static')

# Spotify API Configuration
SPOTIFY_CLIENT_ID = '526866e6e8d444f09ce2b6115ffe104c'
SPOTIFY_CLIENT_SECRET = '77798743c3824ebc8d37fced346da0d3'
SPOTIFY_TOKEN_URL = 'https://accounts.spotify.com/api/token'
SPOTIFY_API_BASE_URL = 'https://api.spotify.com/v1'

# Apple Music API Configuration
APPLE_MUSIC_API_BASE_URL = 'https://api.music.apple.com/v1'

# Apple Music JWT Configuration
TEAM_ID = 'J72YS4R6L2'
KEY_ID = 'DP4VTL282Q'
PRIVATE_KEY_PATH = 'auth.p8'  # Path to your private key file

# Current JWT token for Apple Music
APPLE_MUSIC_JWT = None

# Token cache for Spotify
spotify_token_info = {
    'access_token': None,
    'expires_at': 0
}

# Cache for active table names - to avoid checking the DB on every request
spotify_active_table_cache = {
    'name': None,     # Will be populated with the active table name
    'expires_at': 0   # Force check on first request
}

apple_active_table_cache = {
    'name': None,     # Will be populated with the active table name
    'expires_at': 0   # Force check on first request
}

#########################################
# JWT MANAGEMENT FOR APPLE MUSIC
#########################################

def generate_apple_jwt():
    """
    Generate a new JWT token for Apple Music API.
    """
    try:
        # Read the private key
        with open(PRIVATE_KEY_PATH, 'r') as file:
            private_key = file.read()

        # Set token expiration (4 hours from now)
        time_now = datetime.now()
        expiration_time = time_now + timedelta(hours=4)

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
        
        logger.info(f"Generated new Apple Music JWT token, expires at {expiration_time}")
        return token
        
    except Exception as e:
        logger.error(f"Error generating Apple Music JWT: {str(e)}")
        return None

#########################################
# SPOTIFY FUNCTIONS
#########################################

def get_spotify_active_table_name():
    """
    Determine which Spotify table is currently active for queries.
    Checks the db_config table in Spotify Supabase project.
    Uses caching to avoid frequent DB lookups.
    Falls back to "song_spotify_green" if there's any issue.
    """
    global spotify_active_table_cache
    
    now = int(time.time())
    
    # Return cached value if still valid (cache for 5 minutes)
    if spotify_active_table_cache['name'] and now < spotify_active_table_cache['expires_at']:
        return spotify_active_table_cache['name']
    
    try:
        config = spotify_supabase.table("db_config").select("*").eq("id", 1).execute()
        
        if config.data and "active_table" in config.data[0]:
            table_name = config.data[0]["active_table"]
            
            # Update cache
            spotify_active_table_cache['name'] = table_name
            spotify_active_table_cache['expires_at'] = now + 300  # Cache for 5 minutes
            
            logger.info(f"Active Spotify table is: {table_name}")
            return table_name
            
        logger.warning("No active Spotify table found in db_config, falling back to 'song_spotify_green'")
        return "song_spotify_green"  # Fallback to Spotify table
        
    except Exception as e:
        logger.error(f"Error determining active Spotify table: {str(e)}")
        return "song_spotify_green"  # Fallback to Spotify table

def check_spotify_table_switch():
    """
    Check if a Spotify table switch has happened and clear the cache if needed.
    Checks the db_updated.txt file that the updater creates.
    """
    global spotify_active_table_cache
    
    try:
        # Check if db_updated.txt file exists
        if os.path.exists("db_updated.txt"):
            # Get file modification time
            mod_time = os.path.getmtime("db_updated.txt")
            
            # If file was modified after our cache was set, invalidate cache
            if mod_time > spotify_active_table_cache['expires_at'] - 300:  # 300 = cache duration
                logger.info("Detected Spotify database update, clearing active table cache")
                spotify_active_table_cache['expires_at'] = 0  # Force refresh on next request
                
                # Read the new active table from the file
                with open("db_updated.txt", "r") as f:
                    for line in f:
                        if line.startswith("active_table="):
                            new_table = line.strip()[13:]  # Extract table name
                            logger.info(f"New active Spotify table from file: {new_table}")
    except Exception as e:
        logger.error(f"Error checking for Spotify table switch: {str(e)}")

def get_spotify_token():
    """
    Get a valid Spotify API token using the Client Credentials flow.
    Refreshes the token if it has expired.
    """
    global spotify_token_info
    
    now = int(time.time())
    
    # Check if token exists and is still valid
    if spotify_token_info['access_token'] and now < spotify_token_info['expires_at']:
        return spotify_token_info['access_token']
    
    # Token is expired or doesn't exist, get a new one
    try:
        auth_string = f"{SPOTIFY_CLIENT_ID}:{SPOTIFY_CLIENT_SECRET}"
        auth_bytes = auth_string.encode('utf-8')
        auth_base64 = base64.b64encode(auth_bytes).decode('utf-8')
        
        headers = {
            'Authorization': f'Basic {auth_base64}',
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        
        data = {'grant_type': 'client_credentials'}
        
        response = requests.post(SPOTIFY_TOKEN_URL, headers=headers, data=data)
        response.raise_for_status()
        
        token_data = response.json()
        spotify_token_info['access_token'] = token_data['access_token']
        spotify_token_info['expires_at'] = now + token_data['expires_in'] - 60  # Subtract 60 seconds as a buffer
        
        logger.info("Successfully obtained new Spotify token")
        return spotify_token_info['access_token']
    
    except Exception as e:
        logger.error(f"Error getting Spotify token: {str(e)}")
        return None

def search_spotify_track(query, max_retries=3):
    """
    Search for a track on Spotify with automatic retry for rate limiting.
    
    Args:
        query (str): The search query
        max_retries (int): Maximum number of retry attempts for rate limiting
        
    Returns:
        dict: The first track result or None if no results
    """
    token = get_spotify_token()
    if not token:
        logger.error("No token available for Spotify track search")
        return None
    
    headers = {
        'Authorization': f'Bearer {token}'
    }
    
    params = {
        'q': query,
        'type': 'track',
        'limit': 1
    }
    
    url = f'{SPOTIFY_API_BASE_URL}/search'
    retry_count = 0
    
    while retry_count <= max_retries:
        try:
            response = requests.get(url, headers=headers, params=params)
            
            # Check for rate limiting (429)
            if response.status_code == 429:
                retry_count += 1
                
                # Get retry delay from response header or use default
                retry_after = response.headers.get('Retry-After', '3')
                retry_delay = int(retry_after) if retry_after.isdigit() else 3
                
                logger.warning(f"Spotify API rate limit exceeded (429). Retrying in {retry_delay} seconds. Attempt {retry_count}/{max_retries}")
                time.sleep(retry_delay)
                continue  # Retry after delay
            
            # For any other status code, raise for error handling
            response.raise_for_status()
            
            results = response.json()
            
            if not results['tracks']['items']:
                logger.info(f"No tracks found for Spotify query: {query}")
                return None
            
            track = results['tracks']['items'][0]
            return {
                'id': track['id'],
                'name': track['name'],
                'artists': ', '.join([artist['name'] for artist in track['artists']]),
                'album': track['album']['name'],
                'image': track['album']['images'][0]['url'] if track['album']['images'] else None,
                'popularity': track['popularity']
            }
        
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error when searching for Spotify track: {str(e)}")
            return None
            
        except Exception as e:
            logger.error(f"Error searching for Spotify track: {str(e)}")
            return None
    
    logger.error(f"Max retries ({max_retries}) exceeded for Spotify search request")
    return None

def search_spotify_track_by_id(track_id):
    """Search for a track on Spotify by its ID"""
    token = get_spotify_token()
    if not token:
        return None
        
    try:
        headers = {'Authorization': f'Bearer {token}'}
        response = requests.get(f'{SPOTIFY_API_BASE_URL}/tracks/{track_id}', headers=headers)
        response.raise_for_status()
        
        track = response.json()
        return {
            'id': track['id'],
            'name': track['name'],
            'artists': ', '.join([artist['name'] for artist in track['artists']]),
            'album': track['album']['name'],
            'image': track['album']['images'][0]['url'] if track['album']['images'] else None
        }
    except Exception as e:
        logger.error(f"Error searching for Spotify track by ID: {str(e)}")
        return None

def api_request_with_retry(url, headers, params=None, method="GET", data=None, max_retries=3):
    """
    Make an API request with automatic retry for 429 (Too Many Requests) errors.
    
    Args:
        url: API endpoint URL
        headers: Request headers
        params: URL parameters (optional)
        method: HTTP method (GET/POST)
        data: Request body for POST/PUT (optional)
        max_retries: Maximum number of retry attempts
        
    Returns:
        Response object or None if all retries failed
    """
    retry_count = 0
    
    while retry_count <= max_retries:
        try:
            # Make the request
            if method.upper() == "GET":
                response = requests.get(url, headers=headers, params=params)
            elif method.upper() == "POST":
                response = requests.post(url, headers=headers, params=params, json=data)
            else:
                logger.error(f"Unsupported HTTP method: {method}")
                return None
            
            # If not a 429 error, return the response
            if response.status_code != 429:
                response.raise_for_status()  # Raise exception for other errors
                return response
                
            # Handle 429 error (Too Many Requests)
            retry_count += 1
            
            # Get retry delay from response header or use default
            retry_after = response.headers.get('Retry-After', '3')  # Default 3 seconds
            retry_delay = int(retry_after) if retry_after.isdigit() else 3
            
            logger.warning(f"Rate limit exceeded (429). Retrying in {retry_delay} seconds. Attempt {retry_count}/{max_retries}")
            time.sleep(retry_delay)
        
        except Exception as e:
            logger.error(f"Error making API request: {str(e)}")
            return None
    
    logger.error(f"Max retries ({max_retries}) exceeded for request to {url}")
    return None

def get_playlists_for_track_from_spotify_supabase(track_id=None, track_name=None, artist_names=None):
    """
    Find playlists containing a track using Supabase database for Spotify.
    Uses blue-green deployment tables and more flexible matching like Apple Music's implementation.
    
    Args:
        track_id (str, optional): The Spotify track ID
        track_name (str, optional): The name of the track
        artist_names (list, optional): List of artist names
        
    Returns:
        list: List of playlists containing the track
    """
    try:
        # Get the active table name
        table_name = get_spotify_active_table_name()
        
        # Create query using the active table
        query = spotify_supabase.table(table_name)
        
        if track_name:
            # Clean track name - lowercase
            track_name = track_name.lower()
            
            # Use ilike for case-insensitive search similar to Apple Music implementation
            data = query.select("*").ilike("song_name", f"{track_name}").execute()

                
            if not data.data:
                logger.info(f"No Spotify playlists found for {track_name}")
                return []
                
            # If artists are provided, filter by artists
            filtered_results = data.data
            
            if artist_names:
                # Process artist names
                if isinstance(artist_names, str):
                    artist_list = [a.strip().lower() for a in artist_names.split(',')]
                else:
                    artist_list = [a.lower() for a in artist_names if isinstance(a, str)]
                
                # Filter by artists
                artist_filtered = []
                for item in filtered_results:
                    db_artists = item.get("artist_names", [])
                    if not db_artists:
                        continue
                    
                    match_found = False
                    for db_artist in db_artists:
                        if not isinstance(db_artist, str):
                            continue
                            
                        db_artist_lower = db_artist.lower()
                        for search_artist in artist_list:
                            search_artist_lower = search_artist.lower()
                            # Using the same comparison logic as Apple Music
                            if db_artist_lower in search_artist_lower:
                                match_found = True
                                break
                        if match_found:
                            break


                    # This should be OUTSIDE the db_artist loop but inside the item loop
                    if match_found:
                        artist_filtered.append(item)
                        
                        
                filtered_results = artist_filtered
            
            # Format the results
            playlists = []
            seen_urls = set()
            
            # Format playlist data
            for item in filtered_results:
                playlist_url = item.get("playlist_url")
                if playlist_url and playlist_url not in seen_urls:
                    seen_urls.add(playlist_url)
                    
                    playlists.append({
                        'id': playlist_url.split('/')[-1],
                        'name': item.get("playlist_title", "Unknown Playlist"),
                        'image': item.get("playlist_image_url"),
                        'external_url': playlist_url,
                        'description': item.get("playlist_description", ""),
                        'followers': item.get("playlist_followers", 0),
                        'tracks_count': item.get("playlist_tracks_count", 0)
                    })

                
            
            logger.info(f"Found {len(playlists)} Spotify playlists for track '{track_name}'")
            if len(playlists) == 0:
                logger.info("Found songs in database but no playlists after filtering")
                # Log the songs found to help debug
                for idx, item in enumerate(filtered_results[:5]):
                    logger.info(f"Song {idx+1}: {item.get('song_name')} by {item.get('artist_names')}")
            
            return playlists
            
        elif track_id:
            # If we have track_id directly, use it for search using song_url field
            track_url = f"https://open.spotify.com/track/{track_id}"
            data = query.select("*").eq("song_url", track_url).limit(100).execute()
            
            if not data.data:
                logger.info(f"No Spotify playlists found for track ID {track_id}")
                return []
            
            playlists = []
            seen_urls = set()
            
            # Format playlist data
            for item in data.data:
                playlist_url = item.get("playlist_url")
                if playlist_url and playlist_url not in seen_urls:
                    seen_urls.add(playlist_url)
                    
                    playlists.append({
                        'id': playlist_url.split('/')[-1],
                        'name': item.get("playlist_title", "Unknown Playlist"),
                        'image': item.get("playlist_image_url"),
                        'external_url': playlist_url,
                        'description': item.get("playlist_description", ""),
                        'followers': item.get("playlist_followers", 0),
                        'tracks_count': item.get("playlist_tracks_count", 0)
                    })
            
            logger.info(f"Found {len(playlists)} Spotify playlists from {table_name} for track ID {track_id}")
            return playlists
            
        else:
            logger.error("No search criteria provided for Spotify playlist search")
            return []
            
    except Exception as e:
        logger.error(f"Error getting Spotify playlists from Supabase: {str(e)}")
        # Log the error traceback for better debugging
        import traceback
        logger.error(traceback.format_exc())
        return []
def search_spotify_track_fuzzy(track_name, artist_name=None):
    """
    Search for a Spotify track in Supabase using fuzzy matching.
    This is a fallback if the Spotify API search fails.
    Uses blue-green deployment tables.
    
    Args:
        track_name (str): The name of the track
        artist_name (str, optional): The name of the artist
        
    Returns:
        list: List of playlists containing the track
    """
    try:
        # Get the active table name
        table_name = get_spotify_active_table_name()
        
        # Clean track name - lowercase
        track_name = track_name.lower()
        
        # Create the basic query using the active table
        query = spotify_supabase.table(table_name)
        
        # Fetch a limited set and filter in Python
        data = query.select("*").limit(500).execute()
        
        if not data.data:
            logger.info(f"No Spotify tracks found for fuzzy search: {track_name}")
            return []
        
        # Filter results post-query for track name
        filtered_by_name = []
        for item in data.data:
            db_song_name = item.get("song_name", "").lower()
            if track_name in db_song_name:
                filtered_by_name.append(item)
        
        # If artist name is provided, filter the results
        filtered_results = filtered_by_name
        if artist_name:
            artist_name_lower = artist_name.lower()
            filtered_results = []
            
            for item in filtered_by_name:
                # Get artists from the database
                db_artists = item.get("artist_names", [])
                if not db_artists:
                    continue
                
                # Check if our artist matches any artist in the DB
                for db_artist in db_artists:
                    if isinstance(db_artist, str) and artist_name_lower in db_artist.lower():
                        filtered_results.append(item)
                        break
        
        playlists = []
        seen_urls = set()
        
        # Format playlist data
        for item in filtered_results:
            playlist_url = item.get("playlist_url")
            if playlist_url and playlist_url not in seen_urls:
                seen_urls.add(playlist_url)
                
                playlists.append({
                    'id': playlist_url.split('/')[-1],
                    'name': item.get("playlist_title", "Unknown Playlist"),
                    'image': item.get("playlist_image_url"),
                    'external_url': playlist_url,
                    'description': item.get("playlist_description", ""),
                    'followers': item.get("playlist_followers", 0),
                    'tracks_count': item.get("playlist_tracks_count", 0)
                })
        
        logger.info(f"Found {len(playlists)} Spotify playlists from {table_name} fuzzy search for {track_name}")
        return playlists
            
    except Exception as e:
        logger.error(f"Error in Spotify fuzzy track search: {str(e)}")
        return []

#########################################
# APPLE MUSIC FUNCTIONS
#########################################

def format_apple_music_image_url(url):
    """
    Ensure Apple Music image URLs are properly formatted.
    Apple Music image URLs from API might be in different formats.
    """
    if not url:
        return None
        
    # If already a complete URL, return as is
    if url.startswith('http'):
        return url
        
    # Add proper prefix if needed
    if not url.startswith('https://'):
        # Check if it starts with a slash
        if url.startswith('/'):
            return f"https://is1-ssl.mzstatic.com{url}"
        else:
            return f"https://is1-ssl.mzstatic.com/{url}"
            
    return url


def get_apple_active_table_name():
    """
    Determine which Apple Music table is currently active for queries.
    Checks the db_config table in Apple Music Supabase project.
    Uses caching to avoid frequent DB lookups.
    Falls back to "song_apple_green" if there's any issue.
    """
    global apple_active_table_cache
    
    now = int(time.time())
    
    # Return cached value if still valid (cache for 5 minutes)
    if apple_active_table_cache['name'] and now < apple_active_table_cache['expires_at']:
        return apple_active_table_cache['name']
    
    try:
        config = apple_supabase.table("db_config").select("*").eq("id", 2).execute()  # id: 2 for Apple Music
        
        if config.data and "active_table" in config.data[0]:
            table_name = config.data[0]["active_table"]
            
            # Update cache
            apple_active_table_cache['name'] = table_name
            apple_active_table_cache['expires_at'] = now + 300  # Cache for 5 minutes
            
            logger.info(f"Active Apple Music table is: {table_name}")
            return table_name
            
        logger.warning("No active Apple Music table found in db_config, falling back to 'song_apple_green'")
        return "song_apple_green"  # Fallback to Apple Music table
        
    except Exception as e:
        logger.error(f"Error determining active Apple Music table: {str(e)}")
        return "song_apple_green"  # Fallback to Apple Music table

def check_apple_table_switch():
    """
    Check if an Apple Music table switch has happened and clear the cache if needed.
    Checks the apple_db_updated.txt file that the updater creates.
    """
    global apple_active_table_cache
    
    try:
        # Check if apple_db_updated.txt file exists
        if os.path.exists("apple_db_updated.txt"):
            # Get file modification time
            mod_time = os.path.getmtime("apple_db_updated.txt")
            
            # If file was modified after our cache was set, invalidate cache
            if mod_time > apple_active_table_cache['expires_at'] - 300:  # 300 = cache duration
                logger.info("Detected Apple Music database update, clearing active table cache")
                apple_active_table_cache['expires_at'] = 0  # Force refresh on next request
                
                # Read the new active table from the file
                with open("apple_db_updated.txt", "r") as f:
                    for line in f:
                        if line.startswith("active_table="):
                            new_table = line.strip()[13:]  # Extract table name
                            logger.info(f"New active Apple Music table from file: {new_table}")
    except Exception as e:
        logger.error(f"Error checking for Apple Music table switch: {str(e)}")

def apple_music_request(endpoint, method="GET", params=None, data=None, max_retries=3):
    """
    Make a request to the Apple Music API with automatic token renewal on 401 errors
    and retry on 429 (Too Many Requests) errors.
    
    Args:
        endpoint (str): The API endpoint (without the base URL)
        method (str): HTTP method (GET, POST, etc.)
        params (dict, optional): URL parameters
        data (dict, optional): Request body for POST/PUT requests
        max_retries (int): Maximum number of retry attempts for rate limiting
        
    Returns:
        dict: The JSON response or None if the request failed
    """
    global APPLE_MUSIC_JWT
    
    # Initialize token if needed
    if APPLE_MUSIC_JWT is None:
        APPLE_MUSIC_JWT = generate_apple_jwt()
        if APPLE_MUSIC_JWT is None:
            logger.error("Failed to generate Apple Music JWT token")
            return None
    
    # Prepare the request
    url = f"{APPLE_MUSIC_API_BASE_URL}/{endpoint}"
    headers = {
        'Authorization': f'Bearer {APPLE_MUSIC_JWT}',
        'Content-Type': 'application/json'
    }
    
    retry_count = 0
    
    while retry_count <= max_retries:
        try:
            # Make the request
            if method == "GET":
                response = requests.get(url, headers=headers, params=params)
            elif method == "POST":
                response = requests.post(url, headers=headers, params=params, json=data)
            else:
                logger.error(f"Unsupported HTTP method: {method}")
                return None
            
            # Check for 401 (Unauthorized) - expired token
            if response.status_code == 401:
                logger.warning("Apple Music API returned 401 - token has expired, generating new token")
                
                # Generate a new token
                new_token = generate_apple_jwt()
                if new_token is None:
                    logger.error("Failed to generate new Apple Music JWT token after 401 error")
                    return None
                
                # Update the global token
                APPLE_MUSIC_JWT = new_token
                
                # Retry the request with the new token
                headers['Authorization'] = f'Bearer {APPLE_MUSIC_JWT}'
                continue  # Retry immediately with new token
            
            # Check for 429 (Too Many Requests) - rate limiting
            elif response.status_code == 429:
                retry_count += 1
                
                # Get retry delay from response header or use default
                retry_after = response.headers.get('Retry-After', '3')
                retry_delay = int(retry_after) if retry_after.isdigit() else 3
                
                logger.warning(f"Apple Music API rate limit exceeded (429). Retrying in {retry_delay} seconds. Attempt {retry_count}/{max_retries}")
                time.sleep(retry_delay)
                continue  # Retry after delay
            
            # For any other status code, raise for error or return response
            response.raise_for_status()
            return response.json()
        
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error from Apple Music API: {str(e)}")
            return None
        
        except Exception as e:
            logger.error(f"Error making Apple Music API request: {str(e)}")
            return None
    
    logger.error(f"Max retries ({max_retries}) exceeded for Apple Music API request to {endpoint}")
    return None


def search_apple_track(query):
    """
    Search for a track on Apple Music with automatic token renewal on 401 errors.
    
    Args:
        query (str): The search query
        
    Returns:
        dict: The first track result or None if no results
    """
    # Search for tracks, limit to 1 result
    params = {
        'term': query,
        'types': 'songs',
        'limit': 1
    }
    
    results = apple_music_request('catalog/us/search', params=params)
    
    if not results or not results.get('results', {}).get('songs', {}).get('data', []):
        logger.info(f"No tracks found for Apple Music query: {query}")
        return None
    
    track = results['results']['songs']['data'][0]['attributes']
    track_id = results['results']['songs']['data'][0]['id']
    
    # Get the artwork URL and format it properly
    artwork_url = None
    if 'artwork' in track:
        artwork = track['artwork']
        if 'url' in artwork:
            # The URL might contain {w} and {h} placeholders
            url_template = artwork['url']
            # Replace with actual dimensions - Apple typically has 1000x1000 artwork
            width = artwork.get('width', 1000)
            height = artwork.get('height', 1000)
            artwork_url = url_template.replace('{w}', str(width)).replace('{h}', str(height))
    
    # Format the track data similar to Spotify's format for consistency
    return {
        'id': track_id,
        'name': track['name'],
        'artists': track['artistName'],
        'album': track['albumName'],
        'image': artwork_url,
        'popularity': 0  # Apple Music doesn't have popularity score
    }

def search_apple_track_by_id(track_id):
    """
    Search for a track on Apple Music by its ID with automatic token renewal.
    """
    results = apple_music_request(f'catalog/us/songs/{track_id}')
    
    if not results or not results.get('data', []):
        logger.info(f"No track found for Apple Music ID: {track_id}")
        return None
    
    track = results['data'][0]['attributes']
    
    # Get the artwork URL
    artwork_url = None
    if 'artwork' in track:
        artwork = track['artwork']
        if 'url' in artwork:
            url_template = artwork['url']
            width = artwork.get('width', 1000)
            height = artwork.get('height', 1000)
            artwork_url = url_template.replace('{w}', str(width)).replace('{h}', str(height))
            
    return {
        'id': track_id,
        'name': track['name'],
        'artists': track['artistName'],
        'album': track['albumName'],
        'image': artwork_url
    }

def get_playlists_for_track_from_apple_supabase(track_id=None, track_name=None, artist_names=None):
    """
    Find playlists containing a track using Supabase database for Apple Music.
    Uses blue-green deployment tables and direct server-side filtering.
    
    Args:
        track_id (str, optional): The Apple Music track ID
        track_name (str, optional): The name of the track
        artist_names (list, optional): List of artist names
        
    Returns:
        list: List of playlists containing the track
    """
    try:
        # Get the active table name
        table_name = get_apple_active_table_name()
        
        # Create query using the active table
        query = apple_supabase.table(table_name)
        
        if track_name:
            # Clean track name - lowercase
            track_name = track_name.lower()
            
            # Use ilike for case-insensitive search across the entire database
            data = query.select("*").ilike("song_name", f"{track_name}").execute()

                
            
            # Execute the query without limits to search the entire database
            with open("SearchQuery.txt", "w", encoding="utf-8") as f:
                f.write(str(data))
                
                
                
            if not data.data:
                logger.info(f"No Apple Music playlists found for {track_name}")
                return []
                
            # If artist names are provided, filter by artists
            filtered_results = data.data
            
            
            if artist_names:
                # Process artist names
                if isinstance(artist_names, str):
                    artist_list = [a.strip().lower() for a in artist_names.split(',')]
                else:
                    artist_list = [a.lower() for a in artist_names if isinstance(a, str)]
                
                # Filter by artists
                artist_filtered = []
                for item in filtered_results:
                    db_artists = item.get("artist_names", [])
                    if not db_artists:
                        continue
                    
                    match_found = False
                    for db_artist in db_artists:
                        if not isinstance(db_artist, str):
                            continue
                            
                        db_artist_lower = db_artist.lower()
                        for search_artist in artist_list:
                            search_artist_lower = search_artist.lower()
                            # Reversed comparison: check if db_artist is in search_artist
                            if db_artist_lower in search_artist_lower:
                                match_found = True
                                break
                                
                        if match_found:
                            break

                    # This should be OUTSIDE the db_artist loop but inside the item loop
                    if match_found:
                        artist_filtered.append(item)
              
                filtered_results = artist_filtered
            
            # Format the results
            playlists = []
            seen_urls = set()
            
            # Format playlist data
            for item in filtered_results:
                playlist_url = item.get("playlist_url")
                if playlist_url and playlist_url not in seen_urls:
                    seen_urls.add(playlist_url)
                    
                    # Format the image URL
                    playlist_image = format_apple_music_image_url(item.get("playlist_image_url"))
                    
                    playlists.append({
                        'id': playlist_url.split('/')[-1],
                        'name': item.get("playlist_title", "Unknown Playlist"),
                        'image': playlist_image,
                        'external_url': playlist_url,
                        'description': item.get("playlist_description", ""),
                        'followers': item.get("playlist_followers", 0),
                        'tracks_count': item.get("playlist_tracks_count", 0)
                    })
            
            logger.info(f"Found {len(playlists)} Apple Music playlists for track '{track_name}'")
            if len(playlists) == 0:
                logger.info("Found songs in database but no playlists after filtering")
                # Log the songs found to help debug
                for idx, item in enumerate(filtered_results[:5]):
                    logger.info(f"Song {idx+1}: {item.get('song_name')} by {item.get('artist_names')}")
            
            return playlists
            
        elif track_id:
            # Direct track ID search
            track_url = f"https://music.apple.com/us/song/{track_id}"
            data = query.select("*").eq("song_url", track_url).execute()
            
            if not data.data:
                logger.info(f"No Apple Music playlists found for track ID {track_id}")
                return []
            
            playlists = []
            seen_urls = set()
            
            # Format playlist data
            for item in data.data:
                playlist_url = item.get("playlist_url")
                if playlist_url and playlist_url not in seen_urls:
                    seen_urls.add(playlist_url)
                    
                    # Format the image URL
                    playlist_image = format_apple_music_image_url(item.get("playlist_image_url"))
                    
                    playlists.append({
                        'id': playlist_url.split('/')[-1],
                        'name': item.get("playlist_title", "Unknown Playlist"),
                        'image': playlist_image,
                        'external_url': playlist_url,
                        'description': item.get("playlist_description", ""),
                        'followers': item.get("playlist_followers", 0),
                        'tracks_count': item.get("playlist_tracks_count", 0)
                    })
            
            logger.info(f"Found {len(playlists)} Apple Music playlists for track ID {track_id}")
            return playlists
            
        else:
            logger.error("No search criteria provided for Apple Music playlist search")
            return []
            
    except Exception as e:
        logger.error(f"Error getting Apple Music playlists from Supabase: {str(e)}")
        # Log the error traceback for better debugging
        import traceback
        logger.error(traceback.format_exc())
        return []

def search_apple_track_fuzzy(track_name, artist_name=None):
    """
    Search for an Apple Music track in Supabase using fuzzy matching.
    This is a fallback if the Apple Music API search fails.
    
    Args:
        track_name (str): The name of the track
        artist_name (str, optional): The name of the artist
        
    Returns:
        tuple: (list of playlists, basic track info dict)
    """
    try:
        # Get the active table name
        table_name = get_apple_active_table_name()
        
        # Clean track name - lowercase
        track_name = track_name.lower()
        
        # Create the basic query using the active table
        query = apple_supabase.table(table_name)
        
        # Fetch a limited set and filter in Python
        data = query.select("*").limit(500).execute()
        
        if not data.data:
            logger.info(f"No Apple Music tracks found for fuzzy search: {track_name}")
            return [], None
        
        # Filter results post-query for track name
        filtered_by_name = []
        for item in data.data:
            db_song_name = item.get("song_name", "").lower()
            if track_name in db_song_name:
                filtered_by_name.append(item)
        
        # If artist name is provided, filter the results
        filtered_results = filtered_by_name
        if artist_name:
            artist_name_lower = artist_name.lower()
            filtered_results = []
            
            for item in filtered_by_name:
                # Get artists from the database
                db_artists = item.get("artist_names", [])
                if not db_artists:
                    continue
                
                # Check if our artist matches any artist in the DB
                for db_artist in db_artists:
                    if isinstance(db_artist, str) and artist_name_lower in db_artist.lower():
                        filtered_results.append(item)
                        break
        
        playlists = []
        seen_urls = set()
        track_image = None
        
        # Format playlist data
        for item in filtered_results:
            playlist_url = item.get("playlist_url")
            
            # Try to get the song image from the first match
            if not track_image and item.get("song_image_url"):
                track_image = format_apple_music_image_url(item.get("song_image_url"))
            
            if playlist_url and playlist_url not in seen_urls:
                seen_urls.add(playlist_url)
                
                playlists.append({
                    'id': playlist_url.split('/')[-1],
                    'name': item.get("playlist_title", "Unknown Playlist"),
                    'image': format_apple_music_image_url(item.get("playlist_image_url")),
                    'external_url': playlist_url,
                    'description': item.get("playlist_description", ""),
                    'followers': item.get("playlist_followers", 0),
                    'tracks_count': item.get("playlist_tracks_count", 0)
                })
        
        logger.info(f"Found {len(playlists)} Apple Music playlists from {table_name} fuzzy search for {track_name}")
        
        # Create basic track info with image if available
        basic_track = {
            'name': track_name,
            'artists': artist_name or "",
            'album': "",
            'image': track_image,
            'playlist_count': len(playlists)
        }
        
        return playlists, basic_track
            
    except Exception as e:
        logger.error(f"Error in Apple Music fuzzy track search: {str(e)}")
        return [], None

#########################################
# ROUTES
#########################################

@app.route('/')
def index():
    """Serve the main HTML page"""
    # Check for table switches before responding
    check_spotify_table_switch()
    check_apple_table_switch()
    return render_template('index.html')

@app.route('/download_csv/<service>/<track_id>')
def download_csv(service, track_id):
    """
    Generate and download a CSV file for a track's playlists.
    
    Args:
        service (str): 'spotify' or 'apple'
        track_id (str): The track ID
    """
    try:
        if service == 'spotify':
            track = search_spotify_track_by_id(track_id)
            if track:
                playlists = get_playlists_for_track_from_spotify_supabase(track_id=track_id)
            else:
                return jsonify({'error': 'Track not found'}), 404
        elif service == 'apple':
            track = search_apple_track_by_id(track_id)
            if track:
                playlists = get_playlists_for_track_from_apple_supabase(track_id=track_id)
            else:
                return jsonify({'error': 'Track not found'}), 404
        else:
            return jsonify({'error': 'Invalid service'}), 400
            
        # Generate CSV content
        csv_content = 'Song Name,Playlist Name,Playlist URL\n'
        for playlist in playlists:
            csv_content += f'"{track["name"].replace(","," ")}","{playlist["name"].replace(","," ")}","{playlist["external_url"]}"\n'
            
        # Set up response
        response = app.response_class(
            response=csv_content,
            mimetype='text/csv',
            headers={'Content-Disposition': f'attachment;filename={service}_{track_id}.csv'}
        )
        return response
            
    except Exception as e:
        logger.error(f"Error generating CSV: {str(e)}")
        return jsonify({'error': 'Failed to generate CSV'}), 500

@app.route('/search', methods=['POST'])
def search():
    """
    Search endpoint that finds a track on Spotify and returns playlists containing it.
    
    Expected JSON input: {'query': 'track name'}
    """
    try:
        # Check for table switch before processing search
        check_spotify_table_switch()
        
        data = request.json
        query = data.get('query', '')
        
        if not query:
            return jsonify({'error': 'No search query provided'}), 400
        
        # Search for the track using Spotify API
        track = search_spotify_track(query)
        
        if not track:
            # If track not found on Spotify, try fuzzy search in our database
            playlists = search_spotify_track_fuzzy(query)
            
            if playlists:
                # Create a basic track object
                track = {
                    'id': None,
                    'name': query,
                    'artists': "",
                    'album': "",
                    'image': None,
                    'popularity': 0,
                    'playlist_count': len(playlists)
                }
                
                return jsonify({
                    'track': track,
                    'playlists': playlists
                })
            else:
                return jsonify({'error': 'Track not found on Spotify'}), 404
        
        playlists = get_playlists_for_track_from_spotify_supabase(
            track_id=track['id'],
            track_name=track['name'],
            artist_names=track['artists']
        )
        
        # Add playlist count to track info
        track['playlist_count'] = len(playlists)
        
        return jsonify({
            'track': track,
            'playlists': playlists
        })
    
    except Exception as e:
        logger.error(f"Error in Spotify search endpoint: {str(e)}")
        return jsonify({'error': 'An error occurred during the Spotify search'}), 500

@app.route('/search_apple', methods=['POST'])
def search_apple():
    """
    Search endpoint that finds a track on Apple Music and returns playlists containing it.
    Uses automatic token renewal when token expires (401 errors).
    
    Expected JSON input: {'query': 'track name'}
    """
    try:
        # Check for table switch before processing search
        check_apple_table_switch()
        
        data = request.json
        query = data.get('query', '')
        
        if not query:
            return jsonify({'error': 'No search query provided'}), 400
        
        # Search for the track using Apple Music API with automatic token renewal
        track = search_apple_track(query)
        
        if not track:
            # If track not found on Apple Music, try fuzzy search in our database
            playlists, basic_track = search_apple_track_fuzzy(query)
            
            if playlists:
                return jsonify({
                    'track': basic_track,
                    'playlists': playlists
                })
            else:
                return jsonify({'error': 'Track not found on Apple Music'}), 404
        
        # Get playlists containing the track using Supabase
        playlists = get_playlists_for_track_from_apple_supabase(
            track_id=track['id'],
            track_name=track['name'],
            artist_names=track['artists']
        )
        
        # Add playlist count to track info
        track['playlist_count'] = len(playlists)
        
        return jsonify({
            'track': track,
            'playlists': playlists
        })
    
    except Exception as e:
        logger.error(f"Error in Apple Music search endpoint: {str(e)}")
        # Don't expose error details to the user
        return jsonify({'error': 'An error occurred during the Apple Music search'}), 500

@app.errorhandler(404)
def not_found(e):
    return jsonify({'error': 'Not found'}), 404

@app.errorhandler(500)
def server_error(e):
    return jsonify({'error': 'Server error'}), 500

# Update the HTML file to include the Apple Music functionality
@app.route('/update_html', methods=['GET'])
def update_html():
    """Update the HTML file to enable Apple Music search"""
    try:
        # This would update the HTML with the Apple Music functionality
        # In a real-world scenario, you'd likely have a template system
        # For now, we'll just serve the existing index.html
        return render_template('index.html')
    except Exception as e:
        logger.error(f"Error updating HTML: {str(e)}")
        return jsonify({'error': 'Failed to update HTML'}), 500

# Initialize Apple JWT on startup
try:
    # Generate initial token
    logger.info("Initializing Apple Music JWT token")
    APPLE_MUSIC_JWT = generate_apple_jwt()
    if APPLE_MUSIC_JWT:
        logger.info("Apple Music JWT token initialized successfully")
    else:
        logger.warning("Failed to initialize Apple Music JWT token, will try on first request")
except Exception as e:
    logger.error(f"Error during Apple Music JWT initialization: {str(e)}")

if __name__ == '__main__':
    app.run(debug=True)