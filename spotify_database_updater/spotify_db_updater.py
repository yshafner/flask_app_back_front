import time
import os
import sys
import datetime
import argparse
import atexit
import signal
from typing import Callable, Optional, Union
from playwright.sync_api import sync_playwright
import requests
import base64
from bs4 import BeautifulSoup
import warnings
import json
import math
from supabase import create_client
warnings.filterwarnings("ignore")

class tester:
    def __init__(self):
        with open("404_playlist.txt", "w") as f:
            pass
        
        with open("non_region_playlist.txt", "w") as f:
            pass
        
        with open("error.txt", "w") as f:
            pass
    
        with open("spotify_tracks.json", "w") as f:
            pass
    
    def set_up(self):
        playwright = sync_playwright().start()
        browser_options = {
            "headless": True,
        }
        self.browser = playwright.chromium.launch(**browser_options)
        self.context = self.browser.new_context(viewport={"width": 1920, "height": 1080})
        self.page = self.context.new_page()
   
    def get_playlist_image_url(self, playlist_id, client_id, client_secret):
        credentials = f"{client_id}:{client_secret}"
        encoded_credentials = base64.b64encode(credentials.encode()).decode()
        
        token_url = "https://accounts.spotify.com/api/token"
        headers = {
            "Authorization": f"Basic {encoded_credentials}",
            "Content-Type": "application/x-www-form-urlencoded"
        }
        data = {"grant_type": "client_credentials"}
        
        token_response = requests.post(token_url, headers=headers, data=data)
        if token_response.status_code != 200:
            print(f"Error getting token: {token_response.status_code}")
            return None
        
        access_token = token_response.json()["access_token"]
        
        image_url = f"https://api.spotify.com/v1/playlists/{playlist_id}/images"
        headers = {"Authorization": f"Bearer {access_token}"}
        
        max_retries = 3
        retries = 0
        
        while retries < max_retries:
            response = requests.get(image_url, headers=headers)
            
            if response.status_code == 200:
                images = response.json()
                if images and len(images) > 0:
                    return images[0]['url']
            elif response.status_code == 429:
                print(f"Rate limited (429). Waiting 30 seconds before retry...")
                time.sleep(30)
                retries += 1
                continue
            elif response.status_code == 404:
                return None
            
            else:
                print(f"Error: {response.status_code}")
                return None
        
        print(f"Failed after {max_retries} retries")
        return None

   
    def Handler(self):
        with open("spotify_playlist_urls.txt", "r") as f:
            list_of_playlist = f.readlines()
            list_of_playlist = [x.strip() for x in list_of_playlist]

        print("Total Playlist: ", len(list_of_playlist))
        i = 1
        for url in list_of_playlist:
            print(f"Playlist {i}/{len(list_of_playlist)} : {url}")
            self.fetch_Songs(url)
            i += 1
        
        # Close browser when done
        self.browser.close()
        
    def fetch_Songs(self, url):
        try:
            self.page.goto(url, wait_until="networkidle", timeout=120000)
        except:
            # refresh the page
            self.page.goto(url, wait_until="networkidle", timeout=120000)
            
        playlist_id = url.split("/")[-1]
        client_id = "3c48bbfabfb84f25bec77c44f8c59a7d"
        client_secret = "4f822f0fb0f449e6965c2daaa28878f7"
        
        image_url = self.get_playlist_image_url(playlist_id, client_id, client_secret)
        if image_url == None:
            print("404 Playlist")
            with open("404_playlist.txt", "a") as f:
                f.write(url + "\n")
                return
        
        if image_url:
            print(f"Playlist image URL: {image_url}")
        else:
            print("Failed to get image URL")
            input("Failed to get image URL")
            
        
        retries = 0
        max_retries = 10
        while retries < max_retries:
            try:
                # Wait for the song count element to appear
                temp = self.page.wait_for_selector('//span[@class="e-9640-text encore-text-body-small encore-internal-color-text-subdued w1TBi3o5CTM7zW1EB3Bm"]', timeout=10000)
                break  # If successful, break out of the retry loop
            except:
                retries += 1
                print(f"Retry attempt {retries}/{max_retries} for fetching song information")
                if retries < max_retries:
                    print("Refreshing page and trying again...")
                    self.page.reload()
                    time.sleep(120)  # Wait a bit after refresh
                else:
                    # All retries failed, save screenshot and prompt user
                    self.page.screenshot(path="screenshot.png")
                    print("Error in fetching songs after multiple retries")
                    with open("error.txt", "a") as f:
                        f.write(url + "\n")
                        
                    return
                    
        # Get the HTML content and parse with BeautifulSoup
        content = self.page.content()
        soup = BeautifulSoup(content, "html.parser")
        playlist_title = soup.find("h1").text.lstrip().rstrip()
        
        # screenshot
        # Using the same selector as before to get song count
        try:
            temp_elements = soup.select('span.e-9640-text.encore-text-body-small.encore-internal-color-text-subdued.w1TBi3o5CTM7zW1EB3Bm')
            total_songs = int(temp_elements[1].text.replace(" ", "").replace(",", "").replace("songs", "").replace("song", "").lstrip().rstrip())
        
        except Exception as e:
            print("NON-Region Playlist", e)
            with open("non_region_playlist.txt", "a") as f:
                f.write(url + "\n")
                return
        print("Total Songs: ", total_songs)
        
        songs_url = []
        songs_name = []
        artist_name = []
        song_count = 0
        song_retry_c = 0
        song_sk = 0
        ck = 0
        while song_count < total_songs:
            # Wait for tracklist rows to appear
            self.page.wait_for_selector('div[data-testid="tracklist-row"]', timeout=10000)
            
            # Get updated content after scrolling
            content = self.page.content()
            soup = BeautifulSoup(content, "html.parser")
            songs = soup.find_all("div", {"data-testid": "tracklist-row"})
            
            for song in songs:
                tmp = song.find("a", {"data-testid": "internal-track-link"})
                tmp_url = tmp["href"]
                if tmp_url not in songs_url:
                    songs_url.append(tmp_url)
                    songs_name.append(tmp.text)
                    tmp2 = song.find("span", {"class": "e-9640-text encore-text-body-small encore-internal-color-text-subdued UudGCx16EmBkuFPllvss standalone-ellipsis-one-line"})
                    tmp2 = tmp2.find_all("a")
                    tmp_artists = []
                    for artist in tmp2:
                        tmp_artists.append(artist.text)
                    artist_name.append(tmp_artists)
                    song_count += 1
            
            # Find all tracklist rows for scrolling
            rows = self.page.query_selector_all('div[data-testid="tracklist-row"]')
            
            if rows:
                if song_sk == song_count:
                    if song_retry_c >= 30:
                        print("Loading More Songs with 25 sec wait")
                        time.sleep(25)
                        
                        #self.page.goto(url, wait_until="networkidle", timeout=120000)
                        try:
                            temp = self.page.wait_for_selector('//span[@class="e-9640-text encore-text-body-small encore-internal-color-text-subdued w1TBi3o5CTM7zW1EB3Bm"]', timeout=10000)
                            temp_elements = soup.select('span.e-9640-text.encore-text-body-small.encore-internal-color-text-subdued.w1TBi3o5CTM7zW1EB3Bm')
                            total_songs = int(temp_elements[1].text.replace(" ", "").replace(",", "").replace("songs", "").replace("song", "").lstrip().rstrip())

                        except:
                            self.page.screenshot(path="screenshot.png")
                            temp = self.page.wait_for_selector('//span[@class="e-9640-text encore-text-body-small encore-internal-color-text-subdued w1TBi3o5CTM7zW1EB3Bm"]', timeout=10000)
                            temp_elements = soup.select('span.e-9640-text.encore-text-body-small.encore-internal-color-text-subdued.w1TBi3o5CTM7zW1EB3Bm')

                            total_songs = int(temp_elements[1].text.replace(" ", "").replace(",", "").replace("songs", "").replace("song", "").lstrip().rstrip())

                        song_retry_c = 0
                        # screenshot
                    else:
                        song_retry_c += 1
                        self.page.evaluate('el => el.scrollIntoView({behavior: "smooth", block: "center"})', rows[-1])       
            
                else:
                    self.page.evaluate('el => el.scrollIntoView({behavior: "smooth", block: "center"})', rows[-1])       

            
            else:
                break
            
            song_sk = song_count
            print(f"Song count: {song_count}/{total_songs}")
        
        if song_count != total_songs:
            input("Some songs are missing")    
        else:
            with open("done.txt", "w") as f:
                f.write(url + "\n")
                
            self.Save_Database(songs_name, artist_name, songs_url, url, playlist_title, image_url)
       
       
    def Save_Database(self, songs_name, artist_name, songs_url, playlist_url, playlist_title, image_url):
        data = []
        filename = "spotify_tracks.json"  # Changed to a fixed name "tracks.json"
    
        # Check if file exists and load existing data
        if os.path.exists(filename):
            try:
                with open(filename, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except json.JSONDecodeError:
                # If file exists but is not valid JSON, start with empty list
                data = []
    
        # Add new songs
        for i in range(len(songs_name)):
            song_data = {
                "song_name": songs_name[i],
                "artist_name": artist_name[i],
                "song_url": "https://open.spotify.com" + songs_url[i],
                "playlist_url": playlist_url,
                "playlist_title": playlist_title,
                "playlist_image_url": image_url
            }
            data.append(song_data)
    
        # Save back to the JSON file
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)



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


def update_database():
    print(f"Starting Spotify database update at {time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Load your JSON data
    with open('spotify_tracks.json', 'r', encoding='utf-8') as f:
        songs_data = json.load(f)
    
    # Initialize Supabase client for Spotify
    spotify_url = "https://cdcpztcchbrddkuqrtlq.supabase.co"
    spotify_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImNkY3B6dGNjaGJyZGRrdXFydGxxIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc0MTU3OTgwOCwiZXhwIjoyMDU3MTU1ODA4fQ.CciL70S5vvGgmIEIYTp9tGBur6CbrnuBaxq7PoiAvjU"
    spotify_supabase = create_client(spotify_url, spotify_key)
    
    # Check which table is currently active
    try:
        config = spotify_supabase.table("db_config").select("*").eq("id", 1).execute()
        if not config.data:
            # If no config data, create it (fallback)
            spotify_supabase.table("db_config").insert({"id": 1, "active_table": "song_spotify_green"}).execute()
            active_table = "song_spotify_green"
        else:
            active_table = config.data[0]["active_table"]
        
        # Determine which table to update
        inactive_table = "song_spotify_blue" if active_table == "song_spotify_green" else "song_spotify_green"
        
        print(f"Current active Spotify table: {active_table}")
        print(f"Will update inactive Spotify table: {inactive_table}")
        
        # Clear the inactive table in batches instead of all at once
        clear_table_in_batches(spotify_supabase, inactive_table)
        
        # Process in batches
        batch_size = 1000
        total_songs = len(songs_data)
        total_batches = math.ceil(total_songs / batch_size)
        
        print(f"Starting upload of {total_songs} Spotify songs in {total_batches} batches to {inactive_table}...")
        
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
                response = spotify_supabase.table(inactive_table).insert(formatted_batch).execute()
                
                songs_uploaded = i + len(batch)
                percent_complete = (songs_uploaded / total_songs) * 100
                
                print(f"Batch {current_batch}/{total_batches} complete: {songs_uploaded}/{total_songs} songs uploaded ({percent_complete:.1f}%)")
            except Exception as e:
                print(f"Error inserting batch {current_batch}: {e}")
                # Continue with next batch
        
        # Now switch the active table
        print(f"Switching active Spotify table from {active_table} to {inactive_table}...")
        spotify_supabase.table("db_config").update({"active_table": inactive_table}).eq("id", 1).execute()
        
        # Create a status file that our Flask app can check
        with open("db_updated.txt", "w") as f:
            f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"active_table={inactive_table}\n")
        
        print(f"Spotify database update complete. Active table is now: {inactive_table}")
        
    except Exception as e:
        print(f"Error in Spotify database update: {e}")
        print("Update failed. Please check logs and try again.")
    
    print(f"Spotify upload process finished at {time.strftime('%Y-%m-%d %H:%M:%S')}")


def run_main_task():
    """Function that runs your main code"""
    print("Running task at:", datetime.datetime.now())
    sys.stdout.flush()
    
    test = tester()
    test.set_up()
    test.Handler()
    
    json_file_path = "spotify_tracks.json"
    if os.path.exists(json_file_path):
        update_database()
        print("Database updated successfully...")
    else:
        print("Data file not found")
    
    # Add a small delay to ensure output is visible before console clear
    time.sleep(3)
    print("\nTask execution completed. Console will be cleared in 3 seconds...")
    sys.stdout.flush()
    time.sleep(3)






if __name__ == "__main__":

    test = tester()
    test.set_up()
    test.Handler()
    
    json_file_path = "spotify_tracks.json"
    if os.path.exists(json_file_path):
        update_database()
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