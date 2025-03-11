# Playlist Search Application

A web application that allows users to search for songs and find playlists containing those songs across both Spotify and Apple Music platforms.

## Features

- Search for songs on Spotify and Apple Music
- Find playlists containing specific songs
- Download playlist data as CSV
- Real-time search results
- Blue-green deployment support for database updates

## Prerequisites

- Python 3.7+
- Flask
- Supabase account and API keys
- Spotify Developer account and API credentials
- Apple Music Developer account and credentials

## Installation

1. Clone the repository:
```bash
git clone <your-repository-url>
cd flask_app_back_front
```

2. Create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows use: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Create a `.env` file with your credentials:
```
SPOTIFY_CLIENT_ID=your_spotify_client_id
SPOTIFY_CLIENT_SECRET=your_spotify_client_secret
SPOTIFY_SUPABASE_URL=your_spotify_supabase_url
SPOTIFY_SUPABASE_KEY=your_spotify_supabase_key
APPLE_SUPABASE_URL=your_apple_supabase_url
APPLE_SUPABASE_KEY=your_apple_supabase_key
```

5. Place your Apple Music private key (`auth.p8`) in the project root directory

## Running the Application

```bash
python flask_app.py
```

The application will be available at `http://localhost:5000`

## API Endpoints

- `GET /`: Main application page
- `POST /search`: Search for tracks on Spotify
- `POST /search_apple`: Search for tracks on Apple Music
- `GET /download_csv/<service>/<track_id>`: Download playlist data as CSV

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details 