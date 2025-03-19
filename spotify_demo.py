import os
from dotenv import load_dotenv
import spotipy
from spotipy.oauth2 import SpotifyOAuth

# Load variables from .env
load_dotenv()

def main():
    # Retrieve environment variables
    CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
    CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")
    REDIRECT_URI = os.getenv("SPOTIFY_REDIRECT_URI")
    
    # Confirm they loaded
    if not all([CLIENT_ID, CLIENT_SECRET, REDIRECT_URI]):
        raise ValueError("Missing Spotify API credentials in .env file.")
    
    # Define scope (adjust if needed)
    SCOPE = "user-read-private,user-read-email"
    
    # Create auth manager
    auth_manager = SpotifyOAuth(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        redirect_uri=REDIRECT_URI,
        scope=SCOPE,
        show_dialog=True
    )
    # Spotipy client
    sp = spotipy.Spotify(auth_manager=auth_manager)
    
    # Test calls
    user_info = sp.current_user()
    print("=== Current User Profile ===")
    print("Display Name:", user_info.get('display_name'))
    print("User ID:", user_info.get('id'))
    print("Email:", user_info.get('email'))
    print("Country:", user_info.get('country'))
    
    print("\n=== Searching for 'Daft Punk' (track) ===")
    results = sp.search(q="Daft Punk", limit=5, type="track")
    for idx, item in enumerate(results['tracks']['items'], start=1):
        track_name = item['name']
        artist_name = item['artists'][0]['name']
        print(f"{idx}. {track_name} - {artist_name}")

if __name__ == "__main__":
    main()
