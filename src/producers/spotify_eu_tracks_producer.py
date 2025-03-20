import os
import json
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from kafka import KafkaProducer
from dotenv import load_dotenv

def produce_eu_tracks(countries=None):
    load_dotenv()
    if countries is None:
        # List of European country codes to include
        countries = ["DE", "FR", "ES", "CH", "SE", "AT", "DK", "NL", "BE"]

    sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
        client_id=os.getenv("SPOTIFY_CLIENT_ID"),
        client_secret=os.getenv("SPOTIFY_CLIENT_SECRET"),
        redirect_uri=os.getenv("SPOTIFY_REDIRECT_URI"),
        scope="playlist-read-private playlist-read-collaborative"
    ))

    producer = KafkaProducer(bootstrap_servers="localhost:9092")
    total_messages = 0

    for country in countries:
        print(f"\nFetching new releases for country {country}...")
        try:
            # Use the new releases endpoint to get albums
            new_releases = sp.new_releases(country=country, limit=20, offset=0)
        except Exception as e:
            print(f"Error fetching new releases for {country}: {e}")
            continue

        albums = new_releases.get("albums", {}).get("items", [])
        print(f"Found {len(albums)} new release albums in {country}.")
        for album in albums:
            album_id = album.get("id")
            if not album_id:
                continue
            try:
                # Get full album details (including tracks)
                album_details = sp.album(album_id, market=country)
                tracks = album_details.get("tracks", {}).get("items", [])
                print(f"Album '{album.get('name')}' in {country} has {len(tracks)} tracks.")
                
                # Retrieve full track details (including popularity)
                track_ids = [track.get("id") for track in tracks if track.get("id")]
                if track_ids:
                    full_tracks_response = sp.tracks(track_ids)
                    full_tracks = full_tracks_response.get("tracks", [])
                    for full_track in full_tracks:
                        message_data = {
                            "country": country,
                            "album_id": album_id,
                            "album_name": album.get("name"),
                            "track_id": full_track.get("id"),
                            "track_name": full_track.get("name"),
                            "artist_ids": [artist.get("id") for artist in full_track.get("artists", [])],
                            "artist_names": [artist.get("name") for artist in full_track.get("artists", [])],
                            "popularity": full_track.get("popularity")
                        }
                        producer.send("spotify_eu_tracks", json.dumps(message_data).encode("utf-8"))
                        total_messages += 1
                else:
                    print(f"No valid track IDs found for album {album.get('name')} in {country}.")

            except Exception as e:
                print(f"Error processing album {album_id} for {country}: {e}")

    producer.flush()
    print(f"\nProduced {total_messages} track messages to 'spotify_eu_tracks' topic.")

if __name__ == "__main__":
    produce_eu_tracks()
