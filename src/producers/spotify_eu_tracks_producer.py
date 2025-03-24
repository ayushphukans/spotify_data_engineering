import os
import json
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from kafka import KafkaProducer
from dotenv import load_dotenv

def produce_eu_tracks(countries=None):
    load_dotenv()
    if countries is None:
        countries = ["DE", "FR", "ES", "CH", "SE", "AT", "DK", "NL", "BE"]

    # Switch to SpotifyClientCredentials
    sp = spotipy.Spotify(
        auth_manager=SpotifyClientCredentials(
            client_id=os.getenv("SPOTIFY_CLIENT_ID"),
            client_secret=os.getenv("SPOTIFY_CLIENT_SECRET")
        )
    )

    producer = KafkaProducer(bootstrap_servers="kafka1:9092")
    all_track_ids = set()

    for country in countries:
        print(f"\nFetching new releases for country {country}...")
        try:
            new_releases = sp.new_releases(country=country, limit=20, offset=0)
        except Exception as e:
            print(f"Error fetching new releases for {country}: {e}")
            continue

        albums = new_releases.get("albums", {}).get("items", [])
        for album in albums:
            album_id = album.get("id")
            if not album_id:
                continue
            try:
                album_details = sp.album(album_id, market=country)
                tracks = album_details.get("tracks", {}).get("items", [])
                for track in tracks:
                    if track.get("id"):
                        all_track_ids.add(track["id"])
            except Exception as e:
                print(f"Error processing album {album_id} for {country}: {e}")

    track_ids = list(all_track_ids)
    print(f"\nFound {len(track_ids)} unique track IDs across selected countries.")

    def chunks(lst, n):
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    count = 0
    for chunk in chunks(track_ids, 50):
        try:
            tracks_info = sp.tracks(chunk)
            for track in tracks_info.get("tracks", []):
                producer.send("spotify_eu_tracks", json.dumps(track).encode("utf-8"))
                count += 1
        except Exception as e:
            print(f"Error fetching track info for chunk: {e}")

    producer.flush()
    print(f"\nProduced track info for {count} tracks to 'spotify_eu_tracks' topic.")

if __name__ == "__main__":
    produce_eu_tracks()