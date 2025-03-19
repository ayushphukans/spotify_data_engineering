import os
import json
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from kafka import KafkaProducer
from dotenv import load_dotenv

def produce_eu_artists(countries=None):
    load_dotenv()
    if countries is None:
        countries = ["DE", "FR", "ES", "CH", "SE", "AT", "DK", "NL", "BE"]

    sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
        client_id=os.getenv("SPOTIFY_CLIENT_ID"),
        client_secret=os.getenv("SPOTIFY_CLIENT_SECRET"),
        redirect_uri=os.getenv("SPOTIFY_REDIRECT_URI"),
        scope="playlist-read-private playlist-read-collaborative"
    ))

    producer = KafkaProducer(bootstrap_servers="localhost:9092")
    all_artist_ids = set()

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
                    for artist in track.get("artists", []):
                        if artist.get("id"):
                            all_artist_ids.add(artist.get("id"))
            except Exception as e:
                print(f"Error processing album {album_id} for {country}: {e}")

    artist_ids = list(all_artist_ids)
    print(f"\nFound {len(artist_ids)} unique artist IDs across selected countries.")

    def chunks(lst, n):
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    count = 0
    for chunk in chunks(artist_ids, 50):
        try:
            artists_info = sp.artists(chunk)
            for artist in artists_info.get("artists", []):
                producer.send("spotify_eu_artists", json.dumps(artist).encode("utf-8"))
                count += 1
        except Exception as e:
            print(f"Error fetching artist info for chunk: {e}")

    producer.flush()
    print(f"\nProduced artist info for {count} artists to 'spotify_eu_artists' topic.")

if __name__ == "__main__":
    produce_eu_artists()
