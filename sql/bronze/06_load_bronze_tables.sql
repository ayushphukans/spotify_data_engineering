USE DATABASE spotify_db;
USE SCHEMA spotify_db.bronze;

-- Load raw Spotify tracks from S3
COPY INTO raw_spotify_tracks
FROM @spotify_stage/bronze/eu_tracks/
FILE_FORMAT = (TYPE = JSON STRIP_OUTER_ARRAY = TRUE)
ON_ERROR = 'CONTINUE';

-- Load raw Spotify artists from S3
COPY INTO raw_spotify_artists
FROM @spotify_stage/bronze/eu_artists/
FILE_FORMAT = (TYPE = JSON STRIP_OUTER_ARRAY = TRUE)
ON_ERROR = 'CONTINUE';