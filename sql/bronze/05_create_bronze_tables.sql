USE DATABASE spotify_db;
USE SCHEMA spotify_db.bronze;

-- 1) For tracks
CREATE OR REPLACE TABLE raw_spotify_tracks (
    data VARIANT
);

-- 2) For artists
CREATE OR REPLACE TABLE raw_spotify_artists (
    data VARIANT
);