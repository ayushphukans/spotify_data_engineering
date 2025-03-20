USE DATABASE spotify_db;
USE SCHEMA spotify_db.bronze;

-- Create raw tables for tracks and artists
CREATE OR REPLACE TABLE raw_spotify_tracks (
    data VARIANT
);

CREATE OR REPLACE TABLE raw_spotify_artists (
    data VARIANT
);