-- 07_create_curated_bronze_tables.sql
-- These "curated" tables are still in the bronze layer but are flattened out of the raw VARIANT data.

USE DATABASE spotify_db;
USE SCHEMA spotify_db.bronze;

-- Flatten the raw tracks JSON data into a tabular format
DROP TABLE IF EXISTS tracks;
CREATE TABLE tracks AS
SELECT
    data:album_id::VARCHAR    AS album_id,
    data:album_name::VARCHAR  AS album_name,
    data:artist_ids           AS artist_ids,   -- still variant/array if needed
    data:artist_names         AS artist_names,
    data:country::VARCHAR     AS country,
    data:popularity::NUMBER   AS popularity,
    data:track_id::VARCHAR    AS track_id,
    data:track_name::VARCHAR  AS track_name
FROM raw_spotify_tracks;

-- Flatten the raw artists JSON data into a tabular format
DROP TABLE IF EXISTS artists;
CREATE TABLE artists AS
SELECT
    data:artist_id::VARCHAR   AS artist_id,
    data:artist_name::VARCHAR AS artist_name,
    data:genres               AS genres,       -- keep as VARIANT/array
    data:popularity::NUMBER   AS popularity,
    data:followers.total::NUMBER AS followers
FROM raw_spotify_artists;
