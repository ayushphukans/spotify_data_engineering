-- =============================================
-- File: 01_create_gold_schema_and_tables.sql
-- =============================================

-- 1) Switch to the correct database (adjust if needed)
USE DATABASE spotify_db;

-- 2) Create the gold schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS spotify_db.gold;

-- 3) Switch to the gold schema
USE SCHEMA spotify_db.gold;

-- =============================================
-- Star Schema Tables
-- =============================================

-- DIMENSION: Artists
DROP TABLE IF EXISTS dim_artists;
CREATE TABLE dim_artists AS
SELECT DISTINCT
    artist_id,
    artist_name,
    genres,                -- Keep as VARIANT if you wish to parse further later
    popularity  AS artist_popularity,
    followers   AS artist_followers
FROM spotify_db.bronze.artists;

-- DIMENSION: Albums (optional, if you want separate album dimension)
DROP TABLE IF EXISTS dim_albums;
CREATE TABLE dim_albums AS
SELECT DISTINCT
    album_id,
    album_name
FROM spotify_db.bronze.tracks
WHERE album_id IS NOT NULL;

-- FACT: Tracks
-- (Typically you'd have metrics like play_count, stream_date, etc.)
DROP TABLE IF EXISTS fact_tracks;
CREATE TABLE fact_tracks AS
SELECT
    track_id,
    track_name,
    popularity AS track_popularity,
    country,
    album_id
FROM spotify_db.bronze.tracks;

-- BRIDGE TABLE (Tracks ↔ Artists) for the many-to-many relationship
-- Because a track can have multiple artists
DROP TABLE IF EXISTS bridge_track_artists;
CREATE TABLE bridge_track_artists AS
SELECT
    t.track_id,
    f.value::STRING AS artist_id
FROM spotify_db.bronze.tracks t,
     LATERAL FLATTEN(INPUT => t.artist_ids) f;

-- =============================================
-- End of script
-- =============================================
