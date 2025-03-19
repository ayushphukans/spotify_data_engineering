USE DATABASE spotify_db;
USE SCHEMA spotify_db.bronze;

CREATE STAGE IF NOT EXISTS spotify_stage
URL='s3://my-spotify-bronze-bucket/spotify/'
STORAGE_INTEGRATION= spotify_integration
FILE_FORMAT=(TYPE=JSON STRIP_OUTER_ARRAY=TRUE);