USE DATABASE spotify_db;
USE SCHEMA spotify_db.bronze;

CREATE STORAGE INTEGRATION IF NOT EXISTS spotify_integration
    TYPE= EXTERNAL_STAGE
    ENABLED=TRUE
    STORAGE_PROVIDER=S3
    STORAGE_ALLOWED_LOCATIONS= ('s3://my-spotify-bronze-bucket/spotify/')
    STORAGE_AWS_ROLE_ARN='arn:aws:iam::148761668347:role/snowflake_to_spotifys3_access';


