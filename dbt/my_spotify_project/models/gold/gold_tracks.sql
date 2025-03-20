{{ config(materialized='view') }}

SELECT
    track_id,
    track_name,
    popularity AS track_popularity,
    country,
    album_id
FROM {{ source('bronze', 'tracks') }}