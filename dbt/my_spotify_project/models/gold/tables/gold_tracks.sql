{{ config(materialized='table') }}

SELECT DISTINCT
    track_id,
    track_name,
    popularity AS track_popularity,
    album_id
FROM {{ source('bronze', 'tracks') }}