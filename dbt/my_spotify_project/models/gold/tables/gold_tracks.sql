{{ config(materialized='table') }}

SELECT DISTINCT
    track_id,
    track_name,
    track_popularity,
    album_id
FROM {{ source('bronze', 'tracks') }} 