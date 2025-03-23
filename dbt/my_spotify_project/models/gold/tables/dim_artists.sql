{{ config(materialized='table') }}

SELECT DISTINCT
    artist_id,
    artist_name,
    artist_popularity,
    artist_genres
FROM {{ source('bronze', 'artists') }}
WHERE artist_id IS NOT NULL 