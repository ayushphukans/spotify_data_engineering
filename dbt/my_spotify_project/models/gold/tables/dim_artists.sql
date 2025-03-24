{{ config(materialized='table') }}

SELECT DISTINCT
    artist_id,
    artist_name,
    popularity AS artist_popularity,
    genres AS artist_genres
FROM {{ source('bronze', 'artists') }}
WHERE artist_id IS NOT NULL