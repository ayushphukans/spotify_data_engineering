{{ config(materialized='view') }}

SELECT DISTINCT
    artist_id,
    artist_name,
    genres,
    popularity AS artist_popularity,
    followers AS artist_followers
FROM {{ source('bronze', 'artists') }}
