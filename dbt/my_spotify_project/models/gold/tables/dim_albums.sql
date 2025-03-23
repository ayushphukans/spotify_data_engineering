{{ config(materialized='table') }}

SELECT DISTINCT
    album_id,
    album_name,
    album_release_date
FROM {{ source('bronze', 'tracks') }} 