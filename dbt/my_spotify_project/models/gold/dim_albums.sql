{{ config(materialized='view') }}

SELECT DISTINCT
    album_id,
    album_name
FROM {{ source('bronze', 'tracks') }}
WHERE album_id IS NOT NULL
