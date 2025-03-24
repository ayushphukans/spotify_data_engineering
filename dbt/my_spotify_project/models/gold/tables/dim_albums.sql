{{ config(materialized='table') }}

SELECT DISTINCT
    album_id,
    album_name
FROM {{ source('bronze', 'tracks') }}