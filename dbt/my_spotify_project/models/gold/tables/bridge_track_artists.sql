{{ config(materialized='table') }}

SELECT DISTINCT
    track_id,
    artist_id
FROM {{ source('bronze', 'tracks') }},
LATERAL FLATTEN(input => artist_ids) 