{{ config(materialized='table') }}

SELECT DISTINCT
    track_id,
    flattened.value::VARCHAR AS artist_id
FROM {{ source('bronze', 'tracks') }},
LATERAL FLATTEN(input => artist_ids) AS flattened