{{ config(materialized='view') }}

SELECT
    t.track_id,
    f.value::STRING AS artist_id
FROM {{ source('bronze', 'tracks') }} AS t,
     LATERAL FLATTEN(input => t.artist_ids) AS f
