{{ config(materialized='table') }}

WITH raw_countries AS (
    SELECT DISTINCT
        country
    FROM {{ source('bronze', 'tracks') }}
    WHERE country IS NOT NULL
)

SELECT
    country AS geo_code,
    CASE country
        WHEN 'DE' THEN 'Germany'
        WHEN 'FR' THEN 'France'
        WHEN 'ES' THEN 'Spain'
        WHEN 'CH' THEN 'Switzerland'
        WHEN 'SE' THEN 'Sweden'
        WHEN 'AT' THEN 'Austria'
        WHEN 'DK' THEN 'Denmark'
        WHEN 'NL' THEN 'Netherlands'
        WHEN 'BE' THEN 'Belgium'
        ELSE 'Other'
    END AS country_name
FROM raw_countries 