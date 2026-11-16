-- Gold dimension: dim_factor
--
-- What it is:
--   The list of contributing factors the NYPD records for each vehicle in
--   a collision, such as "Driver Inattention/Distraction" or "Following
--   Too Closely". The five per-vehicle factor columns are unpivoted into a
--   single list of distinct factors.
--
-- Grain: one row per distinct factor name (trimmed).
--
-- Key:
--   factor_key = MD5 of the lower-cased name, so the same factor gets the
--   same key on every run. bridge_collision_factor uses the same key.
--
-- Notes:
--   - "Unspecified" is a real value in the source and is usually one of
--     the most frequent. It means the factor wasn't recorded.
--   - Links to collisions go through bridge_collision_factor, because one
--     collision can have several factors.

WITH all_factors AS (

    SELECT contributing_factor_vehicle_1 AS factor
    FROM {{ ref('stg_collisions') }}

    UNION ALL

    SELECT contributing_factor_vehicle_2 AS factor
    FROM {{ ref('stg_collisions') }}

    UNION ALL

    SELECT contributing_factor_vehicle_3 AS factor
    FROM {{ ref('stg_collisions') }}

    UNION ALL

    SELECT contributing_factor_vehicle_4 AS factor
    FROM {{ ref('stg_collisions') }}

    UNION ALL

    SELECT contributing_factor_vehicle_5 AS factor
    FROM {{ ref('stg_collisions') }}

),

cleaned_factors AS (

    SELECT DISTINCT
        TRIM(factor) AS factor_name

    FROM all_factors

    WHERE factor IS NOT NULL
      AND TRIM(factor) <> ''

)

SELECT
    MD5(LOWER(factor_name)) AS factor_key,
    factor_name

FROM cleaned_factors
