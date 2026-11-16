
-- Gold bridge: collision-to-contributing-factor relationships
-- Grain: one row per unique collision and normalized factor

WITH all_factors AS (

    SELECT collision_id, contributing_factor_vehicle_1 AS factor
    FROM {{ ref('stg_collisions') }}

    UNION ALL

    SELECT collision_id, contributing_factor_vehicle_2 AS factor
    FROM {{ ref('stg_collisions') }}

    UNION ALL

    SELECT collision_id, contributing_factor_vehicle_3 AS factor
    FROM {{ ref('stg_collisions') }}

    UNION ALL

    SELECT collision_id, contributing_factor_vehicle_4 AS factor
    FROM {{ ref('stg_collisions') }}

    UNION ALL

    SELECT collision_id, contributing_factor_vehicle_5 AS factor
    FROM {{ ref('stg_collisions') }}

),

cleaned_factors AS (

    SELECT DISTINCT
        collision_id,
        LOWER(TRIM(factor)) AS normalized_factor
    FROM all_factors
    WHERE collision_id IS NOT NULL
      AND factor IS NOT NULL
      AND TRIM(factor) <> ''

),

factor_relationships AS (

    SELECT
        collision_id,
        MD5(normalized_factor) AS factor_key
    FROM cleaned_factors

)

SELECT
    b.collision_id,
    b.factor_key
FROM factor_relationships b
INNER JOIN {{ ref('fact_collision') }} f
    ON b.collision_id = f.collision_id
INNER JOIN {{ ref('dim_factor') }} d
    ON b.factor_key = d.factor_key

