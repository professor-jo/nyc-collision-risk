-- Gold bridge: bridge_collision_factor
--
-- What it is:
--   The link between collisions and their contributing factors. A
--   collision can involve up to five vehicles, each with its own factor,
--   so collisions and factors are many-to-many. This table resolves that,
--   so fact_collision can be joined to dim_factor without duplicating
--   collision rows by accident.
--
-- Grain: one row per collision and distinct contributing factor.
--
-- Keys:
--   - collision_id: joins to fact_collision.
--   - factor_key: MD5 of the lower-cased, trimmed factor name; joins to
--     dim_factor.
--
-- Notes:
--   - If two vehicles in the same collision have the same factor, it is
--     listed once for that collision.
--   - Empty and NULL factors are dropped.
--   - The inner joins keep only rows whose collision exists in
--     fact_collision and whose factor exists in dim_factor, so every row
--     joins cleanly in both directions.

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

