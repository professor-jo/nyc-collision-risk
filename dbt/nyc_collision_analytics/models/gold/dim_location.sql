-- Gold dimension: dim_location
--
-- What it is:
--   The "where" dimension. NYC is divided into Uber H3 hexagons, and each
--   row is one resolution-9 cell (about 0.1 km2, a few city blocks) that
--   has had at least one collision with valid coordinates. It is the basis
--   for hotspot maps and area comparisons.
--
-- Grain: one row per H3 resolution-9 cell.
--
-- Key:
--   location_key = h3_res9. fact_collision uses the same value.
--
-- Columns:
--   - h3_res8: the larger parent cell (about 0.7 km2), for rolling up to
--     neighbourhood level.
--   - borough, zip_code: one representative value per cell (MAX). A cell
--     on a boundary can span two boroughs or ZIP codes, so treat these as
--     labels, not exact assignments.
--   - avg_latitude, avg_longitude: the average position of the collisions
--     in the cell, not the hexagon's geometric centre.
--
-- Excludes collisions with missing H3 cells or invalid coordinates.

WITH valid_locations AS (

    SELECT
        h3_res9,
        h3_res8,
        borough,
        zip_code,
        latitude,
        longitude

    FROM {{ ref('stg_collisions') }}

    WHERE h3_res9 IS NOT NULL
      AND invalid_coordinates = false

),

aggregated_locations AS (

    SELECT
        h3_res9,

        -- Representative geographic attributes
        MAX(h3_res8) AS h3_res8,
        MAX(borough) AS borough,
        MAX(zip_code) AS zip_code,

        -- Approximate collision-location centroid
        AVG(latitude) AS avg_latitude,
        AVG(longitude) AS avg_longitude

    FROM valid_locations

    GROUP BY h3_res9

)

SELECT
    h3_res9 AS location_key,
    h3_res9,
    h3_res8,
    borough,
    zip_code,
    avg_latitude,
    avg_longitude

FROM aggregated_locations
