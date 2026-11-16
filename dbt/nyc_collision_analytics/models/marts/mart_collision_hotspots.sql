-- Geographic hotspot analytical mart
--
-- What it is:
--   Answers "where in NYC do collisions happen most, and where are they
--   most severe?". Each row is one H3 hexagon with its collision counts
--   and coordinates, ready for a hotspot or choropleth map.
--
-- Grain: one row per H3 resolution-9 cell (about 0.1 km2).
--
-- Metrics:
--   - total_collisions, total_injuries, total_fatalities
--   - fatal_collisions, injury_collisions: collisions by severity.
--   - injury_or_fatal_collision_pct: share of the cell's collisions that
--     injured or killed someone.
--
-- How to read it:
--   - Only collisions with valid coordinates and an H3 cell are included.
--   - Cells with only a few collisions can show extreme percentages (one
--     collision with an injury is 100%). Filter on a minimum
--     total_collisions before ranking by percentage.
--   - Use h3_res8 to roll cells up to larger areas.

WITH location_metrics AS (

    SELECT
        f.location_key,

        COUNT(*) AS total_collisions,

        SUM(f.number_of_persons_injured)
            AS total_injuries,

        SUM(f.number_of_persons_killed)
            AS total_fatalities,

        SUM(
            CASE
                WHEN f.severity_category = 'Fatal'
                THEN 1 ELSE 0
            END
        ) AS fatal_collisions,

        SUM(
            CASE
                WHEN f.severity_category = 'Injury'
                THEN 1 ELSE 0
            END
        ) AS injury_collisions

    FROM {{ ref('fact_collision') }} f

    WHERE f.location_key IS NOT NULL

    GROUP BY f.location_key

)

SELECT
    l.location_key,
    l.h3_res9,
    l.h3_res8,
    l.borough,
    l.zip_code,
    l.avg_latitude,
    l.avg_longitude,

    m.total_collisions,
    m.total_injuries,
    m.total_fatalities,
    m.fatal_collisions,
    m.injury_collisions,

    ROUND(
        100.0 * (m.fatal_collisions + m.injury_collisions)
        / NULLIF(m.total_collisions, 0),
        2
    ) AS injury_or_fatal_collision_pct

FROM location_metrics m

INNER JOIN {{ ref('dim_location') }} l
    ON m.location_key = l.location_key
