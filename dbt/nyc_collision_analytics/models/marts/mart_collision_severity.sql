-- P4.5: Collision severity analytical mart
--
-- What it is:
--   Answers "how severe are collisions in each borough, and is that
--   changing from year to year?". It splits each borough's collisions into
--   Fatal, Injury and No Injury.
--
-- Grain: one row per crash_year, borough and severity_category.
--
-- Metrics:
--   - total_collisions, total_injuries, total_fatalities
--   - severity_pct: share of that borough's collisions in that year that
--     fall in this severity category. The three categories of a
--     borough-year add up to 100.
--
-- How to read it:
--   - severity_category comes from fact_collision: Fatal if anyone was
--     killed, otherwise Injury if anyone was injured, otherwise No Injury.
--   - borough comes from the collision's H3 cell (dim_location). Collisions
--     without valid coordinates appear as "Unknown".
--   - crash_year is empty for collisions without a usable date and hour.

WITH severity_metrics AS (

    SELECT
        t.crash_year,
        COALESCE(l.borough, 'Unknown') AS borough,
        f.severity_category,

        COUNT(*) AS total_collisions,

        SUM(f.number_of_persons_injured)
            AS total_injuries,

        SUM(f.number_of_persons_killed)
            AS total_fatalities

    FROM {{ ref('fact_collision') }} f

    LEFT JOIN {{ ref('dim_time') }} t
        ON f.time_key = t.time_key

    LEFT JOIN {{ ref('dim_location') }} l
        ON f.location_key = l.location_key

    GROUP BY
        t.crash_year,
        COALESCE(l.borough, 'Unknown'),
        f.severity_category

),

borough_year_totals AS (

    SELECT
        *,
        SUM(total_collisions) OVER (
            PARTITION BY crash_year, borough
        ) AS borough_year_collisions

    FROM severity_metrics

)

SELECT
    crash_year,
    borough,
    severity_category,
    total_collisions,
    total_injuries,
    total_fatalities,

    ROUND(
        100.0 * total_collisions
        / NULLIF(borough_year_collisions, 0),
        2
    ) AS severity_pct

FROM borough_year_totals
