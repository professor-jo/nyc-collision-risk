-- P4.5: Contributing factor analytical mart
--
-- What it is:
--   Answers "which contributing factors are linked to the most collisions,
--   and how often do they lead to injuries or deaths?". Ready to chart as a
--   ranked bar chart of factors.
--
-- Grain: one row per contributing factor.
--
-- Metrics:
--   - total_collisions: collisions where the factor was recorded.
--   - total_injuries, total_fatalities: people injured or killed in those
--     collisions.
--   - fatal_collisions, injury_collisions: collisions by severity.
--   - injury_or_fatal_collision_pct: share of the factor's collisions that
--     injured or killed someone.
--
-- How to read it:
--   - A collision with several factors counts once under each of them, and
--     its injuries and deaths are counted in full under each factor. So the
--     rows don't add up to the city total. Compare factors with each other
--     rather than summing them.
--   - "Unspecified" is usually near the top. It means no factor was
--     recorded, not a cause.

WITH factor_metrics AS (

    SELECT
        b.factor_key,

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

    FROM {{ ref('bridge_collision_factor') }} b

    INNER JOIN {{ ref('fact_collision') }} f
        ON b.collision_id = f.collision_id

    GROUP BY b.factor_key

)

SELECT
    d.factor_key,
    d.factor_name,

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

FROM factor_metrics m

INNER JOIN {{ ref('dim_factor') }} d
    ON m.factor_key = d.factor_key
