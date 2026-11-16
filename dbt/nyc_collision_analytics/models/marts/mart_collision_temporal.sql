-- P4.5: Temporal collision analysis mart
--
-- What it is:
--   Answers "when do collisions happen?": by year, month, weekday and hour
--   of the day. Ready for heatmaps such as weekday by hour, or trend lines
--   over months.
--
-- Grain: one row per crash_year, crash_month, crash_day_of_week and
-- crash_hour (time_of_day is carried along as a label).
--
-- Metrics:
--   - total_collisions, total_injuries, total_fatalities
--   - fatal_collisions, injury_collisions: collisions by severity.
--   - injury_or_fatal_collision_pct: share of collisions in that slot that
--     injured or killed someone.
--
-- How to read it:
--   - crash_day_of_week: 1 = Sunday, 2 = Monday, ..., 7 = Saturday.
--   - Only collisions with a valid date and hour are included.
--   - A slot with no collisions has no row, rather than a row of zeros.

WITH hourly_metrics AS (

    SELECT
        t.crash_year,
        t.crash_month,
        t.crash_day_of_week,
        t.crash_hour,
        t.time_of_day,

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

    INNER JOIN {{ ref('dim_time') }} t
        ON f.time_key = t.time_key

    GROUP BY
        t.crash_year,
        t.crash_month,
        t.crash_day_of_week,
        t.crash_hour,
        t.time_of_day

)

SELECT
    crash_year,
    crash_month,
    crash_day_of_week,
    crash_hour,
    time_of_day,

    total_collisions,
    total_injuries,
    total_fatalities,
    fatal_collisions,
    injury_collisions,

    ROUND(
        100.0 * (fatal_collisions + injury_collisions)
        / NULLIF(total_collisions, 0),
        2
    ) AS injury_or_fatal_collision_pct

FROM hourly_metrics
