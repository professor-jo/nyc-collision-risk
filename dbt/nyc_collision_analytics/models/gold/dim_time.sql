-- Gold dimension: dim_time
--
-- What it is:
--   The "when" dimension of the star schema. Each row is one hour on one
--   calendar date, with the calendar attributes needed to slice collisions
--   by year, quarter, month, weekday, hour and part of the day.
--
-- Grain: one row per crash_date and crash_hour.
--
-- Key:
--   time_key = YYYYMMDDHH as a BIGINT (for example 2026110814 is
--   8 Nov 2026, 14:00). fact_collision builds the same key, so the
--   join needs no lookup.
--
-- Notes:
--   - Only hours that actually had a collision appear. This is not a
--     full calendar, so missing hours mean "no collisions", not missing data.
--   - crash_day_of_week comes from Spark's dayofweek(): 1 = Sunday,
--     2 = Monday, ..., 7 = Saturday.
--   - time_of_day buckets: Morning 06-11, Afternoon 12-17,
--     Evening 18-21, Night 22-05.

WITH distinct_times AS (

    SELECT DISTINCT
        crash_date,
        crash_hour,
        crash_day_of_week,
        crash_month
    FROM {{ ref('stg_collisions') }}
    WHERE crash_date IS NOT NULL
    AND crash_hour IS NOT NULL
)

SELECT
    CAST(TO_CHAR(crash_date, 'YYYYMMDD') AS BIGINT) * 100 + crash_hour AS time_key,
    crash_date,
    crash_hour,
    crash_day_of_week,
    crash_month,

    EXTRACT(YEAR FROM crash_date) AS crash_year,
    EXTRACT(QUARTER FROM crash_date) AS crash_quarter,

    CASE
        WHEN crash_hour BETWEEN 6 AND 11 THEN 'Morning'
        WHEN crash_hour BETWEEN 12 AND 17 THEN 'Afternoon'
        WHEN crash_hour BETWEEN 18 AND 21 THEN 'Evening'
        ELSE 'Night'
    END AS time_of_day

FROM distinct_times
