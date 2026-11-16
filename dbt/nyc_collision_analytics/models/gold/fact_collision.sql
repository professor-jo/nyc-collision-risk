-- Gold fact table: fact_collision
--
-- What it is:
--   The centre of the star schema: one row for every collision, with its
--   injury and fatality counts and keys to the time and location
--   dimensions. Every mart is built from this table.
--
-- Grain: one row per collision_id.
--
-- Keys:
--   - time_key (to dim_time): YYYYMMDDHH. NULL when the date is missing
--     or the hour isn't between 0 and 23.
--   - location_key (to dim_location): the H3 resolution-9 cell. NULL when
--     the coordinates are invalid or there is no H3 cell.
--   - Factors join through bridge_collision_factor on collision_id.
--
-- Measures:
--   - number_of_{persons,pedestrians,cyclist,motorist}_{injured,killed}:
--     people hurt in the collision. A missing count is treated as 0.
--   - severity_category: Fatal if anyone was killed, otherwise Injury if
--     anyone was injured, otherwise No Injury.
--   - invalid_coordinates: TRUE when the collision's location is outside
--     NYC or unusable, so spatial analysis can exclude it.
--
-- Collisions with a NULL key stay in the table, so city-wide totals stay
-- complete. Use LEFT JOINs to the dimensions to keep them.

WITH collision_events AS (

    SELECT
        collision_id,
        crash_date,
        crash_hour,
        h3_res9,
        invalid_coordinates,

        COALESCE(number_of_persons_injured, 0)
            AS number_of_persons_injured,

        COALESCE(number_of_persons_killed, 0)
            AS number_of_persons_killed,

        COALESCE(number_of_pedestrians_injured, 0)
            AS number_of_pedestrians_injured,

        COALESCE(number_of_pedestrians_killed, 0)
            AS number_of_pedestrians_killed,

        COALESCE(number_of_cyclist_injured, 0)
            AS number_of_cyclist_injured,

        COALESCE(number_of_cyclist_killed, 0)
            AS number_of_cyclist_killed,

        COALESCE(number_of_motorist_injured, 0)
            AS number_of_motorist_injured,

        COALESCE(number_of_motorist_killed, 0)
            AS number_of_motorist_killed

    FROM {{ ref('stg_collisions') }}

),

fact_records AS (

    SELECT
        collision_id,

        -- Foreign key to dim_time
        CASE
            WHEN crash_date IS NOT NULL
             AND crash_hour BETWEEN 0 AND 23
            THEN
                CAST(
                    TO_CHAR(crash_date, 'YYYYMMDD')
                    AS BIGINT
                ) * 100 + crash_hour
            ELSE NULL
        END AS time_key,

        -- Foreign key to dim_location
        CASE
            WHEN invalid_coordinates = false
            THEN h3_res9
            ELSE NULL
        END AS location_key,

        number_of_persons_injured,
        number_of_persons_killed,
        number_of_pedestrians_injured,
        number_of_pedestrians_killed,
        number_of_cyclist_injured,
        number_of_cyclist_killed,
        number_of_motorist_injured,
        number_of_motorist_killed,

        CASE
            WHEN number_of_persons_killed > 0
                THEN 'Fatal'
            WHEN number_of_persons_injured > 0
                THEN 'Injury'
            ELSE 'No Injury'
        END AS severity_category,

        invalid_coordinates

    FROM collision_events

)

SELECT *
FROM fact_records
