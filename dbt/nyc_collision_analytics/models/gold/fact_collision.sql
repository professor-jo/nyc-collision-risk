-- Gold fact table: one row per collision
-- Connects collision events to time and location dimensions

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
