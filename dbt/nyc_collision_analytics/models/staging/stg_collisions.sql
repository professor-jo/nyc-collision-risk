-- Read collision records from the Silver Iceberg table.
-- This staging model will become the foundation
-- for our Gold dimensional models.


-- P4.3: Silver-to-Gold staging model
-- Source: Redshift Spectrum external Iceberg table

SELECT
    -- Collision identifier
    collision_id,

    -- Temporal features
    crash_date,
    crash_time,
    crash_timestamp,
    crash_hour,
    crash_day_of_week,
    crash_month,

    -- Geographic features
    borough,
    zip_code,
    latitude,
    longitude,
    h3_res8,
    h3_res9,
    invalid_coordinates,

    -- Street information
    on_street_name,
    cross_street_name,
    off_street_name,

    -- Injury and fatality metrics
    number_of_persons_injured,
    number_of_persons_killed,
    number_of_pedestrians_injured,
    number_of_pedestrians_killed,
    number_of_cyclist_injured,
    number_of_cyclist_killed,
    number_of_motorist_injured,
    number_of_motorist_killed,

    -- Contributing factors (up to five vehicles)
    contributing_factor_vehicle_1,
    contributing_factor_vehicle_2,
    contributing_factor_vehicle_3,
    contributing_factor_vehicle_4,
    contributing_factor_vehicle_5,

    -- Vehicle types
    vehicle_type_code1,
    vehicle_type_code2,
    vehicle_type_code_3,
    vehicle_type_code_4,
    vehicle_type_code_5,

    -- Data lineage and ingestion metadata
    source_file,
    source_modified_at,
    ingestion_date,
    source_version_at
FROM {{ source('silver', 'silver_collisions') }}