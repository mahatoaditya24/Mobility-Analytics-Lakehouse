{{ config(materialized='view') }}

with source as (
    select * from {{ source('lakehouse', 'traffic_silver') }}
),

renamed as (
    select
        cast(vehicle_id as string) as vehicle_id,
        cast(road_id as string) as road_id,
        cast(city_zone as string) as city_zone,
        cast(speed_int as integer) as speed_kmh,
        cast(congestion_level as integer) as congestion_level,
        cast(congestion_risk_score as integer) as congestion_risk_score,
        cast(speed_band as string) as speed_band,
        cast(peak_flag as integer) as is_peak_hour,
        cast(is_weekend as integer) as is_weekend,
        cast(weather as string) as weather_condition,
        cast(vehicle_type as string) as vehicle_type,
        cast(sensor_id as string) as sensor_id,
        cast(latitude as double) as latitude,
        cast(longitude as double) as longitude,
        cast(event_ts as timestamp) as event_timestamp,
        cast(to_date(event_ts) as date) as event_date,
        cast(event_hour as integer) as event_hour
    from source
    where vehicle_id is not null
)

select * from renamed
