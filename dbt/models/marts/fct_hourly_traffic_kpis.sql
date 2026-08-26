{{ config(
    materialized='table',
    file_format='delta',
    partition_by=['event_date']
) }}

with traffic as (
    select * from {{ ref('stg_traffic') }}
),

roads as (
    select * from {{ ref('dim_roads') }}
),

joined as (
    select
        t.event_date,
        t.event_hour,
        t.city_zone,
        t.road_id,
        r.road_classification,
        r.speed_limit_kmh,
        t.is_peak_hour,
        t.is_weekend,
        t.weather_condition,
        t.vehicle_id,
        t.speed_kmh,
        t.congestion_level,
        case when t.speed_kmh > r.speed_limit_kmh then 1 else 0 end as is_speeding
    from traffic t
    left join roads r on t.road_id = r.road_id
),

aggregated as (
    select
        event_date,
        event_hour,
        city_zone,
        road_id,
        road_classification,
        is_peak_hour,
        is_weekend,
        count(vehicle_id) as total_vehicle_volume,
        round(avg(speed_kmh), 2) as avg_speed_kmh,
        min(speed_kmh) as min_speed_kmh,
        max(speed_kmh) as max_speed_kmh,
        round(avg(congestion_level), 2) as avg_congestion_level,
        sum(case when congestion_level >= 4 then 1 else 0 end) as gridlock_observations,
        sum(is_speeding) as speeding_violation_count,
        round((sum(is_speeding) * 100.0 / count(vehicle_id)), 2) as speeding_violation_rate_pct,
        current_timestamp() as aggregated_at
    from joined
    group by
        event_date,
        event_hour,
        city_zone,
        road_id,
        road_classification,
        is_peak_hour,
        is_weekend
)

select * from aggregated
