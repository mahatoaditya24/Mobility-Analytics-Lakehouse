{{ config(materialized='table', file_format='delta') }}

with distinct_roads as (
    select distinct road_id from {{ ref('stg_traffic') }}
)

select
    road_id,
    case
        when road_id in ('R100', 'R200') then 'Expressway / Highway'
        when road_id = 'R300' then 'Major Arterial Road'
        when road_id = 'R400' then 'Urban Collector Road'
        else 'Local Access Street'
    end as road_classification,
    case
        when road_id in ('R100', 'R200') then 100
        when road_id = 'R300' then 60
        when road_id = 'R400' then 50
        else 40
    end as speed_limit_kmh,
    case
        when road_id in ('R100', 'R200') then 4
        when road_id = 'R300' then 3
        when road_id = 'R400' then 2
        else 1
    end as total_lanes,
    current_timestamp() as dbt_updated_at
from distinct_roads
