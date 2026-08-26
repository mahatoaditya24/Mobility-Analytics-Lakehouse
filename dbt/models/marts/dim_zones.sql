{{ config(materialized='table', file_format='delta') }}

with distinct_zones as (
    select distinct city_zone from {{ ref('stg_traffic') }}
)

select
    city_zone,
    case 
        when city_zone = 'CBD' then 'Commercial / Financial Center'
        when city_zone = 'TECHPARK' then 'IT & Technology Hub'
        when city_zone in ('AIRPORT', 'TRAINSTATION', 'HARBOR') then 'Multimodal Transit Hub'
        else 'Residential & Suburban District'
    end as zone_classification,
    case
        when city_zone in ('CBD', 'AIRPORT', 'TRAINSTATION') then 'CRITICAL'
        when city_zone = 'TECHPARK' then 'HIGH'
        when city_zone = 'HARBOR' then 'MEDIUM'
        else 'LOW'
    end as traffic_risk_tier,
    current_timestamp() as dbt_updated_at
from distinct_zones
