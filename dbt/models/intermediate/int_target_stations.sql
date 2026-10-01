-- Configured stations enriched with metadata and observed coverage.
with targets as (
    select * from {{ ref('stg_pipeline__target_stations') }}
),

stations as (
    select * from {{ ref('stg_ghcnd__stations') }}
),

countries as (
    select * from {{ ref('stg_ghcnd__countries') }}
),

inventory_coverage as (
    select
        station_id,
        min(first_year)  as inventory_first_year,
        max(last_year)   as inventory_last_year,
        count(*)         as inventory_element_count
    from {{ ref('stg_ghcnd__inventory') }}
    group by station_id
),

observed_coverage as (
    select
        station_id,
        min(observation_date)  as first_observation_date,
        max(observation_date)  as last_observation_date
    from {{ ref('stg_ghcnd__daily_observations') }}
    group by station_id
)

select
    t.station_id,
    t.city,
    s.station_name,
    s.state_code,
    s.country_code,
    c.country_name,
    s.latitude,
    s.longitude,
    s.elevation_m,
    s.wmo_id,
    t.resolution_method,
    t.match_rule,
    i.inventory_first_year,
    i.inventory_last_year,
    i.inventory_element_count,
    o.first_observation_date,
    o.last_observation_date
from targets as t
inner join stations as s using (station_id)
left join countries as c using (country_code)
left join inventory_coverage as i using (station_id)
left join observed_coverage as o using (station_id)
