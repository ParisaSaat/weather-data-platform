with stations as (
    select * from {{ ref('int_target_stations') }}
),

analysis_window as (
    select * from {{ ref('int_analysis_window') }}
),

window_coverage as (
    select station_id, count(distinct observation_date) as days_with_observations
    from {{ ref('int_daily_observations_scoped') }}
    where value_clean is not null
    group by station_id
)

select
    s.station_id,
    s.city,
    s.station_name,
    s.state_code,
    s.country_code,
    s.country_name,
    s.latitude,
    s.longitude,
    s.elevation_m,
    s.wmo_id,
    s.resolution_method,
    s.inventory_first_year,
    s.inventory_last_year,
    s.first_observation_date,
    s.last_observation_date,
    date_diff('day', s.last_observation_date, current_date)                    as days_since_last_observation,
    date_diff('day', s.last_observation_date, current_date)
        > {{ var('staleness_threshold_days') }}                                  as is_stale,
    coalesce(c.days_with_observations, 0)                                        as window_days_with_observations,
    round(100.0 * coalesce(c.days_with_observations, 0) / w.window_days, 1)      as window_coverage_pct
from stations as s
cross join analysis_window as w
left join window_coverage as c using (station_id)
