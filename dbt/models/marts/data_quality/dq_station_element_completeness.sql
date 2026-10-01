{#
  Monthly completeness per (station, element) against the days the inventory says
  should exist. Only element categories listed in config are monitored; event-based
  series (snow depth, weather-type flags, gusts) are naturally sparse.
#}
{%- set monitored = var('completeness_monitored_categories') -%}

with analysis_window as (
    select * from {{ ref('int_analysis_window') }}
),

expected_days as (
    select
        date_trunc('month', d)::date   as month_start,
        count(*)                       as days_in_scope
    from (
        select unnest(generate_series(window_start::timestamp, window_end::timestamp, interval 1 day))::date as d
        from analysis_window
    )
    group by 1
),

pairs as (
    select station_id, element, element_category
    from {{ ref('int_station_elements_selected') }}
),

observed as (
    select
        station_id,
        element,
        date_trunc('month', observation_date)::date  as month_start,
        count(*)                                     as days_reported,
        count(value_clean)                           as days_valid,
        count_if(is_quality_rejected)                as days_rejected
    from {{ ref('int_daily_observations_scoped') }}
    group by all
)

select
    p.station_id,
    p.element,
    p.element_category,
    e.month_start,
    e.days_in_scope,
    coalesce(o.days_reported, 0)                                       as days_reported,
    coalesce(o.days_valid, 0)                                          as days_valid,
    coalesce(o.days_rejected, 0)                                       as days_rejected,
    round(100.0 * coalesce(o.days_valid, 0) / e.days_in_scope, 1)      as completeness_pct,
    p.element_category in (
        {%- for c in monitored %}'{{ c }}'{% if not loop.last %}, {% endif %}{% endfor -%}
    )                                                                  as is_monitored,
    round(100.0 * coalesce(o.days_valid, 0) / e.days_in_scope, 1)
        < {{ var('min_completeness_pct') }}                            as is_below_threshold
from pairs as p
cross join expected_days as e
left join observed as o
    on o.station_id = p.station_id
   and o.element = p.element
   and o.month_start = e.month_start
