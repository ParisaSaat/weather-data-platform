{#
  Wide, analyst-friendly daily table: one row per station x calendar day in the window
  (a date spine, so missing days are explicit rows rather than silent gaps).
  Element columns are generated at compile time from the inventory-driven element
  selection: a station that starts reporting e.g. AWND gets an `awnd` column
  on the next run with no SQL change.
#}
-- depends_on: {{ ref('int_station_elements_selected') }}
{%- set elements = get_selected_elements() %}

with analysis_window as (
    select * from {{ ref('int_analysis_window') }}
),

spine as (
    select
        s.station_id,
        s.city,
        unnest(generate_series(w.window_start::timestamp, w.window_end::timestamp, interval 1 day))::date
            as observation_date
    from {{ ref('dim_stations') }} as s
    cross join analysis_window as w
),

expected as (
    select station_id, count(*) as elements_expected
    from {{ ref('int_station_elements_selected') }}
    group by station_id
),

pivoted as (
    select
        station_id,
        observation_date,
        {%- for element in elements %}
        max(value_clean) filter (where element = '{{ element }}')  as {{ element | lower }},
        {%- endfor %}
        count(value_clean)             as elements_reported,
        count_if(is_quality_rejected)  as elements_rejected
    from {{ ref('fct_daily_observations') }}
    group by station_id, observation_date
)

select
    spine.station_id,
    spine.city,
    spine.observation_date,
    {%- for element in elements %}
    p.{{ element | lower }},
    {%- endfor %}
    coalesce(p.elements_reported, 0)                                        as elements_reported,
    coalesce(p.elements_rejected, 0)                                        as elements_rejected,
    e.elements_expected,
    p.station_id is null                                                    as is_missing_day
from spine
left join pivoted as p using (station_id, observation_date)
left join expected as e using (station_id)
