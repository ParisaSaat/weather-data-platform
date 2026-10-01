{#
  The contract between the warehouse and the LLM pipeline: one row per station-day
  with every clean observation as a self-describing JSON list (element, description,
  value, unit, previous-day value). Because rows are generic, new elements flow into
  narratives with no Python change. `input_hash` lets the generator skip days whose
  inputs haven't changed since the last narrative.
#}
with observations as (
    select
        f.station_id,
        f.observation_date,
        f.element,
        f.element_category,
        f.value_clean,
        f.unit,
        f.is_trace,
        e.description,
        lag(f.value_clean) over w       as previous_value,
        lag(f.observation_date) over w  as previous_date
    from {{ ref('fct_daily_observations') }} as f
    inner join {{ ref('dim_elements') }} as e using (element)
    where f.value_clean is not null
    window w as (partition by f.station_id, f.element order by f.observation_date)
),

per_day as (
    select
        station_id,
        observation_date,
        list(
            struct_pack(
                element := element,
                description := description,
                category := element_category,
                value := value_clean,
                unit := unit,
                is_trace := is_trace,
                previous_day_value := case
                    when previous_date = observation_date - 1 then previous_value
                end
            )
            order by element_category, element
        ) as observations
    from observations
    group by station_id, observation_date
)

select
    d.station_id,
    s.city,
    s.station_name,
    s.state_code                              as province,
    d.observation_date,
    len(d.observations)                       as observation_count,
    to_json(d.observations)::varchar          as observations_json,
    md5(
        concat_ws('|', s.city, s.station_name, d.observation_date::varchar, to_json(d.observations)::varchar)
    )                                         as input_hash
from per_day as d
inner join {{ ref('dim_stations') }} as s using (station_id)
