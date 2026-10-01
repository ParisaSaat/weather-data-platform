{#
  Long-format fact: one row per station x date x element.

  Incremental with a per-(station, element) watermark and a trailing lookback:
    * NOAA revises recent values/flags, so the last N days are re-processed each run.
    * A station or element that is new to config has no watermark, so its whole
      window is backfilled automatically: adding a city needs no --full-refresh.
  The post-hook trims rows that fell out of scope (window moved forward, or a
  station was removed from config) so the table always matches the current config.
#}
{{
    config(
        materialized='incremental',
        unique_key=['station_id', 'observation_date', 'element'],
        incremental_strategy='delete+insert',
        on_schema_change='fail',
        post_hook=[
            "delete from {{ this }}
             where observation_date < (select window_start from {{ ref('int_analysis_window') }})
                or observation_date > (select window_end from {{ ref('int_analysis_window') }})
                or (station_id, element) not in (
                    select station_id, element from {{ ref('int_station_elements_selected') }}
                )"
        ]
    )
}}
-- depends_on: {{ ref('int_analysis_window') }}
-- depends_on: {{ ref('int_station_elements_selected') }}

with scoped as (
    select * from {{ ref('int_daily_observations_scoped') }}
)

{% if is_incremental() %}
, watermarks as (
    select station_id, element, max(observation_date) as loaded_through
    from {{ this }}
    group by station_id, element
)
{% endif %}

select
    s.station_id::varchar                  as station_id,
    s.observation_date::date               as observation_date,
    s.element::varchar                     as element,
    s.element_category::varchar            as element_category,
    s.raw_value::integer                   as raw_value,
    s.value::decimal(10, 2)                as value,
    s.value_clean::decimal(10, 2)          as value_clean,
    s.unit::varchar                        as unit,
    s.measurement_flag::varchar            as measurement_flag,
    s.quality_flag::varchar                as quality_flag,
    s.source_flag::varchar                 as source_flag,
    s.is_quality_rejected::boolean         as is_quality_rejected,
    s.is_trace::boolean                    as is_trace,
    s.is_unit_corrected::boolean           as is_unit_corrected,
    s.batch_id::varchar                    as ingestion_batch_id,
    current_timestamp::timestamp           as transformed_at
from scoped as s
{% if is_incremental() %}
left join watermarks as w using (station_id, element)
where w.loaded_through is null
   or s.observation_date > w.loaded_through - interval '{{ var("incremental_lookback_days") | int }} days'
{% endif %}
