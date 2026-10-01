{{ config(materialized='view') }}
{#
  Consumption view: every narrative-eligible station-day with its current narrative
  and validation result. A view so it reflects narratives generated after `dbt build`.
#}
select
    i.station_id,
    i.city,
    i.observation_date,
    n.narrative,
    n.model,
    n.prompt_version,
    n.generated_at,
    n.input_hash = i.input_hash               as is_narrative_current,
    v.status                                  as validation_status,
    v.issues                                  as validation_issues,
    i.observations_json
from {{ ref('mart_narrative_inputs') }} as i
left join {{ source('narratives', 'daily_weather_narratives') }} as n
    using (station_id, observation_date)
left join {{ source('narratives', 'narrative_validations') }} as v
    on v.station_id = n.station_id
   and v.observation_date = n.observation_date
   and v.input_hash = n.input_hash
