-- Observations NOAA flagged as failing QA, explained with the readme's own flag definitions.
select
    o.station_id,
    o.element,
    o.quality_flag,
    f.flag_description,
    count(*)                    as observation_count,
    min(o.observation_date)     as first_seen,
    max(o.observation_date)     as last_seen
from {{ ref('int_daily_observations_scoped') }} as o
left join {{ ref('stg_ghcnd__flag_definitions') }} as f
    on f.flag_type = 'Q' and f.flag_code = o.quality_flag
where o.quality_flag is not null
group by all
