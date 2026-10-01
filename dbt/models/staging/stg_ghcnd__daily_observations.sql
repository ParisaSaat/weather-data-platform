with source as (
    select * from {{ source('ghcnd', 'ghcnd_daily') }}
)

select
    station_id,
    try_strptime(obs_date, '%Y%m%d')::date                    as observation_date,
    element,
    -- GHCN encodes missing as -9999; values are integers in the element's native unit
    nullif(try_cast(data_value as integer), -9999)            as raw_value,
    nullif(trim(m_flag), '')                                  as measurement_flag,
    nullif(trim(q_flag), '')                                  as quality_flag,
    nullif(trim(s_flag), '')                                  as source_flag,
    nullif(trim(obs_time), '')                                as observation_time_hhmm,
    _source_file                                              as source_file,
    _batch_id                                                 as batch_id,
    _loaded_at                                                as loaded_at
from source
