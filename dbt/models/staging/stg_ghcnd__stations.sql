with source as (
    select * from {{ source('ghcnd', 'ghcnd_stations') }}
)

select
    id                                              as station_id,
    left(id, 2)                                     as country_code,
    substr(id, 3, 1)                                as network_code,
    name                                            as station_name,
    state                                           as state_code,
    try_cast(latitude as double)                    as latitude,
    try_cast(longitude as double)                   as longitude,
    -- -999.9 is the documented "missing elevation" sentinel
    nullif(try_cast(elevation as double), -999.9)   as elevation_m,
    gsn_flag,
    hcn_crn_flag,
    wmo_id
from source
