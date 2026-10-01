with source as (
    select * from {{ source('ghcnd', 'ghcnd_inventory') }}
)

select
    id                               as station_id,
    element,
    try_cast(firstyear as integer)   as first_year,
    try_cast(lastyear as integer)    as last_year
from source
