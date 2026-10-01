{#
  Units are interpreted from the readme's own unit vocabulary ("tenths of mm",
  "tenths of degrees C", "hPa * 10", ...) so any element NOAA documents gets a
  scale factor and display unit without per-element code.
#}
with source as (
    select * from {{ source('ghcnd', 'ghcnd_elements') }}
),

interpreted as (
    select
        element,
        description,
        unit_text,
        case
            when unit_text ilike 'tenths of %' then 0.1
            when unit_text ilike '% * 10'      then 0.1
            else 1.0
        end::decimal(4, 2) as scale_factor,
        case
            when unit_text ilike '%degre_s C%'         then '°C'   -- readme has a "degress C" typo
            when unit_text ilike '%meters per second%' then 'm/s'
            when unit_text ilike 'hPa%'                then 'hPa'
            when unit_text ilike '%mm'                 then 'mm'
            when unit_text = 'degrees'                 then 'degrees'
            when unit_text = 'percent'                 then '%'
            when unit_text = 'presence flag'           then 'flag'
            when unit_text in ('cm', 'km', 'minutes')  then unit_text
            else 'unknown'
        end as unit
    from source
)

select
    *,
    -- categorised from the dictionary's own wording, not per-element lists
    case
        when unit = 'flag'                          then 'weather_type'
        when unit = '°C'                            then 'temperature'
        when description ilike '%snow%'             then 'snow'
        when description ilike '%precip%'           then 'precipitation'
        when description ilike '%wind%' or description ilike '%gust%' then 'wind'
        else 'other'
    end as element_category
from interpreted
