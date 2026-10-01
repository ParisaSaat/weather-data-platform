select
    flag_type,
    case flag_type
        when 'M' then 'measurement'
        when 'Q' then 'quality'
        when 'S' then 'source'
    end                         as flag_type_name,
    nullif(code, '')            as flag_code,
    description                 as flag_description
from {{ source('ghcnd', 'ghcnd_flag_definitions') }}
