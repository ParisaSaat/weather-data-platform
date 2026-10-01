select
    element,
    any_value(description)        as description,
    any_value(unit)               as unit,
    any_value(scale_factor)       as scale_factor,
    any_value(element_category)   as element_category,
    bool_and(is_documented)       as is_documented,
    count(distinct station_id)    as stations_reporting
from {{ ref('int_station_elements_selected') }}
group by element
