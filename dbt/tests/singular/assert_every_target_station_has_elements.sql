-- A configured station with no in-scope elements means it doesn't report during the
-- window (or the element filters exclude everything). Fail loudly: it's a config problem.
select t.station_id, t.city, t.inventory_last_year
from {{ ref('int_target_stations') }} as t
left join {{ ref('int_station_elements_selected') }} as e using (station_id)
where e.station_id is null
