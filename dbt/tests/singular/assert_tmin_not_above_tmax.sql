{{ config(severity='warn') }}
-- Internal consistency: a day's minimum temperature shouldn't exceed its maximum.
select tmin.station_id, tmin.observation_date, tmin.value_clean as tmin, tmax.value_clean as tmax
from {{ ref('fct_daily_observations') }} as tmin
inner join {{ ref('fct_daily_observations') }} as tmax
    on tmax.station_id = tmin.station_id
   and tmax.observation_date = tmin.observation_date
   and tmax.element = 'TMAX'
where tmin.element = 'TMIN'
  and tmin.value_clean > tmax.value_clean
