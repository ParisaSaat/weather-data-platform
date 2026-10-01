-- Target-station observations inside the window, for selected elements, scaled
-- to real units and annotated with quality outcome. Grain: station x date x element.
with observations as (
    select * from {{ ref('stg_ghcnd__daily_observations') }}
),

selected as (
    select * from {{ ref('int_station_elements_selected') }}
),

analysis_window as (
    select * from {{ ref('int_analysis_window') }}
),

unit_corrections as (
    select * from {{ ref('source_unit_corrections') }}
),

scoped as (
    select
        o.station_id,
        o.observation_date,
        o.element,
        o.raw_value,
        round(o.raw_value * coalesce(c.scale_factor, s.scale_factor), 2)  as value,
        s.unit,
        c.element is not null                           as is_unit_corrected,
        s.element_category,
        o.measurement_flag,
        o.quality_flag,
        o.source_flag,
        {{ is_quality_rejected('o.quality_flag') }}     as is_quality_rejected,
        coalesce(o.measurement_flag = 'T', false)       as is_trace,
        o.batch_id,
        o.loaded_at
    from observations as o
    inner join selected as s using (station_id, element)
    left join unit_corrections as c
        on c.element = o.element
       and c.source_flag = o.source_flag
    cross join analysis_window as w
    where o.observation_date between w.window_start and w.window_end
)

select
    *,
    case when not is_quality_rejected and raw_value is not null then value end  as value_clean
from scoped
