{#
  Element selection is driven by ghcnd-inventory.txt: a (station, element) pair is in
  scope when the inventory says the station reports it during the analysis window.
  Config include/exclude globs are applied as policy on top.
#}
with inventory as (
    select * from {{ ref('stg_ghcnd__inventory') }}
),

targets as (
    select station_id from {{ ref('int_target_stations') }}
),

window_years as (
    select year(window_start) as start_year, year(window_end) as end_year
    from {{ ref('int_analysis_window') }}
),

elements as (
    select * from {{ ref('stg_ghcnd__elements') }}
)

select
    i.station_id,
    i.element,
    i.first_year,
    i.last_year,
    e.element is not null                   as is_documented,
    coalesce(e.description, i.element)      as description,
    coalesce(e.unit, 'unknown')             as unit,
    coalesce(e.scale_factor, 1.0)           as scale_factor,
    coalesce(e.element_category, 'other')   as element_category
from inventory as i
inner join targets as t using (station_id)
cross join window_years as w
left join elements as e using (element)
where i.first_year <= w.end_year
  and i.last_year >= w.start_year
  and {{ element_selection_predicate('i.element') }}
