{#
  Resolves the analysis window from the data instead of hardcoding dates.
  Exactly one row. See `analysis_window` in config/pipeline.yaml.
#}
{%- set anchor = var('window_anchor') -%}
{%- set lookback_days = var('window_lookback_days') | int -%}
{%- if anchor not in ['latest_common', 'latest_any', 'fixed'] -%}
    {{ exceptions.raise_compiler_error("Unknown window_anchor '" ~ anchor ~ "'") }}
{%- endif -%}
{%- if anchor == 'fixed' and not var('window_end_date') -%}
    {{ exceptions.raise_compiler_error("window_end_date is required when window_anchor is 'fixed'") }}
{%- endif -%}

with station_latest as (
    select last_observation_date
    from {{ ref('int_target_stations') }}
),

anchored as (
    select
        {%- if anchor == 'fixed' %}
        cast('{{ var("window_end_date") }}' as date)
        {%- elif anchor == 'latest_any' %}
        max(last_observation_date)
        {%- else %}
        min(last_observation_date)
        {%- endif %} as window_end
    from station_latest
)

select
    cast(window_end - interval '{{ lookback_days - 1 }} days' as date)  as window_start,
    window_end,
    '{{ anchor }}'                                                  as window_anchor,
    {{ lookback_days }}                                             as window_days
from anchored
