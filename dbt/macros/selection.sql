{#
  Config-driven predicates. The lists come from config/pipeline.yaml via --vars,
  so policy changes never require editing model SQL.
#}

{% macro element_selection_predicate(column) -%}
    {%- set include = var('element_include_patterns') -%}
    {%- set exclude = var('element_exclude_patterns') -%}
    (
        {%- for pattern in include %}
        {{ column }} glob '{{ pattern }}'{% if not loop.last %} or{% endif %}
        {%- else %} false {%- endfor %}
    )
    {%- if exclude | length > 0 %}
    and not (
        {%- for pattern in exclude %}
        {{ column }} glob '{{ pattern }}'{% if not loop.last %} or{% endif %}
        {%- endfor %}
    )
    {%- endif %}
{%- endmacro %}


{% macro is_quality_rejected(column) -%}
    {%- set rejected = var('rejected_qflags') -%}
    {%- if '*' in rejected -%}
        ({{ column }} is not null)
    {%- elif rejected | length == 0 -%}
        false
    {%- else -%}
        coalesce({{ column }} in ({% for f in rejected %}'{{ f }}'{% if not loop.last %}, {% endif %}{% endfor %}), false)
    {%- endif -%}
{%- endmacro %}


{#
  Elements selected for the analysis (derived from ghcnd-inventory.txt in
  int_station_elements_selected). Used to generate the wide pivot at compile time.
  Returns [] when the relation doesn't exist yet (e.g. `dbt compile` on an empty DB).
#}
{% macro get_selected_elements() -%}
    {%- if not execute -%}
        {{ return([]) }}
    {%- endif -%}
    {%- set rel = ref('int_station_elements_selected') -%}
    {%- if adapter.get_relation(rel.database, rel.schema, rel.identifier) is none -%}
        {{ return([]) }}
    {%- endif -%}
    {%- set result = run_query("select distinct element from " ~ rel ~ " order by element") -%}
    {{ return(result.columns[0].values() | list) }}
{%- endmacro %}
