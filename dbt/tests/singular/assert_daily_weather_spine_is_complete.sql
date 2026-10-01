-- The wide table must contain exactly one row per target station per day in the window.
with expected as (
    select (select count(*) from {{ ref('dim_stations') }}) * window_days as n
    from {{ ref('int_analysis_window') }}
),
actual as (
    select count(*) as n from {{ ref('fct_daily_weather') }}
)
select expected.n as expected_rows, actual.n as actual_rows
from expected, actual
where expected.n <> actual.n
