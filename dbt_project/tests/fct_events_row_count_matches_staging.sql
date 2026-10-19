-- fct_events must have exactly one row per staging event:
-- a bad join to a dimension would duplicate rows, a bad filter would drop them
with counts as (
    select
        (select count(*) from {{ ref('stg_cowrie_events') }}) as stg_rows,
        (select count(*) from {{ ref('fct_events') }}) as fct_rows
)

select *
from counts
where stg_rows != fct_rows
