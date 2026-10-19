-- fct_events trebuie sa aiba exact un rand per eveniment din staging:
-- un join gresit catre o dimensiune ar dubla randuri, un filtru gresit le-ar pierde
with counts as (
    select
        (select count(*) from {{ ref('stg_cowrie_events') }}) as stg_rows,
        (select count(*) from {{ ref('fct_events') }}) as fct_rows
)

select *
from counts
where stg_rows != fct_rows
