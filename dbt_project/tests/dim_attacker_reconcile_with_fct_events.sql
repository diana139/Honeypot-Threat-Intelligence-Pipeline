-- fiecare eveniment e atribuit exact unui atacator, deci totalul pe dim_attacker
-- trebuie sa fie egal cu numarul de evenimente din fct_events
with counts as (
    select
        (select count(*) from {{ ref('fct_events') }}) as events_expected,
        (select sum(events) from {{ ref('dim_attacker') }}) as events_actual
)

select *
from counts
where events_expected != events_actual
