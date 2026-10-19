-- numarul de sesiuni si de IP-uri din campaigns trebuie sa se regaseasca in fct_sessions
with expected as (
    select
        campaign_key,
        count(*) as sessions,
        count(distinct attacker_key) as attackers
    from {{ ref('fct_sessions') }}
    where campaign_key in (select campaign_key from {{ ref('campaigns') }})
    group by campaign_key
)

select
    c.campaign_key,
    c.sessions,
    e.sessions as expected_sessions,
    c.attackers,
    e.attackers as expected_attackers
from {{ ref('campaigns') }} c
left join expected e using (campaign_key)
where e.campaign_key is null
    or c.sessions != e.sessions
    or c.attackers != e.attackers
