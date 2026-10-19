-- Networks (ASN) the attacks come from: mostly cloud and hosting providers
select asn, as_name, as_domain, count(*) as attackers, sum(sessions) as sessions
from gold.dim_attacker
group by all
order by attackers desc
limit 20;
