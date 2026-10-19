-- SSH tools (hassh) used by the most attackers
select cl.hassh, cl.client_version, count(distinct f.attacker_key) as attackers
from gold.fct_events f
join gold.dim_client cl using (client_key)
group by all
order by attackers desc
limit 10;
