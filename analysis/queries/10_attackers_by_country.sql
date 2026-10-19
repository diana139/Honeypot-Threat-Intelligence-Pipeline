-- Countries the attacks come from (IP geolocation from IPinfo Lite)
select country, count(*) as attackers, sum(sessions) as sessions, sum(commands) as commands
from gold.dim_attacker
group by all
order by attackers desc
limit 20;
