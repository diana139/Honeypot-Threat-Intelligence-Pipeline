-- The IPs of one campaign. Defaults to the largest campaign;
-- to pick another one, replace the subquery with a campaign_key from 01_top_campaigns.sql
set variable campaign_key = (select campaign_key from gold.campaigns order by attackers desc limit 1);

select a.src_ip, count(*) as sessions, min(s.started_at) as first_seen, max(s.started_at) as last_seen
from gold.fct_sessions s
join gold.dim_attacker a using (attacker_key)
where s.campaign_key = getvariable('campaign_key')
group by 1
order by 2 desc;
