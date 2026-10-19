-- Sessions per day, and how many of them ran commands
select d.date_day,
       count(*) as sessions,
       count(*) filter (where s.commands > 0) as with_commands,
       count(*) filter (where s.campaign_key in (select campaign_key from gold.campaigns)) as in_campaigns
from gold.fct_sessions s
join gold.dim_date d using (date_key)
group by 1
order by 1;
