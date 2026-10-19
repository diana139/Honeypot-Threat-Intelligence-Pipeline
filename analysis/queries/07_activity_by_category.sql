-- Events per day, by event category (session, client, login, command, log, direct-tcpip)
select d.date_day, t.event_category, count(*) as events
from gold.fct_events f
join gold.dim_date d using (date_key)
join gold.dim_event_type t using (event_type_key)
group by all
order by 1, 2;
