-- The most run commands and by how many attackers
select f.command_input, count(*) as executions, count(distinct f.attacker_key) as attackers
from gold.fct_events f
join gold.dim_event_type t using (event_type_key)
where t.event_type = 'cowrie.command.input'
group by 1
order by executions desc
limit 20;
