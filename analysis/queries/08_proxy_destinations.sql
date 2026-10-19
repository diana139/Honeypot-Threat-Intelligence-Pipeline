-- Where attackers try to reach when using the honeypot as a proxy (direct-tcpip)
select tunnel_dst_ip, tunnel_dst_port, count(*) as requests, count(distinct attacker_key) as attackers
from gold.fct_events
where tunnel_dst_ip is not null
group by all
order by requests desc
limit 20;
