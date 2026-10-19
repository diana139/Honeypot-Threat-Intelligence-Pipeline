-- o campanie inseamna acelasi script rulat de pe cel putin `campaign_min_ips` IP-uri diferite
select
    campaign_key,
    attackers
from {{ ref('campaigns') }}
where attackers < {{ var('campaign_min_ips', 10) }}
    or sessions < attackers
    or last_seen_at < first_seen_at
