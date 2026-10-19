-- a campaign is the same script run from at least `campaign_min_ips` different IPs
select
    campaign_key,
    attackers
from {{ ref('campaigns') }}
where attackers < {{ var('campaign_min_ips', 10) }}
    or sessions < attackers
    or last_seen_at < first_seen_at
