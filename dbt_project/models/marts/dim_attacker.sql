-- one row per source IP
with attackers as (
    select
        src_ip,
        min(event_ts) as first_seen_at,
        max(event_ts) as last_seen_at,
        count(distinct event_date) as active_days,
        count(distinct session_id) as sessions,
        count(*) as events,
        count(*) filter (where event_type = 'cowrie.login.success') as successful_logins,
        count(*) filter (where event_type = 'cowrie.command.input') as commands,
        count(*) filter (where event_type = 'cowrie.session.file_download') as downloads
    from {{ ref('stg_cowrie_events') }}
    group by src_ip
)

select
    md5(a.src_ip) as attacker_key,
    a.*,
    -- NULL until the IP is enriched (a new IP, or a run without IPINFO_TOKEN)
    geo.country_code,
    geo.country,
    geo.continent,
    geo.asn,
    geo.as_name,
    geo.as_domain
from attackers a
left join {{ source('enrichment', 'ipinfo') }} geo on geo.ip = a.src_ip
