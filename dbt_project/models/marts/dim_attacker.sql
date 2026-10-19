-- un rand per IP sursa
select
    md5(src_ip) as attacker_key,
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
