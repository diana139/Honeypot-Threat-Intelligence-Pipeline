-- un rand = o campanie: acelasi script de comenzi rulat cu aceeasi unealta SSH (hassh)
-- de pe cel putin `campaign_min_ips` IP-uri diferite
with sessions as (
    select * from {{ ref('fct_sessions') }}
    where campaign_key is not null
),

files as (
    select s.campaign_key, count(distinct e.file_key) as distinct_files
    from sessions s
    join {{ ref('fct_events') }} e using (session_id)
    where e.file_key is not null
    group by s.campaign_key
)

select
    s.campaign_key,
    s.hassh,
    s.command_sequence_hash,
    any_value(s.command_sequence) as example_command_sequence,
    any_value(s.commands) as commands_per_session,
    count(distinct s.attacker_key) as attackers,
    count(*) as sessions,
    min(s.started_at) as first_seen_at,
    max(s.started_at) as last_seen_at,
    count(distinct s.date_key) as active_days,
    sum(s.downloads) as downloads,
    coalesce(any_value(f.distinct_files), 0) as distinct_files,
    count(distinct s.ssh_key_fingerprint) as distinct_ssh_keys
from sessions s
left join files f using (campaign_key)
group by s.campaign_key, s.hassh, s.command_sequence_hash
having count(distinct s.attacker_key) >= {{ var('campaign_min_ips', 10) }}
