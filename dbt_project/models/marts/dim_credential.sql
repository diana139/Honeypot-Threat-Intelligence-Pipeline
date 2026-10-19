-- username/password pairs tried at login (password can be missing on login.failed)
select
    md5(concat_ws('|', username, coalesce(password, '<null>'))) as credential_key,
    username,
    password,
    min(event_ts) as first_seen_at,
    count(*) as attempts,
    count(*) filter (where event_type = 'cowrie.login.success') as successes
from {{ ref('stg_cowrie_events') }}
where event_type in ('cowrie.login.success', 'cowrie.login.failed')
group by username, password
