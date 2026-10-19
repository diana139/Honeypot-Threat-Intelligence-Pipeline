-- clientul SSH al atacatorului: versiunea (din client.version) + fingerprint-ul hassh (din client.kex).
-- Fiecare sesiune are cel mult o versiune si un hassh, deci clientul se stabileste la nivel de sesiune.
with sessions as (
    select
        session_id,
        max(client_version) as client_version,
        max(hassh) as hassh,
        max(hassh_algorithms) as hassh_algorithms,
        max(event_ts) as last_ts,
        min(event_ts) as first_ts
    from {{ ref('stg_cowrie_events') }}
    group by session_id
    having max(client_version) is not null or max(hassh) is not null
)

select
    md5(concat_ws('|', coalesce(client_version, '<null>'), coalesce(hassh, '<null>'))) as client_key,
    client_version,
    hassh,
    any_value(hassh_algorithms) as hassh_algorithms,
    min(first_ts) as first_seen_at,
    max(last_ts) as last_seen_at,
    count(*) as sessions
from sessions
group by client_version, hassh
