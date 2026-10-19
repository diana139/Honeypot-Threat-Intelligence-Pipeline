-- one row = one Cowrie event
with events as (
    select
        *,
        -- the SSH client belongs to the session, so it is set on all of the session's events
        max(client_version) over (partition by session_id) as session_client_version,
        max(hassh) over (partition by session_id) as session_hassh
    from {{ ref('stg_cowrie_events') }}
)

select
    event_id,
    session_id,
    event_ts,

    -- dimension keys
    cast(strftime(event_date, '%Y%m%d') as integer) as date_key,
    md5(src_ip) as attacker_key,
    md5(event_type) as event_type_key,
    case when session_client_version is not null or session_hassh is not null
        then md5(concat_ws('|', coalesce(session_client_version, '<null>'), coalesce(session_hassh, '<null>')))
    end as client_key,
    case when event_type in ('cowrie.login.success', 'cowrie.login.failed')
        then md5(concat_ws('|', username, coalesce(password, '<null>')))
    end as credential_key,
    case when event_type in ('cowrie.session.file_download', 'cowrie.session.file_upload') and file_sha256 is not null
        then md5(file_sha256)
    end as file_key,

    -- degenerate attributes (specific to the event, no dimension of their own)
    command_input,
    download_url,
    destfile,               -- where the attacker saved the file on the honeypot
    ssh_key_fingerprint,    -- the public key used to authenticate
    tunnel_dst_ip,
    tunnel_dst_port,
    tunnel_data,            -- the request sent through the tunnel (usually HTTP)
    ja4,                    -- TLS fingerprint of the tunnel traffic
    ja4h,                   -- HTTP fingerprint of the tunnel traffic

    -- measures
    duration_ms
from events
