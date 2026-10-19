-- un rand = un eveniment Cowrie
with events as (
    select
        *,
        -- clientul SSH e o proprietate a sesiunii, asa ca il atribuim tuturor evenimentelor din ea
        max(client_version) over (partition by session_id) as session_client_version,
        max(hassh) over (partition by session_id) as session_hassh
    from {{ ref('stg_cowrie_events') }}
)

select
    event_id,
    session_id,
    event_ts,

    -- chei catre dimensiuni
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

    -- atribute degenerate (specifice evenimentului, fara dimensiune proprie)
    command_input,
    download_url,
    destfile,               -- unde a salvat atacatorul fisierul pe honeypot
    ssh_key_fingerprint,    -- cheia publica folosita la autentificare
    tunnel_dst_ip,
    tunnel_dst_port,
    tunnel_data,            -- cererea trimisa prin tunel (de obicei HTTP)
    ja4,                    -- fingerprint TLS al traficului din tunel
    ja4h,                   -- fingerprint HTTP al traficului din tunel

    -- masuri
    duration_ms
from events
