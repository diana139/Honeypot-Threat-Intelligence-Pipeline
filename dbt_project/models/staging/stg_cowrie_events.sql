with source as (
    select * from {{ source('bronze', 'cowrie_events') }}
),

staged as (
    select
        -- keys
        md5(session || '|' || cast("timestamp" as varchar) || '|' || eventid) as event_id,
        session as session_id,
        uuid as sensor_uuid,
        sensor as sensor_name,

        "timestamp" as event_ts,
        dt as event_date,

        eventid as event_type,

        -- network: src = the attacker, dst = the honeypot
        protocol,
        src_ip,
        src_port,
        dst_ip,
        dst_port,

        -- client SSH
        version as client_version,
        hassh,
        "hasshAlgorithms" as hassh_algorithms,
        "kexAlgs" as kex_algs,
        "keyAlgs" as key_algs,
        "encCS" as enc_cs,
        "macCS" as mac_cs,
        "compCS" as comp_cs,
        "langCS" as lang_cs,
        -- public key authentication (client.fingerprint)
        fingerprint as ssh_key_fingerprint,
        "key" as ssh_public_key,
        "type" as ssh_key_type,
        arch,

        -- login
        username,
        password,

        -- commands
        input as command_input,

        -- files
        url as download_url,
        shasum as file_sha256,
        outfile,
        destfile,
        filename,
        size as file_size_bytes,
        duplicate as is_duplicate_file,

        -- direct-tcpip: the attacker uses the honeypot as a proxy to dst_ip:dst_port
        case when eventid like 'cowrie.direct-tcpip.%' then dst_ip end as tunnel_dst_ip,
        case when eventid like 'cowrie.direct-tcpip.%' then dst_port end as tunnel_dst_port,
        orig_ip as tunnel_orig_ip, -- the origin the client claims (usually 0.0.0.0 / 127.0.0.1)
        orig_port as tunnel_orig_port,
        "data" as tunnel_data,
        ja4,
        ja4h,

        -- other
        duration_ms,
        message,
        ttylog,
        "name" as var_name,
        "value" as var_value,
        width,
        height,
        error
    from source
)

select * from staged