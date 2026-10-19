with source as (
    select * from {{ source('bronze', 'cowrie_events') }}
),

staged as (
    select
        -- chei
        md5(session || '|' || cast("timestamp" as varchar) || '|' || eventid) as event_id,
        session as session_id,
        uuid as sensor_uuid,
        sensor as sensor_name,

        "timestamp" as event_ts,
        dt as event_date,

        eventid as event_type,         
        replace(eventid, 'cowrie.', '') as event_name,         
        -- rețea: src = atacatorul, dst = honeypot-ul
        -- (pe evenimentele direct-tcpip.*, dst e destinatia tunelului, vezi mai jos)
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
        -- autentificare cu cheie publica (client.fingerprint)
        fingerprint as ssh_key_fingerprint,
        "key" as ssh_public_key,
        "type" as ssh_key_type,
        arch,

        -- login
        username,
        password,

        -- comenzi
        input as command_input,

        -- fișiere
        url as download_url,
        shasum as file_sha256,
        outfile,
        destfile,
        filename,
        size as file_size_bytes,
        duplicate as is_duplicate_file,

        -- direct-tcpip: atacatorul foloseste honeypot-ul ca proxy catre dst_ip:dst_port
        case when eventid like 'cowrie.direct-tcpip.%' then dst_ip end as tunnel_dst_ip,
        case when eventid like 'cowrie.direct-tcpip.%' then dst_port end as tunnel_dst_port,
        orig_ip as tunnel_orig_ip,         -- originea declarata de client (de obicei 0.0.0.0 / 127.0.0.1)
        orig_port as tunnel_orig_port,
        "data" as tunnel_data,
        ja4,
        ja4h,

        -- diverse
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