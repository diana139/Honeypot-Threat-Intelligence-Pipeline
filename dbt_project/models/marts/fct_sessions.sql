-- un rand = o sesiune (o conexiune a unui atacator)
with events as (
    select
        f.*,
        t.event_type
    from {{ ref('fct_events') }} f
    join {{ ref('dim_event_type') }} t using (event_type_key)
),

sessions as (
    select
        session_id,
        any_value(attacker_key) as attacker_key,
        any_value(client_key) as client_key,
        min(event_ts) as started_at,
        max(event_ts) as ended_at,
        max(duration_ms) filter (where event_type = 'cowrie.session.closed') as duration_ms,
        count(*) as events,

        -- login
        count(*) filter (where event_type in ('cowrie.login.success', 'cowrie.login.failed')) as login_attempts,
        bool_or(event_type = 'cowrie.login.success') as login_success,
        arg_max(credential_key, event_ts) filter (where event_type = 'cowrie.login.success') as credential_key,

        -- comenzi: lista lor in ordinea rularii = "reteta" sesiunii
        count(*) filter (where event_type = 'cowrie.command.input') as commands,
        string_agg(command_input, ' ; ' order by event_ts) filter (where event_type = 'cowrie.command.input') as command_sequence,
        -- aceeasi lista, cu cifrele si sirurile hex lungi inlocuite cu N,
        -- ca scripturile care pun valori aleatoare (parole, nume de fisiere) sa dea acelasi hash
        md5(string_agg(regexp_replace(command_input, '[0-9a-fA-F]{6,}|[0-9]+', 'N', 'g'), ' ; ' order by event_ts)
            filter (where event_type = 'cowrie.command.input')) as command_sequence_hash,

        -- fisiere si tunel
        count(*) filter (where event_type = 'cowrie.session.file_download') as downloads,
        count(*) filter (where event_type = 'cowrie.session.file_upload') as uploads,
        count(*) filter (where event_type = 'cowrie.direct-tcpip.request') as tunnel_requests,
        any_value(ssh_key_fingerprint) as ssh_key_fingerprint
    from events
    group by session_id
)

select
    s.session_id,
    cast(strftime(s.started_at, '%Y%m%d') as integer) as date_key,
    s.attacker_key,
    s.client_key,
    s.credential_key,
    -- semnatura campaniei: aceeasi unealta (hassh) + acelasi script (secventa de comenzi)
    -- Doar semnaturile vazute de pe destule IP-uri ajung in campaigns
    case when s.command_sequence_hash is not null
        then md5(concat_ws('|', coalesce(c.hassh, '<null>'), s.command_sequence_hash))
    end as campaign_key,
    c.hassh,
    s.started_at,
    s.ended_at,
    s.duration_ms,
    s.events,
    s.login_attempts,
    s.login_success,
    s.commands,
    s.command_sequence,
    s.command_sequence_hash,
    s.downloads,
    s.uploads,
    s.tunnel_requests,
    s.ssh_key_fingerprint
from sessions s
left join {{ ref('dim_client') }} c using (client_key)
