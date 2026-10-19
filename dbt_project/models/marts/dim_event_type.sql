select distinct
    md5(event_type) as event_type_key,
    event_type,                                -- ex. cowrie.login.success
    event_name,                                -- ex. login.success
    split_part(event_name, '.', 1) as event_category   -- session, client, login, command, log, direct-tcpip
from {{ ref('stg_cowrie_events') }}
