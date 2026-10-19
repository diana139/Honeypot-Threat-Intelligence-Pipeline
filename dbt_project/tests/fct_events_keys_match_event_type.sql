-- the optional keys in fct_events must be filled on exactly the right event types:
-- credential_key on login.*, file_key only on file_download / file_upload
select
    f.event_id,
    t.event_type,
    f.credential_key,
    f.file_key
from {{ ref('fct_events') }} f
join {{ ref('dim_event_type') }} t using (event_type_key)
where (t.event_type in ('cowrie.login.success', 'cowrie.login.failed')) != (f.credential_key is not null)
    or (f.file_key is not null
        and t.event_type not in ('cowrie.session.file_download', 'cowrie.session.file_upload'))
    or (f.file_key is null and t.event_type = 'cowrie.session.file_download')
