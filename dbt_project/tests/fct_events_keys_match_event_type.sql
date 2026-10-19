-- cheile optionale din fct_events trebuie sa fie completate exact pe tipurile de evenimente potrivite:
-- credential_key pe login.*, file_key doar pe file_download / file_upload
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
