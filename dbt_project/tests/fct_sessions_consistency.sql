-- common-sense rules for a session
select *
from {{ ref('fct_sessions') }}
where ended_at < started_at
    or events < 1
    or login_attempts > events
    or commands > events
    -- a session with a successful login always has the credentials it used
    or (login_success and credential_key is null)
    or (not login_success and credential_key is not null)
    -- campaign_key exists only if the session ran commands
    or ((commands > 0) != (campaign_key is not null))
