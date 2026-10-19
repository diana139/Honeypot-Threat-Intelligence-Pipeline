-- reguli de bun simt pentru o sesiune
select *
from {{ ref('fct_sessions') }}
where ended_at < started_at
    or events < 1
    or login_attempts > events
    or commands > events
    -- o sesiune cu login reusit are mereu credentialele folosite
    or (login_success and credential_key is null)
    or (not login_success and credential_key is not null)
    -- campaign_key exista doar daca sesiunea a rulat comenzi
    or ((commands > 0) != (campaign_key is not null))
