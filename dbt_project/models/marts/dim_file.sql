-- files downloaded/uploaded by attackers.
-- Note: shasum also appears on cowrie.log.closed, but there it is the hash of the ttylog, not an attacker file.
select
    md5(file_sha256) as file_key,
    file_sha256,
    min(event_ts) as first_seen_at,
    max(event_ts) as last_seen_at,
    count(*) filter (where event_type = 'cowrie.session.file_download') as downloads,
    count(*) filter (where event_type = 'cowrie.session.file_upload') as uploads,
    count(distinct download_url) as distinct_urls,
    arg_min(download_url, event_ts) filter (where download_url is not null) as first_url
from {{ ref('stg_cowrie_events') }}
where event_type in ('cowrie.session.file_download', 'cowrie.session.file_upload')
    and file_sha256 is not null
group by file_sha256
