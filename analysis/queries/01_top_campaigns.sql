-- The largest campaigns: same SSH tool (hassh) + same script, by number of IPs
select campaign_key, hassh, attackers, sessions, active_days, distinct_files, example_command_sequence
from gold.campaigns
order by attackers desc;
