-- The most tried username/password pairs; useful as a banned-password list for your own infrastructure
select username, password, attempts, successes
from gold.dim_credential
order by attempts desc
limit 50;
