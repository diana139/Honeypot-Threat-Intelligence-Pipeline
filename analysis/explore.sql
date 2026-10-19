-- SELECT dt, count(*) AS events
-- FROM read_json_auto('s3://sundew-honeypot-diana/raw/dt=*/cowrie.json.gz', hive_partitioning=true)
-- GROUP BY dt ORDER BY dt;

-- SELECT count(DISTINCT src_ip) FROM read_json_auto('s3://sundew-honeypot-diana/raw/dt=*/cowrie.json.gz');

-- SELECT username, password, count(*) AS n
-- FROM read_json_auto('s3://sundew-honeypot-diana/raw/dt=*/cowrie.json.gz')
-- WHERE eventid LIKE 'cowrie.login%'
-- GROUP BY 1,2 ORDER BY n DESC LIMIT 20;

-- SELECT input, count(*) AS n
-- FROM read_json_auto('s3://sundew-honeypot-diana/raw/dt=*/cowrie.json.gz')
-- WHERE eventid = 'cowrie.command.input'
-- GROUP BY 1 ORDER BY n DESC LIMIT 20;


-- SELECT src_ip, count(*) AS n FROM read_json_auto('s3://sundew-honeypot-diana/raw/dt=*/cowrie.json.gz', hive_partitioning=true) WHERE dt = '2026-09-05' GROUP BY 1 ORDER BY n DESC LIMIT 10;

-- SELECT src_ip, hassh, count(*) FROM read_json_auto('s3://sundew-honeypot-diana/raw/dt=*/cowrie.json.gz') WHERE src_ip IN ('39.105.94.74','107.173.49.55') AND hassh IS NOT NULL GROUP BY 1,2;


SELECT src_ip, count(*) AS n FROM read_json_auto('s3://sundew-honeypot-diana/raw/dt=*/cowrie.json.gz') WHERE hassh = '0a07365cc01fa9fc82608ba4019af499' GROUP BY 1 ORDER BY n DESC;