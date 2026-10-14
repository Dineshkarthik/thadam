-- DuckDB explorer for the edge-capture demo output (level 1: local Parquet).
--
--   make query
--   # or directly: duckdb -c ".read sql/explore.sql"   (from examples/edge-capture/)
--
-- The glob path derives from the data-directory layout:
--   data/sinks/local/ready/{dataset}/date=YYYY-MM-DD/hour=HH/*.parquet
-- Quarantine lives on its own subtree:
--   data/quarantine/local/ready/_quarantine/...
--
-- For S3 (level 2), load httpfs and point the same queries at
-- s3://thadam-demo/fleet/thadam/v1/edge-capture/{dataset}/...

------------ files and schema ------------------------------------------------------------
SELECT '== files per dataset ==' AS "──";
SELECT
    regexp_extract(file, 'ready/([^/]+)/', 1) AS dataset,
    count(*)                                  AS files
FROM glob('./data/sinks/local/ready/**/*.parquet')
GROUP BY ALL
ORDER BY dataset;

SELECT '== telemetry column layout ==' AS "──";
SELECT column_name, column_type
FROM (DESCRIBE SELECT * FROM read_parquet('./data/sinks/local/ready/telemetry/**/*.parquet',
                                          hive_partitioning = true) LIMIT 0);

------------ row counts ------------------------------------------------------------------
SELECT '== rows per dataset ==' AS "──";
SELECT 'telemetry' AS dataset, count(*) AS rows FROM read_parquet('./data/sinks/local/ready/telemetry/**/*.parquet')
UNION ALL SELECT 'joints',  count(*) FROM read_parquet('./data/sinks/local/ready/joints/**/*.parquet')
UNION ALL SELECT 'imu',     count(*) FROM read_parquet('./data/sinks/local/ready/imu/**/*.parquet')
UNION ALL SELECT 'pose',    count(*) FROM read_parquet('./data/sinks/local/ready/pose/**/*.parquet')
UNION ALL SELECT 'state',   count(*) FROM read_parquet('./data/sinks/local/ready/state/**/*.parquet')
UNION ALL SELECT 'events',  count(*) FROM read_parquet('./data/sinks/local/ready/events/**/*.parquet')
ORDER BY dataset;

------------ ingest vs event time --------------------------------------------------------
SELECT '== source vs ingest time (robot-04 is deliberately +90s fast) ==' AS "──";
SELECT
    robot,
    count(*)                                   AS rows,
    min(_event_time)                           AS first_event,
    max(_event_time)                           AS last_event,
    round(avg(epoch(_event_time) - epoch(_ingest_time)), 1) AS mean_event_minus_ingest_s
FROM read_parquet('./data/sinks/local/ready/telemetry/**/*.parquet')
GROUP BY robot
ORDER BY robot;

------------ quality ---------------------------------------------------------------------
SELECT '== quality status per dataset ==' AS "──";
SELECT 'telemetry' AS dataset, _quality_status, count(*) AS rows, count(_quality_reason) AS explained
FROM read_parquet('./data/sinks/local/ready/telemetry/**/*.parquet')
GROUP BY ALL
UNION ALL
SELECT 'imu', _quality_status, count(*), count(_quality_reason)
FROM read_parquet('./data/sinks/local/ready/imu/**/*.parquet')
GROUP BY ALL
ORDER BY dataset, _quality_status;

SELECT '== flagged rows (validate/chaos: still delivered, never silently dropped) ==' AS "──";
SELECT robot, _quality_flags, count(*) AS rows, any_value(_quality_reason) AS example_reason
FROM read_parquet('./data/sinks/local/ready/telemetry/**/*.parquet')
WHERE _quality_flags <> 0
GROUP BY ALL
ORDER BY rows DESC
LIMIT 20;

------------ fleet behavior ---------------------------------------------------------------
SELECT '== states per robot ==' AS "──";
SELECT robot, mode, count(*) AS rows, min(_ingest_time) AS first_seen, max(_ingest_time) AS last_seen
FROM read_parquet('./data/sinks/local/ready/state/**/*.parquet')
GROUP BY ALL
ORDER BY robot, first_seen;

SELECT '== events by type and level (level debug must be absent: the filter transform) ==' AS "──";
SELECT event_type, level, count(*) AS rows
FROM read_parquet('./data/sinks/local/ready/events/**/*.parquet')
GROUP BY ALL
ORDER BY rows DESC;

------------ joints: one robot's recent pose trace ---------------------------------------
SELECT '== robot-01 pose trace (last 5 samples) ==' AS "──";
SELECT robot, seq, sample_idx, _event_time, round(x_m, 3) AS x_m, round(y_m, 3) AS y_m
FROM read_parquet('./data/sinks/local/ready/pose/**/*.parquet')
WHERE robot = 'robot-01'
ORDER BY _event_time DESC
LIMIT 5;

------------ quarantine --------------------------------------------------------------------
-- If the second query errors with "No files found", nothing was quarantined yet —
-- the simulator's malformed-rate chaos produces a steady trickle (and F8 bursts).
SELECT '== quarantine files ==' AS "──";
SELECT count(*) AS quarantine_files FROM glob('./data/quarantine/local/ready/**/*.parquet');

SELECT '== quarantined messages by reason ==' AS "──";
SELECT error_domain, error_code, reason, count(*) AS rows
FROM read_parquet('./data/quarantine/local/ready/**/*.parquet')
GROUP BY ALL
ORDER BY rows DESC
LIMIT 20;
