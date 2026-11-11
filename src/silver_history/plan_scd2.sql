-- =============================================================================
-- Silver SCD Type 2 history (plan domain), Lakeflow SDP, SQL.
-- Source: Change Data Feed of the industry-model silver tables (Type 1, current
-- state). Each table gets a companion <domain>_<table>_scd2 streaming table in
-- the silver history schema; the model tables themselves are untouched, so
-- their PK/FK design stays intact.
--
-- * The first run seeds from the table's current contents (a CDF stream with no
--   startingVersion emits the current snapshot as inserts), so history starts at
--   go-live, not at true business effective dates.
-- * pipelines.reset.allowed = false: a pipeline full refresh does NOT wipe history.
-- * Audit timestamps are untracked: a change only to them updates the current
--   version in place instead of opening a new one.
-- * To add a table: copy a block, set KEYS and TRACK HISTORY EXCEPT, and add the
--   table to the scd2_source_tables bundle variable (enables CDF on it).
-- =============================================================================

-- plan.health_plan -> plan_health_plan_scd2
-- History of: benefit design, cost share, status by plan year. Key: health_plan_id.
CREATE OR REFRESH STREAMING TABLE plan_health_plan_scd2
COMMENT 'SCD Type 2 history of plan.health_plan (from Change Data Feed). __START_AT/__END_AT = source commit timestamps; __END_AT IS NULL = current version.'
TBLPROPERTIES ('pipelines.reset.allowed' = 'false');

CREATE FLOW plan_health_plan_scd2_cdc AS AUTO CDC INTO plan_health_plan_scd2
FROM (
  SELECT *, _commit_version AS _source_commit_version
  FROM STREAM ${silver_catalog}.plan.`health_plan` WITH (readChangeFeed = true)
  WHERE _change_type IN ('insert', 'update_postimage', 'delete')
)
KEYS (health_plan_id)
APPLY AS DELETE WHEN _change_type = 'delete'
SEQUENCE BY _commit_timestamp
COLUMNS * EXCEPT (_change_type, _commit_version, _commit_timestamp)
STORED AS SCD TYPE 2
TRACK HISTORY ON * EXCEPT (created_timestamp, updated_timestamp, _source_commit_version);

-- plan.benefit_package -> plan_benefit_package_scd2
-- History of: benefit package composition. Key: benefit_package_id.
CREATE OR REFRESH STREAMING TABLE plan_benefit_package_scd2
COMMENT 'SCD Type 2 history of plan.benefit_package (from Change Data Feed). __START_AT/__END_AT = source commit timestamps; __END_AT IS NULL = current version.'
TBLPROPERTIES ('pipelines.reset.allowed' = 'false');

CREATE FLOW plan_benefit_package_scd2_cdc AS AUTO CDC INTO plan_benefit_package_scd2
FROM (
  SELECT *, _commit_version AS _source_commit_version
  FROM STREAM ${silver_catalog}.plan.`benefit_package` WITH (readChangeFeed = true)
  WHERE _change_type IN ('insert', 'update_postimage', 'delete')
)
KEYS (benefit_package_id)
APPLY AS DELETE WHEN _change_type = 'delete'
SEQUENCE BY _commit_timestamp
COLUMNS * EXCEPT (_change_type, _commit_version, _commit_timestamp)
STORED AS SCD TYPE 2
TRACK HISTORY ON * EXCEPT (created_timestamp, updated_timestamp, _source_commit_version);
