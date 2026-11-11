-- =============================================================================
-- Silver SCD Type 2 history (provider domain), Lakeflow SDP, SQL.
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

-- provider.provider -> provider_provider_scd2
-- History of: participation, credentialing, specialty, location. Key: provider_id.
CREATE OR REFRESH STREAMING TABLE provider_provider_scd2
COMMENT 'SCD Type 2 history of provider.provider (from Change Data Feed). __START_AT/__END_AT = source commit timestamps; __END_AT IS NULL = current version.'
TBLPROPERTIES ('pipelines.reset.allowed' = 'false');

CREATE FLOW provider_provider_scd2_cdc AS AUTO CDC INTO provider_provider_scd2
FROM (
  SELECT *, _commit_version AS _source_commit_version
  FROM STREAM ${silver_catalog}.provider.`provider` WITH (readChangeFeed = true)
  WHERE _change_type IN ('insert', 'update_postimage', 'delete')
)
KEYS (provider_id)
APPLY AS DELETE WHEN _change_type = 'delete'
SEQUENCE BY _commit_timestamp
COLUMNS * EXCEPT (_change_type, _commit_version, _commit_timestamp)
STORED AS SCD TYPE 2
TRACK HISTORY ON * EXCEPT (created_timestamp, last_verified_timestamp, updated_timestamp, _source_commit_version);

-- provider.group_practice -> provider_group_practice_scd2
-- History of: group / IPA attributes and status. Key: group_practice_id.
CREATE OR REFRESH STREAMING TABLE provider_group_practice_scd2
COMMENT 'SCD Type 2 history of provider.group_practice (from Change Data Feed). __START_AT/__END_AT = source commit timestamps; __END_AT IS NULL = current version.'
TBLPROPERTIES ('pipelines.reset.allowed' = 'false');

CREATE FLOW provider_group_practice_scd2_cdc AS AUTO CDC INTO provider_group_practice_scd2
FROM (
  SELECT *, _commit_version AS _source_commit_version
  FROM STREAM ${silver_catalog}.provider.`group_practice` WITH (readChangeFeed = true)
  WHERE _change_type IN ('insert', 'update_postimage', 'delete')
)
KEYS (group_practice_id)
APPLY AS DELETE WHEN _change_type = 'delete'
SEQUENCE BY _commit_timestamp
COLUMNS * EXCEPT (_change_type, _commit_version, _commit_timestamp)
STORED AS SCD TYPE 2
TRACK HISTORY ON * EXCEPT (created_timestamp, updated_timestamp, _source_commit_version);

-- provider.facility -> provider_facility_scd2
-- History of: facility type, accreditation, status. Key: facility_id.
CREATE OR REFRESH STREAMING TABLE provider_facility_scd2
COMMENT 'SCD Type 2 history of provider.facility (from Change Data Feed). __START_AT/__END_AT = source commit timestamps; __END_AT IS NULL = current version.'
TBLPROPERTIES ('pipelines.reset.allowed' = 'false');

CREATE FLOW provider_facility_scd2_cdc AS AUTO CDC INTO provider_facility_scd2
FROM (
  SELECT *, _commit_version AS _source_commit_version
  FROM STREAM ${silver_catalog}.provider.`facility` WITH (readChangeFeed = true)
  WHERE _change_type IN ('insert', 'update_postimage', 'delete')
)
KEYS (facility_id)
APPLY AS DELETE WHEN _change_type = 'delete'
SEQUENCE BY _commit_timestamp
COLUMNS * EXCEPT (_change_type, _commit_version, _commit_timestamp)
STORED AS SCD TYPE 2
TRACK HISTORY ON * EXCEPT (record_created_timestamp, record_updated_timestamp, _source_commit_version);
