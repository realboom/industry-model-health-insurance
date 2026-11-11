-- =============================================================================
-- Silver SCD Type 2 history (employer domain), Lakeflow SDP, SQL.
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
-- * source_change_type / source_commit_timestamp / source_commit_version carry the
--   CDF metadata of the LAST change applied to the row (untracked, so audit-only
--   touches refresh them in place). source_change_type is 'insert' on a key's first
--   version, else 'update_postimage'; deletes close the version (__END_AT) and leave
--   no row of their own.
-- * There is no is_current column: AUTO CDC only maintains __START_AT/__END_AT.
--   Current version = __END_AT IS NULL (derive is_current when reading).
-- * To add a table: copy a block, set KEYS and TRACK HISTORY EXCEPT, and add the
--   table to the scd2_source_tables bundle variable (enables CDF on it).
-- =============================================================================

-- employer.group -> employer_group_scd2
-- History of: employer group account terms and status. Key: group_id.
CREATE OR REFRESH STREAMING TABLE employer_group_scd2
COMMENT 'SCD Type 2 history of employer.group (from Change Data Feed). __START_AT/__END_AT = commit timestamps of the changes that opened/closed the version; __END_AT IS NULL = current version. source_commit_timestamp = last time the source row was modified (incl. untracked audit-only updates).'
TBLPROPERTIES ('pipelines.reset.allowed' = 'false');

CREATE FLOW employer_group_scd2_cdc AS AUTO CDC INTO employer_group_scd2
FROM (
  SELECT *,
         _change_type      AS source_change_type,
         _commit_timestamp AS source_commit_timestamp,
         _commit_version   AS source_commit_version
  FROM STREAM ${silver_catalog}.employer.`group` WITH (readChangeFeed = true)
  WHERE _change_type IN ('insert', 'update_postimage', 'delete')
)
KEYS (group_id)
APPLY AS DELETE WHEN _change_type = 'delete'
SEQUENCE BY _commit_timestamp
COLUMNS * EXCEPT (_change_type, _commit_version, _commit_timestamp)
STORED AS SCD TYPE 2
TRACK HISTORY ON * EXCEPT (created_timestamp, updated_timestamp, source_change_type, source_commit_timestamp, source_commit_version);
