-- =============================================================================
-- Gold Type 2 (SCD2) dimensions, derived from the silver history tables that the
-- hi-silver-scd2 pipeline maintains (${silver_catalog}.${silver_history_schema}).
--
--   <entity>_sk     surrogate key: xxhash64(natural key, version start). Stable
--                   across MV recomputes, unique per version.
--   effective_from  version start. The FIRST version of each key is back-dated to
--                   1900-01-01, because silver history only begins at go-live
--                   (CDF enablement); without this, facts dated before go-live
--                   would find no version. Later versions start at __START_AT.
--   effective_to    exclusive end: __END_AT, or 9999-12-31 for the open version.
--   is_current      __END_AT IS NULL (AUTO CDC has no current flag of its own).
--
-- Facts look a version up with:  ts >= effective_from AND ts < effective_to
-- The Type 1 dims (dim_member, dim_provider, dim_plan) remain for "as is now" reporting.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- dim_member_scd2: member.identity history
-- -----------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW dim_member_scd2 (
  CONSTRAINT sk_not_null  EXPECT (member_sk IS NOT NULL)  ON VIOLATION FAIL UPDATE,
  CONSTRAINT key_not_null EXPECT (member_id IS NOT NULL)  ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_window EXPECT (effective_from < effective_to)
)
COMMENT 'Member dimension, SCD Type 2. Grain: one row per version of member.identity. Join facts as-of with effective_from <= ts < effective_to. No direct identifiers.'
AS
WITH h AS (
  SELECT *,
         row_number() OVER (PARTITION BY identity_id ORDER BY __START_AT) AS version_number
  FROM ${silver_catalog}.${silver_history_schema}.member_identity_scd2
)
SELECT
  xxhash64(identity_id, __START_AT)                                  AS member_sk,
  identity_id                                                        AS member_id,
  version_number,
  CASE WHEN version_number = 1 THEN TIMESTAMP'1900-01-01' ELSE __START_AT END AS effective_from,
  coalesce(__END_AT, TIMESTAMP'9999-12-31')                          AS effective_to,
  __END_AT IS NULL                                                   AS is_current,
  __START_AT                                                         AS source_version_start,
  __END_AT                                                           AS source_version_end,
  subscriber_id,
  member_type,
  relationship_to_subscriber,
  member_status,
  eligibility_status,
  lob                                                                AS member_lob,
  gender,
  year(date_of_birth)                                                AS birth_year,
  race,
  ethnicity,
  language_preference,
  state,
  left(postal_code, 3)                                               AS zip3,
  coverage_start_date,
  coverage_end_date,
  health_plan_id,
  group_id,
  risk_score,
  source_commit_timestamp
FROM h;

-- -----------------------------------------------------------------------------
-- dim_provider_scd2: provider.provider history
-- (specialty is effective-dated in its own table and is not versioned here)
-- -----------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW dim_provider_scd2 (
  CONSTRAINT sk_not_null  EXPECT (provider_sk IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT key_not_null EXPECT (provider_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_window EXPECT (effective_from < effective_to)
)
COMMENT 'Provider dimension, SCD Type 2. Grain: one row per version of provider.provider.'
AS
WITH h AS (
  SELECT *,
         row_number() OVER (PARTITION BY provider_id ORDER BY __START_AT) AS version_number
  FROM ${silver_catalog}.${silver_history_schema}.provider_provider_scd2
)
SELECT
  xxhash64(provider_id, __START_AT)                                  AS provider_sk,
  provider_id,
  version_number,
  CASE WHEN version_number = 1 THEN TIMESTAMP'1900-01-01' ELSE __START_AT END AS effective_from,
  coalesce(__END_AT, TIMESTAMP'9999-12-31')                          AS effective_to,
  __END_AT IS NULL                                                   AS is_current,
  __START_AT                                                         AS source_version_start,
  __END_AT                                                           AS source_version_end,
  npi,
  provider_name,
  provider_type,
  provider_category,
  taxonomy_code,
  taxonomy_description,
  primary_specialty                                                  AS primary_specialty_text,
  participation_status,
  network_participation_flag,
  credentialing_status,
  city,
  state,
  left(postal_code, 5)                                               AS zip5,
  effective_start_date,
  effective_end_date,
  termination_date,
  source_commit_timestamp
FROM h;

-- -----------------------------------------------------------------------------
-- dim_plan_scd2: plan.health_plan history
-- -----------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW dim_plan_scd2 (
  CONSTRAINT sk_not_null  EXPECT (plan_sk IS NOT NULL)        ON VIOLATION FAIL UPDATE,
  CONSTRAINT key_not_null EXPECT (health_plan_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_window EXPECT (effective_from < effective_to)
)
COMMENT 'Health plan dimension, SCD Type 2. Grain: one row per version of plan.health_plan.'
AS
WITH h AS (
  SELECT *,
         row_number() OVER (PARTITION BY health_plan_id ORDER BY __START_AT) AS version_number
  FROM ${silver_catalog}.${silver_history_schema}.plan_health_plan_scd2
)
SELECT
  xxhash64(health_plan_id, __START_AT)                               AS plan_sk,
  health_plan_id,
  version_number,
  CASE WHEN version_number = 1 THEN TIMESTAMP'1900-01-01' ELSE __START_AT END AS effective_from,
  coalesce(__END_AT, TIMESTAMP'9999-12-31')                          AS effective_to,
  __END_AT IS NULL                                                   AS is_current,
  __START_AT                                                         AS source_version_start,
  __END_AT                                                           AS source_version_end,
  plan_code,
  plan_name,
  plan_type,
  plan_category,
  plan_year,
  line_of_business,
  market_segment,
  regulatory_classification,
  plan_state,
  plan_region,
  network_tier,
  cms_plan_code,
  hios_plan_code,
  plan_aca_compliant,
  is_exempt_from_mlr,
  deductible_individual,
  deductible_family,
  out_of_pocket_max_individual,
  out_of_pocket_max_family,
  copay_primary_care,
  copay_specialist,
  coinsurance_percentage,
  plan_status,
  effective_date,
  termination_date,
  source_commit_timestamp
FROM h;
