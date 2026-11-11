-- =============================================================================
-- Gold conformed dimensions (Kimball), built from the health_insurance industry
-- data model (3NF silver). Type 1 (current state) dimensions keyed on the silver
-- natural keys. Coverage history is carried by fact_member_month, which is built
-- from effective-dated eligibility spans, so member attributes that change over
-- time (plan, LOB, group) are analysed through the fact, not an SCD2 dim.
--
-- ${silver_catalog} is substituted from the pipeline configuration.
-- Direct identifiers (name, SSN, full DOB, street address, phone, email) are
-- deliberately NOT carried into gold.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- dim_date: one row per calendar day
-- -----------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW dim_date (
  CONSTRAINT pk_not_null EXPECT (calendar_date IS NOT NULL) ON VIOLATION FAIL UPDATE
)
COMMENT 'Calendar dimension, one row per day. Join facts on calendar_date or month_start.'
AS
SELECT
  d                                         AS calendar_date,
  CAST(date_format(d, 'yyyyMMdd') AS INT)   AS date_key,
  year(d)                                   AS year,
  quarter(d)                                AS quarter,
  concat(year(d), '-Q', quarter(d))         AS year_quarter,
  month(d)                                  AS month,
  date_format(d, 'MMMM')                    AS month_name,
  date_format(d, 'yyyy-MM')                 AS year_month,
  trunc(d, 'MM')                            AS month_start,
  last_day(d)                               AS month_end,
  dayofmonth(d)                             AS day_of_month,
  dayofweek(d)                              AS day_of_week,
  date_format(d, 'EEEE')                    AS day_name,
  dayofweek(d) IN (1, 7)                    AS is_weekend
FROM (
  SELECT explode(sequence(DATE'${date_dim_start}', DATE'${date_dim_end}', INTERVAL 1 DAY)) AS d
);

-- -----------------------------------------------------------------------------
-- dim_member: one row per insured individual (member.identity: subscriber or
-- dependent), enriched with subscriber, current LOB segmentation and current PCP.
-- -----------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW dim_member (
  CONSTRAINT pk_not_null EXPECT (member_id IS NOT NULL) ON VIOLATION FAIL UPDATE
)
COMMENT 'Member dimension (Type 1). Grain: one row per member.identity. No direct identifiers.'
AS
WITH current_lob AS (
  SELECT *
  FROM ${silver_catalog}.member.lob_assignment
  QUALIFY row_number() OVER (
    PARTITION BY identity_id
    ORDER BY CASE WHEN lob_assignment_status = 'active' THEN 0 ELSE 1 END,
             effective_date DESC NULLS LAST,
             updated_timestamp DESC NULLS LAST
  ) = 1
),
current_pcp AS (
  -- PCP is assigned at the subscriber level in the model; dependents inherit it.
  SELECT *
  FROM ${silver_catalog}.member.pcp_assignment
  QUALIFY row_number() OVER (
    PARTITION BY subscriber_id
    ORDER BY CASE WHEN is_current THEN 0 ELSE 1 END,
             CASE WHEN is_primary THEN 0 ELSE 1 END,
             effective_date DESC NULLS LAST
  ) = 1
)
SELECT
  i.identity_id                                        AS member_id,
  i.subscriber_id,
  i.member_type,
  i.relationship_to_subscriber,
  i.member_status,
  i.eligibility_status,
  i.lob                                                AS member_lob,
  i.gender,
  year(i.date_of_birth)                                AS birth_year,
  CAST(floor(months_between(current_date(), i.date_of_birth) / 12) AS INT) AS age,
  CASE
    WHEN i.date_of_birth IS NULL THEN 'unknown'
    WHEN months_between(current_date(), i.date_of_birth) / 12 < 18 THEN '00-17'
    WHEN months_between(current_date(), i.date_of_birth) / 12 < 35 THEN '18-34'
    WHEN months_between(current_date(), i.date_of_birth) / 12 < 50 THEN '35-49'
    WHEN months_between(current_date(), i.date_of_birth) / 12 < 65 THEN '50-64'
    ELSE '65+'
  END                                                  AS age_band,
  i.race,
  i.ethnicity,
  i.language_preference,
  i.state,
  left(i.postal_code, 3)                               AS zip3,
  i.coverage_start_date,
  i.coverage_end_date,
  i.health_plan_id                                     AS current_health_plan_id,
  i.group_id                                           AS current_group_id,
  s.coverage_type                                      AS subscriber_coverage_type,
  s.line_of_business                                   AS subscriber_line_of_business,
  s.subscriber_status,
  l.lob_code                                           AS current_lob_code,
  l.lob_description                                    AS current_lob_description,
  l.dual_eligible_flag,
  l.snp_type,
  l.risk_tier,
  l.hcc_risk_score_tier,
  l.chronic_condition_flag,
  l.sdoh_risk_flag,
  l.star_rating_cohort,
  i.risk_score,
  p.provider_id                                        AS current_pcp_provider_id,
  i.updated_timestamp                                  AS source_updated_timestamp
FROM ${silver_catalog}.member.identity i
LEFT JOIN ${silver_catalog}.member.subscriber s ON i.subscriber_id = s.subscriber_id
LEFT JOIN current_lob l                         ON i.identity_id   = l.identity_id
LEFT JOIN current_pcp p                         ON i.subscriber_id = p.subscriber_id;

-- -----------------------------------------------------------------------------
-- dim_provider: one row per provider, with current primary specialty
-- -----------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW dim_provider (
  CONSTRAINT pk_not_null EXPECT (provider_id IS NOT NULL) ON VIOLATION FAIL UPDATE
)
COMMENT 'Provider dimension (Type 1). Grain: one row per provider.provider, with current primary specialty.'
AS
WITH primary_specialty AS (
  SELECT *
  FROM ${silver_catalog}.provider.specialty
  QUALIFY row_number() OVER (
    PARTITION BY provider_id
    ORDER BY CASE WHEN current_record_flag THEN 0 ELSE 1 END,
             CASE WHEN lower(specialty_type) = 'primary' THEN 0 ELSE 1 END,
             record_effective_date DESC NULLS LAST
  ) = 1
)
SELECT
  p.provider_id,
  p.npi,
  p.provider_name,
  p.provider_type,
  p.provider_category,
  p.taxonomy_code,
  p.taxonomy_description,
  p.primary_specialty                                  AS primary_specialty_text,
  sp.specialty_code                                    AS primary_specialty_code,
  sp.specialty_name                                    AS primary_specialty_name,
  sp.specialty_category                                AS primary_specialty_category,
  sp.pcp_eligible_flag,
  sp.board_certified_flag,
  sp.accepting_new_patients_flag,
  sp.telehealth_enabled_flag,
  p.participation_status,
  p.network_participation_flag,
  p.credentialing_status,
  p.city,
  p.state,
  left(p.postal_code, 5)                               AS zip5,
  p.effective_start_date,
  p.effective_end_date,
  p.termination_date,
  p.updated_timestamp                                  AS source_updated_timestamp
FROM ${silver_catalog}.provider.provider p
LEFT JOIN primary_specialty sp ON p.provider_id = sp.provider_id;

-- -----------------------------------------------------------------------------
-- dim_plan: one row per health plan product
-- -----------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW dim_plan (
  CONSTRAINT pk_not_null EXPECT (health_plan_id IS NOT NULL) ON VIOLATION FAIL UPDATE
)
COMMENT 'Health plan / product dimension (Type 1). Grain: one row per plan.health_plan.'
AS
SELECT
  health_plan_id,
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
  plan_marketplace_eligible,
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
  updated_timestamp                                    AS source_updated_timestamp
FROM ${silver_catalog}.plan.health_plan;
