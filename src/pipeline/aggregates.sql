-- =============================================================================
-- Gold aggregates. agg_pmpm_monthly combines the member-month denominator with
-- claim-line numerators at month x LOB x plan grain, so PMPM ratios are
-- computed from a single source (metric view mv_pmpm reads this table).
-- Claims with no matching eligibility month land in line_of_business = 'unmatched'
-- with member_months = 0 so they stay visible instead of silently dropping.
-- =============================================================================

CREATE OR REFRESH MATERIALIZED VIEW agg_pmpm_monthly
COMMENT 'Monthly cost & utilization by LOB and plan. Grain: month_start x line_of_business x health_plan_id.'
AS
WITH mm AS (
  SELECT
    member_month_start                          AS month_start,
    coalesce(line_of_business, 'unknown')       AS line_of_business,
    coalesce(health_plan_id, -1)                AS health_plan_id,
    sum(member_months)                          AS member_months
  FROM fact_member_month
  GROUP BY ALL
),
clm AS (
  SELECT
    incurred_month                              AS month_start,
    CASE WHEN health_plan_id IS NULL THEN 'unmatched'
         ELSE coalesce(line_of_business, 'unknown') END AS line_of_business,
    coalesce(health_plan_id, -1)                AS health_plan_id,
    sum(paid_amount)                            AS paid_amount,
    sum(allowed_amount)                         AS allowed_amount,
    sum(billed_amount)                          AS billed_amount,
    sum(member_responsibility_amount)           AS member_responsibility_amount,
    count(DISTINCT claim_header_id)             AS claim_count,
    count(*)                                    AS claim_line_count,
    count(DISTINCT CASE WHEN is_emergency THEN claim_header_id END) AS er_visit_count,
    count(DISTINCT CASE WHEN is_inpatient THEN claim_header_id END) AS inpatient_admit_count
  FROM fact_claim_line
  WHERE incurred_month IS NOT NULL
    AND coalesce(line_status, '') <> 'reversed'
  GROUP BY ALL
)
SELECT
  coalesce(mm.month_start, clm.month_start)                AS month_start,
  coalesce(mm.line_of_business, clm.line_of_business)      AS line_of_business,
  coalesce(mm.health_plan_id, clm.health_plan_id)          AS health_plan_id,
  coalesce(mm.member_months, 0)                            AS member_months,
  coalesce(clm.paid_amount, 0)                             AS paid_amount,
  coalesce(clm.allowed_amount, 0)                          AS allowed_amount,
  coalesce(clm.billed_amount, 0)                           AS billed_amount,
  coalesce(clm.member_responsibility_amount, 0)            AS member_responsibility_amount,
  coalesce(clm.claim_count, 0)                             AS claim_count,
  coalesce(clm.claim_line_count, 0)                        AS claim_line_count,
  coalesce(clm.er_visit_count, 0)                          AS er_visit_count,
  coalesce(clm.inpatient_admit_count, 0)                   AS inpatient_admit_count,
  try_divide(coalesce(clm.paid_amount, 0), mm.member_months)    AS paid_pmpm,
  try_divide(coalesce(clm.allowed_amount, 0), mm.member_months) AS allowed_pmpm
FROM mm
FULL OUTER JOIN clm
  ON  mm.month_start      = clm.month_start
  AND mm.line_of_business = clm.line_of_business
  AND mm.health_plan_id   = clm.health_plan_id;
