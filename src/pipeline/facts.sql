-- =============================================================================
-- Gold facts (Kimball) built from the health_insurance industry data model.
-- Unqualified names (fact_member_month, dim_*) resolve to datasets in this
-- pipeline; ${silver_catalog} is substituted from the pipeline configuration.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- fact_member_month: periodic snapshot. Grain: one row per member per month of
-- eligibility. Built by exploding member.member_eligibility_span into months.
-- When spans overlap in a month, the primary coverage (then the latest span) wins.
-- Months are limited to the dim_date range and to months that have started.
-- This is the PMPM denominator and the carrier of coverage history.
-- -----------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW fact_member_month (
  CONSTRAINT member_not_null EXPECT (member_id IS NOT NULL)           ON VIOLATION DROP ROW,
  CONSTRAINT month_not_null  EXPECT (member_month_start IS NOT NULL)  ON VIOLATION DROP ROW
)
COMMENT 'Member-month periodic snapshot. Grain: member_id x member_month_start. member_months = 1 per row.'
AS
WITH spans AS (
  SELECT
    s.*,
    trunc(s.effective_date, 'MM')                                        AS span_start_month,
    trunc(coalesce(s.termination_date, DATE'${date_dim_end}'), 'MM')     AS span_end_month
  FROM ${silver_catalog}.member.member_eligibility_span s
  WHERE s.identity_id IS NOT NULL
    AND s.effective_date IS NOT NULL
    AND coalesce(s.eligibility_status, '') <> 'pending'
),
months AS (
  -- calendar months from dim_date (range = date_dim_start .. date_dim_end)
  SELECT month_start FROM dim_date WHERE day_of_month = 1
),
exploded AS (
  -- join to the month calendar instead of explode(sequence(...)) so the MV stays
  -- eligible for incremental refresh; future months are excluded in WHERE (time
  -- functions are only incremental-safe in WHERE)
  SELECT m.month_start AS member_month_start, sp.*
  FROM spans sp
  JOIN months m
    ON m.month_start BETWEEN sp.span_start_month AND sp.span_end_month
  WHERE m.month_start <= current_date()
),
picked AS (
  SELECT
    member_month_start,
    identity_id                         AS member_id,
    health_plan_id,
    group_id,
    benefit_package_id,
    provider_network_id,
    line_of_business,
    coverage_type,
    enrollment_type,
    eligibility_status,
    is_primary_coverage,
    member_eligibility_span_id,
    1                                   AS member_months,
    premium_amount                      AS span_premium_amount
  FROM exploded
  QUALIFY row_number() OVER (
    PARTITION BY identity_id, member_month_start
    ORDER BY CASE WHEN is_primary_coverage THEN 0 ELSE 1 END,
             effective_date DESC,
             member_eligibility_span_id DESC
  ) = 1
)
-- as-of surrogate keys: dimension version in effect at the start of the month
SELECT
  p.*,
  dm.member_sk,
  dpl.plan_sk
FROM picked p
LEFT JOIN dim_member_scd2 dm
  ON  p.member_id = dm.member_id
  AND CAST(p.member_month_start AS TIMESTAMP) >= dm.effective_from
  AND CAST(p.member_month_start AS TIMESTAMP) <  dm.effective_to
LEFT JOIN dim_plan_scd2 dpl
  ON  p.health_plan_id = dpl.health_plan_id
  AND CAST(p.member_month_start AS TIMESTAMP) >= dpl.effective_from
  AND CAST(p.member_month_start AS TIMESTAMP) <  dpl.effective_to;

-- -----------------------------------------------------------------------------
-- fact_claim_line: transaction fact. Grain: one row per claim.line.
-- Conformed to member, provider, plan and date; plan/LOB are taken from the
-- member's eligibility in the incurred month (fact_member_month).
-- -----------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW fact_claim_line (
  CONSTRAINT line_not_null   EXPECT (claim_line_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT has_member      EXPECT (member_id IS NOT NULL),
  CONSTRAINT has_incurred_dt EXPECT (incurred_date IS NOT NULL),
  CONSTRAINT has_eligibility EXPECT (health_plan_id IS NOT NULL)
)
COMMENT 'Medical claim line fact. Grain: one row per claim.line. Amounts at line level, header attributes denormalised.'
AS
WITH principal_dx AS (
  SELECT header_id, diagnosis_code, icd_version, hcc_category
  FROM ${silver_catalog}.claim.diagnosis
  QUALIFY row_number() OVER (
    PARTITION BY header_id
    ORDER BY CASE WHEN diagnosis_type = 'principal' THEN 0 ELSE 1 END,
             try_cast(sequence AS INT) NULLS LAST,
             diagnosis_id
  ) = 1
),
first_payment AS (
  SELECT header_id, CAST(min(payment_date) AS DATE) AS paid_date
  FROM ${silver_catalog}.claim.payment
  WHERE coalesce(payment_status, '') NOT IN ('voided', 'returned', 'stopped')
    AND coalesce(is_voided, false) = false
  GROUP BY header_id
),
lines AS (
  SELECT
    l.line_id                                             AS claim_line_id,
    l.header_id                                           AS claim_header_id,
    h.claim_number,
    l.number                                              AS line_number,
    h.identity_id                                         AS member_id,
    h.subscriber_id,
    h.group_id                                            AS claim_group_id,
    h.provider_id                                         AS billing_provider_id,
    coalesce(l.provider_id, h.header_rendering_provider_id) AS rendering_provider_id,
    coalesce(l.service_date, h.admission_date)            AS incurred_date,
    fp.paid_date,
    h.claim_type,
    h.claim_status,
    h.claim_source,
    h.lob                                                 AS claim_benefit_lob,
    l.line_status,
    l.line_type,
    coalesce(l.place_of_service_code, h.place_of_service_code) AS place_of_service_code,
    l.procedure_code,
    l.revenue_code,
    l.modifier_1,
    l.ndc_code,
    dx.diagnosis_code                                     AS principal_diagnosis_code,
    dx.hcc_category                                       AS principal_hcc_category,
    h.drg_code,
    h.admission_date,
    h.discharge_date,
    coalesce(l.emergency_indicator, false)                AS is_emergency,
    (h.claim_type = 'institutional' AND h.admission_date IS NOT NULL) AS is_inpatient,
    h.prior_authorization_number,
    coalesce(l.billed_amount, 0)                          AS billed_amount,
    coalesce(l.allowed_amount, 0)                         AS allowed_amount,
    coalesce(l.paid_amount, 0)                            AS paid_amount,
    coalesce(l.cob_paid_amount, 0)                        AS cob_paid_amount,
    coalesce(l.patient_responsibility_amount, 0)          AS member_responsibility_amount,
    coalesce(l.adjustment_amount, 0)                      AS adjustment_amount,
    coalesce(l.units_of_service, 0)                       AS units_of_service
  FROM ${silver_catalog}.claim.line l
  JOIN ${silver_catalog}.claim.header h ON l.header_id = h.header_id
  LEFT JOIN principal_dx  dx ON l.header_id = dx.header_id
  LEFT JOIN first_payment fp ON l.header_id = fp.header_id
)
SELECT
  c.*,
  trunc(c.incurred_date, 'MM')                            AS incurred_month,
  trunc(c.paid_date, 'MM')                                AS paid_month,
  months_between(trunc(c.paid_date, 'MM'), trunc(c.incurred_date, 'MM')) AS lag_months,
  mm.health_plan_id,
  mm.line_of_business,
  mm.group_id,
  mm.provider_network_id,
  -- as-of surrogate keys: dimension version in effect on the incurred date
  dm.member_sk,
  dpr.provider_sk                                         AS rendering_provider_sk,
  dpb.provider_sk                                         AS billing_provider_sk,
  dpl.plan_sk
FROM lines c
LEFT JOIN fact_member_month mm
  ON  c.member_id = mm.member_id
  AND trunc(c.incurred_date, 'MM') = mm.member_month_start
LEFT JOIN dim_member_scd2 dm
  ON  c.member_id = dm.member_id
  AND CAST(c.incurred_date AS TIMESTAMP) >= dm.effective_from
  AND CAST(c.incurred_date AS TIMESTAMP) <  dm.effective_to
LEFT JOIN dim_provider_scd2 dpr
  ON  c.rendering_provider_id = dpr.provider_id
  AND CAST(c.incurred_date AS TIMESTAMP) >= dpr.effective_from
  AND CAST(c.incurred_date AS TIMESTAMP) <  dpr.effective_to
LEFT JOIN dim_provider_scd2 dpb
  ON  c.billing_provider_id = dpb.provider_id
  AND CAST(c.incurred_date AS TIMESTAMP) >= dpb.effective_from
  AND CAST(c.incurred_date AS TIMESTAMP) <  dpb.effective_to
LEFT JOIN dim_plan_scd2 dpl
  ON  mm.health_plan_id = dpl.health_plan_id
  AND CAST(c.incurred_date AS TIMESTAMP) >= dpl.effective_from
  AND CAST(c.incurred_date AS TIMESTAMP) <  dpl.effective_to;

-- -----------------------------------------------------------------------------
-- fact_prior_auth: accumulating snapshot. Grain: one row per PA request, with the
-- latest decision and latest turnaround-time (TAT) compliance event.
-- -----------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW fact_prior_auth (
  CONSTRAINT pk_not_null EXPECT (pa_request_id IS NOT NULL) ON VIOLATION FAIL UPDATE
)
COMMENT 'Prior authorization accumulating snapshot. Grain: one row per utilization.pa_request.'
AS
WITH latest_decision AS (
  SELECT *
  FROM ${silver_catalog}.utilization.pa_decision
  QUALIFY row_number() OVER (
    PARTITION BY pa_request_id
    ORDER BY CASE WHEN decision_status IN ('active', 'amended') THEN 0 ELSE 1 END,
             decision_date DESC NULLS LAST,
             pa_decision_id DESC
  ) = 1
),
latest_tat AS (
  SELECT *
  FROM ${silver_catalog}.utilization.tat_compliance_event
  QUALIFY row_number() OVER (
    PARTITION BY pa_request_id
    ORDER BY event_timestamp DESC NULLS LAST, tat_compliance_event_id DESC
  ) = 1
),
base AS (
  SELECT
    r.pa_request_id,
    r.request_number,
    r.subscriber_id,
    d.identity_id                                        AS member_id,
    r.provider_id                                        AS requesting_provider_id,
    r.health_plan_id,
    r.group_id,
    r.provider_network_id,
    r.request_type,
    (r.request_type IN ('expedited', 'urgent'))          AS is_expedited,
    r.service_type,
    r.service_code,
    r.diagnosis_code,
    r.submission_channel,
    r.pa_request_status,
    r.request_timestamp,
    CAST(r.request_timestamp AS DATE)                    AS request_date,
    coalesce(d.decision_date, CAST(r.prior_auth_decision_date AS TIMESTAMP)) AS decision_timestamp,
    d.decision_type,
    d.decision_status,
    d.denial_reason_category,
    d.denial_reason_code,
    d.criteria_met_flag,
    d.is_urgent                                          AS decision_is_urgent,
    t.line_of_business                                   AS tat_line_of_business,
    t.review_type                                        AS tat_review_type,
    t.urgency_level                                      AS tat_urgency_level,
    try_cast(t.tat_standard_hours AS DOUBLE)             AS tat_standard_hours,
    t.due_timestamp                                      AS tat_due_timestamp,
    t.compliance_flag                                    AS tat_compliance_flag,
    t.compliance_status                                  AS tat_compliance_status,
    coalesce(r.estimated_gross_amount, 0)                AS estimated_gross_amount,
    coalesce(r.estimated_net_amount, 0)                  AS estimated_net_amount
  FROM ${silver_catalog}.utilization.pa_request r
  LEFT JOIN latest_decision d ON r.pa_request_id = d.pa_request_id
  LEFT JOIN latest_tat      t ON r.pa_request_id = t.pa_request_id
)
SELECT
  b.*,
  -- as-of surrogate keys: dimension version in effect when the request was received
  dm.member_sk,
  dpr.provider_sk                                                         AS requesting_provider_sk,
  dpl.plan_sk,
  (decision_timestamp IS NOT NULL)                                       AS is_decided,
  (decision_type IN ('approved', 'partially_approved'))                  AS is_approved,
  (decision_type = 'denied')                                             AS is_denied,
  CASE WHEN decision_timestamp IS NOT NULL AND request_timestamp IS NOT NULL
       THEN (unix_timestamp(decision_timestamp) - unix_timestamp(request_timestamp)) / 3600.0
  END                                                                    AS turnaround_hours,
  -- Prefer the source TAT compliance flag; fall back to standard-hours comparison.
  coalesce(
    tat_compliance_flag,
    CASE WHEN decision_timestamp IS NOT NULL AND tat_standard_hours IS NOT NULL
         THEN (unix_timestamp(decision_timestamp) - unix_timestamp(request_timestamp)) / 3600.0
              <= tat_standard_hours
    END
  )                                                                      AS is_tat_compliant
FROM base b
LEFT JOIN dim_member_scd2 dm
  ON  b.member_id = dm.member_id
  AND b.request_timestamp >= dm.effective_from
  AND b.request_timestamp <  dm.effective_to
LEFT JOIN dim_provider_scd2 dpr
  ON  b.requesting_provider_id = dpr.provider_id
  AND b.request_timestamp >= dpr.effective_from
  AND b.request_timestamp <  dpr.effective_to
LEFT JOIN dim_plan_scd2 dpl
  ON  b.health_plan_id = dpl.health_plan_id
  AND b.request_timestamp >= dpl.effective_from
  AND b.request_timestamp <  dpl.effective_to;
