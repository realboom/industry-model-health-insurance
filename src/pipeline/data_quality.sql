-- =============================================================================
-- Gold data-quality gate, expressed as SDP expectations on a one-row check table.
--   ON VIOLATION FAIL UPDATE  -> hard check: the pipeline update fails.
--   (no action)               -> soft check: recorded as a warning in the event
--                                log / pipeline data-quality UI; row is kept.
-- Query the history of any run with the event log (expectations are under
-- details:flow_progress.data_quality).
-- =============================================================================

CREATE OR REFRESH MATERIALIZED VIEW dq_gold_checks (
  -- grain / primary-key uniqueness (hard)
  CONSTRAINT dim_member_pk_unique        EXPECT (dim_member_dupes = 0)        ON VIOLATION FAIL UPDATE,
  CONSTRAINT dim_provider_pk_unique      EXPECT (dim_provider_dupes = 0)      ON VIOLATION FAIL UPDATE,
  CONSTRAINT dim_plan_pk_unique          EXPECT (dim_plan_dupes = 0)          ON VIOLATION FAIL UPDATE,
  CONSTRAINT fact_member_month_grain     EXPECT (member_month_dupes = 0)      ON VIOLATION FAIL UPDATE,
  CONSTRAINT fact_claim_line_grain       EXPECT (claim_line_dupes = 0)        ON VIOLATION FAIL UPDATE,
  CONSTRAINT fact_prior_auth_grain       EXPECT (prior_auth_dupes = 0)        ON VIOLATION FAIL UPDATE,
  -- SCD2 dimension integrity (hard)
  CONSTRAINT scd2_sk_unique              EXPECT (scd2_sk_dupes = 0)           ON VIOLATION FAIL UPDATE,
  CONSTRAINT scd2_no_overlapping_versions EXPECT (scd2_overlaps = 0)          ON VIOLATION FAIL UPDATE,
  CONSTRAINT scd2_one_current_per_key    EXPECT (scd2_multi_current = 0)      ON VIOLATION FAIL UPDATE,
  -- reconciliation (hard)
  CONSTRAINT claim_lines_reconcile       EXPECT (claim_lines_gold = claim_lines_silver) ON VIOLATION FAIL UPDATE,
  CONSTRAINT pmpm_paid_reconciles        EXPECT (abs(pmpm_paid_total - claim_paid_total) < 0.01) ON VIOLATION FAIL UPDATE,
  -- referential coverage (soft: warn only)
  CONSTRAINT claim_member_orphans_le_1pct   EXPECT (claim_member_orphan_rate   <= 0.01),
  CONSTRAINT claim_provider_orphans_le_1pct EXPECT (claim_provider_orphan_rate <= 0.01),
  CONSTRAINT member_month_plan_orphans_le_1pct EXPECT (member_month_plan_orphan_rate <= 0.01),
  CONSTRAINT claims_without_eligibility_le_5pct EXPECT (claim_no_eligibility_rate <= 0.05),
  CONSTRAINT claims_without_member_sk_le_1pct   EXPECT (claim_no_member_sk_rate <= 0.01)
)
COMMENT 'One-row data-quality gate for the gold layer. See expectations in the pipeline UI / event log.'
AS
WITH
dm AS (SELECT count(*) - count(DISTINCT member_id)       AS dim_member_dupes   FROM dim_member),
dp AS (SELECT count(*) - count(DISTINCT provider_id)     AS dim_provider_dupes FROM dim_provider),
dl AS (SELECT count(*) - count(DISTINCT health_plan_id)  AS dim_plan_dupes     FROM dim_plan),
mm AS (
  SELECT count(*) - count(DISTINCT f.member_id, f.member_month_start) AS member_month_dupes,
         count(*)                                                    AS member_month_rows,
         count_if(f.health_plan_id IS NOT NULL AND p.health_plan_id IS NULL) AS member_month_plan_orphans
  FROM fact_member_month f
  LEFT JOIN dim_plan p ON f.health_plan_id = p.health_plan_id
),
cl AS (
  SELECT count(*) - count(DISTINCT f.claim_line_id)               AS claim_line_dupes,
         count(*)                                                 AS claim_lines_gold,
         count_if(m.member_id IS NULL)                            AS claim_member_orphans,
         count_if(pr.provider_id IS NULL)                         AS claim_provider_orphans,
         count_if(f.health_plan_id IS NULL)                       AS claim_no_eligibility,
         count_if(f.member_sk IS NULL)                            AS claim_no_member_sk,
         coalesce(sum(CASE WHEN f.incurred_month IS NOT NULL AND coalesce(f.line_status, '') <> 'reversed'
                           THEN f.paid_amount END), 0)            AS claim_paid_total
  FROM fact_claim_line f
  LEFT JOIN dim_member   m  ON f.member_id = m.member_id
  LEFT JOIN dim_provider pr ON f.rendering_provider_id = pr.provider_id
),
sl AS (
  SELECT count(*) AS claim_lines_silver
  FROM ${silver_catalog}.claim.line l
  JOIN ${silver_catalog}.claim.header h ON l.header_id = h.header_id
),
scd2_versions AS (
  SELECT 'member'   AS dim, member_id      AS nk, member_sk   AS sk, effective_from, effective_to, is_current FROM dim_member_scd2
  UNION ALL
  SELECT 'provider' AS dim, provider_id    AS nk, provider_sk AS sk, effective_from, effective_to, is_current FROM dim_provider_scd2
  UNION ALL
  SELECT 'plan'     AS dim, health_plan_id AS nk, plan_sk     AS sk, effective_from, effective_to, is_current FROM dim_plan_scd2
),
scd2_ordered AS (
  SELECT *, lead(effective_from) OVER (PARTITION BY dim, nk ORDER BY effective_from) AS next_from
  FROM scd2_versions
),
scd2_current AS (
  SELECT dim, nk, count_if(is_current) AS n_current FROM scd2_versions GROUP BY dim, nk
),
s2 AS (
  SELECT
    (SELECT count(*) - count(DISTINCT dim, sk) FROM scd2_versions)                    AS scd2_sk_dupes,
    (SELECT count_if(next_from IS NOT NULL AND next_from < effective_to) FROM scd2_ordered) AS scd2_overlaps,
    (SELECT count_if(n_current > 1) FROM scd2_current)                                AS scd2_multi_current
),
pa AS (SELECT count(*) - count(DISTINCT pa_request_id) AS prior_auth_dupes FROM fact_prior_auth),
pm AS (SELECT coalesce(sum(paid_amount), 0) AS pmpm_paid_total FROM agg_pmpm_monthly)
SELECT
  current_timestamp()                                              AS checked_at,
  dm.dim_member_dupes, dp.dim_provider_dupes, dl.dim_plan_dupes,
  mm.member_month_dupes, cl.claim_line_dupes, pa.prior_auth_dupes,
  s2.scd2_sk_dupes, s2.scd2_overlaps, s2.scd2_multi_current,
  cl.claim_lines_gold, sl.claim_lines_silver,
  CAST(pm.pmpm_paid_total AS DOUBLE)                               AS pmpm_paid_total,
  CAST(cl.claim_paid_total AS DOUBLE)                              AS claim_paid_total,
  coalesce(try_divide(cl.claim_member_orphans,   cl.claim_lines_gold), 0)   AS claim_member_orphan_rate,
  coalesce(try_divide(cl.claim_provider_orphans, cl.claim_lines_gold), 0)   AS claim_provider_orphan_rate,
  coalesce(try_divide(mm.member_month_plan_orphans, mm.member_month_rows), 0) AS member_month_plan_orphan_rate,
  coalesce(try_divide(cl.claim_no_eligibility,   cl.claim_lines_gold), 0)   AS claim_no_eligibility_rate,
  coalesce(try_divide(cl.claim_no_member_sk,     cl.claim_lines_gold), 0)   AS claim_no_member_sk_rate
FROM s2 CROSS JOIN dm CROSS JOIN dp CROSS JOIN dl CROSS JOIN mm CROSS JOIN cl CROSS JOIN sl CROSS JOIN pa CROSS JOIN pm;
