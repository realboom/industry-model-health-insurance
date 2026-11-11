# Databricks notebook source
# MAGIC %md
# MAGIC # Create gold metric views
# MAGIC
# MAGIC Creates (or replaces) Unity Catalog metric views on top of the gold dimensional layer:
# MAGIC
# MAGIC | Metric view | Source | Purpose |
# MAGIC |---|---|---|
# MAGIC | `mv_pmpm` | `agg_pmpm_monthly` + `dim_plan` | Cost & utilization per member per month (PMPM, per-1,000 rates) |
# MAGIC | `mv_claims` | `fact_claim_line` + `dim_member`, `dim_provider`, `dim_plan` | Claim spend, lag and mix |
# MAGIC | `mv_prior_auth` | `fact_prior_auth` + `dim_plan` | PA volume, approval/denial rates, turnaround, TAT compliance |
# MAGIC
# MAGIC Runs after the gold pipeline in the `hi-gold-refresh` job.

# COMMAND ----------

dbutils.widgets.text("gold_catalog", "health_insurance_gold", "Gold catalog")
dbutils.widgets.text("gold_schema", "core", "Gold schema")

# COMMAND ----------

gold_catalog = dbutils.widgets.get("gold_catalog").strip()
gold_schema = dbutils.widgets.get("gold_schema").strip()
g = f"`{gold_catalog}`.`{gold_schema}`"
src = f"{gold_catalog}.{gold_schema}"  # YAML source references (unquoted 3-part names)

# COMMAND ----------

METRIC_VIEWS = {
    "mv_pmpm": f"""
version: 1.1
comment: Cost and utilization per member per month. Ratios are computed from summed numerators and member months.
source: {src}.agg_pmpm_monthly
joins:
  - name: plan
    source: {src}.dim_plan
    on: source.health_plan_id = plan.health_plan_id
dimensions:
  - name: month
    expr: month_start
  - name: year
    expr: year(month_start)
  - name: line_of_business
    expr: line_of_business
  - name: plan_name
    expr: plan.plan_name
  - name: plan_type
    expr: plan.plan_type
  - name: market_segment
    expr: plan.market_segment
measures:
  - name: member_months
    expr: SUM(member_months)
  - name: paid_amount
    expr: SUM(paid_amount)
  - name: allowed_amount
    expr: SUM(allowed_amount)
  - name: paid_pmpm
    expr: SUM(paid_amount) / NULLIF(SUM(member_months), 0)
  - name: allowed_pmpm
    expr: SUM(allowed_amount) / NULLIF(SUM(member_months), 0)
  - name: claims_per_1000
    expr: SUM(claim_count) * 12000.0 / NULLIF(SUM(member_months), 0)
  - name: er_visits_per_1000
    expr: SUM(er_visit_count) * 12000.0 / NULLIF(SUM(member_months), 0)
  - name: inpatient_admits_per_1000
    expr: SUM(inpatient_admit_count) * 12000.0 / NULLIF(SUM(member_months), 0)
""",
    "mv_claims": f"""
version: 1.1
comment: Medical claim line spend, mix and payment lag.
source: {src}.fact_claim_line
joins:
  - name: mbr
    source: {src}.dim_member
    on: source.member_id = mbr.member_id
  - name: prov
    source: {src}.dim_provider
    on: source.rendering_provider_id = prov.provider_id
  - name: plan
    source: {src}.dim_plan
    on: source.health_plan_id = plan.health_plan_id
dimensions:
  - name: incurred_month
    expr: incurred_month
  - name: paid_month
    expr: paid_month
  - name: line_of_business
    expr: coalesce(line_of_business, 'unmatched')
  - name: claim_type
    expr: claim_type
  - name: claim_status
    expr: claim_status
  - name: place_of_service_code
    expr: place_of_service_code
  - name: procedure_code
    expr: procedure_code
  - name: principal_diagnosis_code
    expr: principal_diagnosis_code
  - name: is_emergency
    expr: is_emergency
  - name: is_inpatient
    expr: is_inpatient
  - name: member_age_band
    expr: mbr.age_band
  - name: member_gender
    expr: mbr.gender
  - name: member_state
    expr: mbr.state
  - name: provider_specialty
    expr: prov.primary_specialty_name
  - name: provider_type
    expr: prov.provider_type
  - name: plan_name
    expr: plan.plan_name
measures:
  - name: claim_count
    expr: COUNT(DISTINCT claim_header_id)
  - name: claim_line_count
    expr: COUNT(1)
  - name: member_count
    expr: COUNT(DISTINCT member_id)
  - name: billed_amount
    expr: SUM(billed_amount)
  - name: allowed_amount
    expr: SUM(allowed_amount)
  - name: paid_amount
    expr: SUM(paid_amount)
  - name: member_responsibility_amount
    expr: SUM(member_responsibility_amount)
  - name: allowed_to_billed_ratio
    expr: SUM(allowed_amount) / NULLIF(SUM(billed_amount), 0)
  - name: avg_paid_per_claim
    expr: SUM(paid_amount) / NULLIF(COUNT(DISTINCT claim_header_id), 0)
  - name: avg_lag_months
    expr: AVG(lag_months)
""",
    "mv_prior_auth": f"""
version: 1.1
comment: Prior authorization volume, outcomes, turnaround time and TAT compliance.
source: {src}.fact_prior_auth
joins:
  - name: plan
    source: {src}.dim_plan
    on: source.health_plan_id = plan.health_plan_id
dimensions:
  - name: request_month
    expr: date_trunc('MONTH', request_date)
  - name: request_type
    expr: request_type
  - name: is_expedited
    expr: is_expedited
  - name: service_type
    expr: service_type
  - name: decision_type
    expr: decision_type
  - name: denial_reason_category
    expr: denial_reason_category
  - name: submission_channel
    expr: submission_channel
  - name: line_of_business
    expr: coalesce(plan.line_of_business, tat_line_of_business)
  - name: plan_name
    expr: plan.plan_name
measures:
  - name: pa_requests
    expr: COUNT(1)
  - name: decided_requests
    expr: COUNT_IF(is_decided)
  - name: approval_rate
    expr: COUNT_IF(is_approved) / NULLIF(COUNT_IF(is_decided), 0)
  - name: denial_rate
    expr: COUNT_IF(is_denied) / NULLIF(COUNT_IF(is_decided), 0)
  - name: avg_turnaround_hours
    expr: AVG(turnaround_hours)
  - name: median_turnaround_hours
    expr: PERCENTILE_APPROX(turnaround_hours, 0.5)
  - name: tat_compliance_rate
    expr: COUNT_IF(is_tat_compliant) / NULLIF(COUNT_IF(is_tat_compliant IS NOT NULL), 0)
""",
}

# COMMAND ----------

failures = []
for name, yaml_body in METRIC_VIEWS.items():
    stmt = f"CREATE OR REPLACE VIEW {g}.`{name}`\nWITH METRICS\nLANGUAGE YAML\nAS $$\n{yaml_body.strip()}\n$$"
    try:
        spark.sql(stmt)
        print(f"created {gold_catalog}.{gold_schema}.{name}")
    except Exception as e:  # keep going so one bad view doesn't hide the others
        failures.append((name, str(e)[:500]))
        print(f"FAILED {name}: {str(e)[:500]}")

if failures:
    raise Exception(f"{len(failures)} metric view(s) failed: {[f[0] for f in failures]}")
