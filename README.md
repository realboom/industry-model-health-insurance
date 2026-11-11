# Health Insurance Gold Layer (Kimball) on the Databricks Industry Data Model

A Databricks Asset Bundle that builds a **Kimball dimensional gold layer** on top of the
[Databricks Industry Data Models](https://github.com/databricks-industry-solutions/lakehouse-industry-data-models)
**health_insurance** model (3NF silver), using **Lakeflow Spark Declarative Pipelines (SDP)**.

```
Bronze (source-shaped) ──► Silver: health_insurance ECM (3NF, one schema per domain)
                                  │        │
                                  │        └─► SDP pipeline  hi-silver-scd2  (Change Data Feed → AUTO CDC SCD2)
                                  │             Silver history: <domain>_<table>_scd2 in ${silver_catalog}.history
                                  │
                                  ▼   SDP pipeline  hi-gold-dimensional  (this repo)
                           Gold: conformed dims + facts + aggregates + DQ gate
                                  │
                                  ▼   job task
                           Metric views (semantic layer) ──► AI/BI dashboards, Genie
```

## What gets built

Two SDP pipelines, both written in SQL:

| Pipeline | Job | Publishes to |
|---|---|---|
| `hi-silver-scd2` | `hi-silver-history` (enable CDF → refresh) | `${silver_catalog}.${silver_history_schema}` |
| `hi-gold-dimensional` | `hi-gold-refresh` (refresh → metric views) | `${gold_catalog}.${gold_schema}` |

### Silver SCD Type 2 history

Companion history tables for the master entities whose attributes change over time. The model's
own tables stay Type 1 (current state), so their PK/FK design is untouched.

| History table | Source | Key |
|---|---|---|
| `member_identity_scd2` | `member.identity` | `identity_id` |
| `member_subscriber_scd2` | `member.subscriber` | `subscriber_id` |
| `provider_provider_scd2` | `provider.provider` | `provider_id` |
| `provider_group_practice_scd2` | `provider.group_practice` | `group_practice_id` |
| `provider_facility_scd2` | `provider.facility` | `facility_id` |
| `plan_health_plan_scd2` | `plan.health_plan` | `health_plan_id` |
| `plan_benefit_package_scd2` | `plan.benefit_package` | `benefit_package_id` |
| `employer_group_scd2` | `employer.group` | `group_id` |
| `network_provider_network_scd2` | `network.provider_network` | `provider_network_id` |

How it works:

- `hi-silver-history` first enables **Change Data Feed** on each source table (idempotent), then runs
  the pipeline. Each table has a SQL `AUTO CDC INTO ... STORED AS SCD TYPE 2` flow reading
  `STREAM <table> WITH (readChangeFeed = true)`, sequenced by `_commit_timestamp`, so
  `__START_AT` / `__END_AT` are timestamps; `__END_AT IS NULL` is the current version.
- **First run seeds** from each table's current contents. History therefore starts at go-live; it is
  not backfilled to business effective dates.
- **No `is_current` flag**: AUTO CDC only maintains `__START_AT` / `__END_AT`; derive it when reading
  (`__END_AT IS NULL AS is_current`).
- **Change metadata columns** (untracked): `source_change_type` (`insert` on a key's first version,
  else `update_postimage`), `source_commit_timestamp` (last time the source row was modified, including
  audit-only touches that don't open a new version) and `source_commit_version`.
- **Audit columns are untracked** (`created_timestamp`, `updated_timestamp`, ...): a change that only
  touches them updates the current version in place instead of opening a new one.
- **`pipelines.reset.allowed = false`** on every history table: a pipeline full refresh does not wipe history.
- Deletes in silver close the current version.
- Already effective-dated tables (`member_eligibility_span`, `enrollment_eligibility_span`,
  `pcp_assignment`, `lob_assignment`, `provider.specialty`, `member.address`) do not need SCD2.

Run `hi-silver-history` after each silver load, then `hi-gold-refresh`.

> **Reinstall warning.** The model installer uses `CREATE OR REPLACE TABLE`. Replacing a source table
> turns CDF off and breaks that table's history stream. Don't reinstall the model over a catalog whose
> history you need. If you must, `DEEP CLONE` the `*_scd2` tables first, then re-run `hi-silver-history`
> (re-enables CDF) and plan the stream recovery for the replaced tables before relying on new history.

> **Governance.** History tables carry the same PHI as their source tables. Unity Catalog column tags
> and masks on the source are **not** copied to the streaming tables; apply them to the history schema.

To add a table: copy a block in `src/silver_history/<domain>_scd2.sql` (set `KEYS` and
`TRACK HISTORY ON * EXCEPT`) and add `<domain>.<table>` to the `scd2_source_tables` variable.

### Gold dimensional layer

All gold tables are SDP **materialized views** in one pipeline, published to `${gold_catalog}.${gold_schema}`.

| Object | Type | Grain | Silver sources |
|---|---|---|---|
| `dim_date` | dimension | calendar day | generated |
| `dim_member` | dimension (Type 1) | member (`member.identity`) | `member.identity`, `member.subscriber`, `member.lob_assignment`, `member.pcp_assignment` |
| `dim_provider` | dimension (Type 1) | provider | `provider.provider`, `provider.specialty` |
| `dim_plan` | dimension (Type 1) | health plan product | `plan.health_plan` |
| `fact_member_month` | periodic snapshot | member × month | `member.member_eligibility_span` |
| `fact_claim_line` | transaction | claim line | `claim.line`, `claim.header`, `claim.diagnosis`, `claim.payment` |
| `fact_prior_auth` | accumulating snapshot | PA request | `utilization.pa_request`, `utilization.pa_decision`, `utilization.tat_compliance_event` |
| `agg_pmpm_monthly` | aggregate | month × LOB × plan | `fact_member_month`, `fact_claim_line` |
| `dq_gold_checks` | DQ gate | one row | all of the above |

Metric views (created by the job after the pipeline):

| Metric view | Answers |
|---|---|
| `mv_pmpm` | Paid/allowed PMPM, claims / ER visits / inpatient admits per 1,000 by month, LOB, plan |
| `mv_claims` | Spend, mix and payment lag by claim type, POS, procedure, diagnosis, member demographics, provider specialty |
| `mv_prior_auth` | PA volume, approval/denial rate, turnaround hours, TAT compliance by request type, service, LOB |

See [docs/bus_matrix.md](docs/bus_matrix.md) for the bus matrix and design decisions.

## Prerequisites

1. The **health_insurance** industry data model installed in Unity Catalog with cataloging style
   **One Catalog** (schemas `member`, `claim`, `plan`, `provider`, `utilization`, ...). Use the
   repo's `model-installer/data-model-installer.ipynb` (`model = health_insurance`, `size = ecm`).
   Set `generate_samples = Yes` to get synthetic data to test against.
2. An existing **gold catalog** (the bundle creates the schema, not the catalog).
3. Databricks CLI ≥ 0.250 authenticated to the workspace, serverless enabled.
4. The deploying identity needs `USE CATALOG` + `SELECT` + `CREATE SCHEMA` on the silver catalog,
   `MODIFY` (to enable CDF via `ALTER TABLE`) on the history source tables, and
   `USE CATALOG` + `CREATE SCHEMA` on the gold catalog.

## Deploy and run

```bash
databricks bundle validate -t dev \
  --var="silver_catalog=<your_silver_catalog>" --var="gold_catalog=<your_gold_catalog>"

databricks bundle deploy -t dev \
  --var="silver_catalog=<your_silver_catalog>" --var="gold_catalog=<your_gold_catalog>"

databricks bundle run hi_silver_history -t dev \
  --var="silver_catalog=<your_silver_catalog>" --var="gold_catalog=<your_gold_catalog>"

databricks bundle run hi_gold_refresh -t dev \
  --var="silver_catalog=<your_silver_catalog>" --var="gold_catalog=<your_gold_catalog>"
```

Or set the values once in `databricks.yml` under the target:

```yaml
targets:
  dev:
    variables:
      silver_catalog: <your_silver_catalog>
      gold_catalog: <your_gold_catalog>
```

> **Development mode** (`-t dev`) prefixes the job/pipeline names with `[dev <user>]` and the
> schema name with `dev_<user>_`, so each developer gets an isolated gold schema. Use `-t prod`
> for the shared schema name.

### Variables

| Variable | Default | Meaning |
|---|---|---|
| `silver_catalog` | `health_insurance` | Catalog holding the installed industry model |
| `gold_catalog` | `health_insurance_gold` | Existing catalog for gold |
| `gold_schema` | `core` | Gold schema (created by the bundle) |
| `silver_history_schema` | `history` | Silver schema for SCD2 history tables (created by the bundle) |
| `scd2_source_tables` | 9 master tables | Tables `hi-silver-history` enables CDF on; keep in sync with `src/silver_history/` |
| `date_dim_start` / `date_dim_end` | `2015-01-01` / `2035-12-31` | `dim_date` range; also clamps member-month explosion |

## Data quality

`dq_gold_checks` is a one-row table whose **SDP expectations** are the quality gate:

- **Hard (`ON VIOLATION FAIL UPDATE`)**: dimension PK uniqueness; fact grain uniqueness;
  `fact_claim_line` row count = silver lines with a header; `agg_pmpm_monthly` paid total = claim paid total.
- **Soft (warn)**: claim→member and claim→provider orphans ≤ 1%; member-month→plan orphans ≤ 1%;
  claim lines with no eligibility in the incurred month ≤ 5%.

Results show in the pipeline's data-quality tab and the event log. With synthetic sample data the
soft checks will typically warn (random keys rarely line up across domains); that is expected.

## Design notes

- **Silver stays 3NF; Kimball lives here.** The industry model is a normalized, enterprise-wide
  silver model; gold reshapes it for consumption.
- **Dimensions are Type 1 and keyed on silver natural keys.** Coverage history (plan, LOB, group over
  time) is carried by `fact_member_month`, built from effective-dated eligibility spans, and claims
  pick up plan/LOB from the member's eligibility in the incurred month.
- **SCD Type 2 lives in silver** (`hi-silver-scd2`). Gold MVs are recomputed from silver and cannot
  reconstruct history that silver never kept, so history is captured once in silver and gold SCD2
  dimensions (next step: `dim_member_scd2` / `dim_provider_scd2` with surrogate keys and
  `valid_from`/`valid_to`, facts looking up the version valid at `incurred_date`) are derived from it.
- **No direct identifiers in gold.** Names, SSN, full DOB, street address, phone and email are not
  carried forward; `dim_member` exposes `birth_year`, `age_band`, `state`, `zip3`.
- **Generated model caveat.** The industry model is AI-generated and provided as-is. Validate
  business definitions (e.g. which claim statuses count as paid, how premium is expressed) with
  domain owners before relying on the KPIs.

## Repo layout

```
databricks.yml                     bundle + variables + targets
resources/silver_history.yml       history schema + SDP pipeline + job (enable CDF -> refresh)
resources/gold_schema.yml          gold schema
resources/gold_pipeline.yml        SDP pipeline (serverless)
resources/gold_job.yml             job: pipeline refresh -> metric views
src/pipeline/dimensions.sql        dim_date, dim_member, dim_provider, dim_plan
src/pipeline/facts.sql             fact_member_month, fact_claim_line, fact_prior_auth
src/pipeline/aggregates.sql        agg_pmpm_monthly
src/pipeline/data_quality.sql      dq_gold_checks (expectations gate)
src/silver_history/<domain>_scd2.sql  SCD2 AUTO CDC flows (member, provider, plan, employer, network)
src/notebooks/enable_cdf.py        enables Change Data Feed on the history source tables
src/notebooks/create_metric_views.py  mv_pmpm, mv_claims, mv_prior_auth
docs/bus_matrix.md                 bus matrix and modeling decisions
```
