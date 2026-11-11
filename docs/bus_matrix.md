# Bus matrix and modeling decisions

## Bus matrix (v1)

| Business process / fact | Grain | dim_date | dim_member | dim_plan | dim_provider | Degenerate / other |
|---|---|:---:|:---:|:---:|:---:|---|
| Eligibility: `fact_member_month` | member × month | ✅ month | ✅ | ✅ | (PCP via dim_member) | LOB, group, network, coverage type |
| Medical claims: `fact_claim_line` | claim line | ✅ incurred, paid | ✅ | ✅ (via eligibility) | ✅ billing, rendering | claim #, procedure, revenue, POS, principal dx, DRG |
| Prior authorization: `fact_prior_auth` | PA request | ✅ request | ✅ (when decided) | ✅ | ✅ requesting | request #, service code, decision, TAT |
| Cost & utilization: `agg_pmpm_monthly` | month × LOB × plan | ✅ | — | ✅ | — | member months + claim numerators |

## Candidate next facts (not built)

| Process | Silver sources | Notes |
|---|---|---|
| Pharmacy claims | `pharmacy.*` (e.g. `pharmacy_claim`) | Rx PMPM, generic dispensing rate |
| Premium / billing | `billing.*` | MLR numerator/denominator |
| Risk adjustment | `risk.*`, `claim.diagnosis.hcc_category` | HCC/RAF by member-year |
| Appeals & grievances | `appeal.*`, `member.member_grievance` | Timeliness, overturn rate |
| Care management | `care.*` | Program enrollment, gaps in care |
| Claims lag / IBNR | `fact_claim_line` (incurred vs paid month), `claim.ibnr` | Lag triangles |

## Key decisions

1. **Member grain = `member.identity`** (every insured individual, subscriber or dependent). Claims,
   eligibility spans and PA decisions reference `identity_id`; `subscriber_id` is kept for household roll-ups.
2. **Plan and LOB on claims come from eligibility**, not the claim header. `claim.header.lob` in the
   model is a *benefit* line (medical/dental/vision...), not the market LOB (commercial/medicare/medicaid),
   so it is kept as `claim_benefit_lob`.
3. **Overlapping eligibility spans** in the same month resolve to the primary coverage, then the latest span.
4. **Paid date** = earliest non-voided/returned/stopped payment on the claim header (`claim.payment`).
5. **Principal diagnosis** = `diagnosis_type = 'principal'`, else lowest `sequence`.
6. **PCP** is assigned at subscriber level in the model; dependents inherit the subscriber's current PCP.
7. **PA turnaround** = decision timestamp − request timestamp (hours). TAT compliance prefers the
   source `tat_compliance_event.compliance_flag`, falling back to turnaround ≤ `tat_standard_hours`.
8. **Type 1 and Type 2 side by side.** Facts carry natural keys (current-state reporting via Type 1 dims)
   and as-of surrogate keys (historical reporting via `dim_*_scd2`), looked up on the fact's own date.
9. **Reversed claim lines** are kept in `fact_claim_line` but excluded from `agg_pmpm_monthly`.
