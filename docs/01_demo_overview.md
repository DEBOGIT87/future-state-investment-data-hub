# Future-State Investment Data Hub (NDA-safe) — Demo Overview

## What this is
This repository is a vendor-neutral demonstration of a controlled migration pattern for investment data:  
**validate → standardize → reconcile → report**.

The focus is not a specific source platform. The focus is the operating controls that protect:
- data completeness and consistency
- traceability of changes and outcomes
- financial integrity checks (NAV / income / cash) as a discipline, not an afterthought

## NDA and IP safety
This repository is **NDA-safe by design**:
- All datasets are **synthetic** and generated locally (no client, employer, or vendor data).
- Labels and structures use **generic, vendor-neutral naming** (no proprietary schemas, tables, or mappings).
- The code demonstrates **reusable engineering patterns** (validation, standardization, run evidence) that apply across implementations.
- Please do not submit or attach confidential datasets, proprietary schemas, or client identifiers in issues or discussions.

## Demo goals
This demo is designed to show three things clearly:

1) **Data is gated before it enters the warehouse**  
   Bad records are identified early, routed to rejects, and summarized with evidence.

2) **A canonical target model is the anchor**  
   Instead of carrying forward “source-shaped” structures, the approach centers around a stable canonical ODS concept that downstream logic can rely on.

3) **Evidence is produced as a standard output**  
   Runs are repeatable and traceable (run-id outputs, clean/reject splits, summary counts) so teams can support governance, triage, and audit needs.

## End-to-end flow (high level)
**Sources (legacy + external feeds)**  
→ **Python Quality Gate** (validation + rejects + run evidence)  
→ **Bronze Landing** (raw/staging zone)  
→ **Snowflake Canonical ODS** (vendor-neutral target model)  
→ **Controls + Reporting**  
- dbt control suite concept (reconciliation checks + tests)  
- Power BI ops command center concept (quality + break trends + T+1 views)  
→ **Stakeholder Evidence Pack** (summary + breaks + actions)

**Governance overlays each stage**: rule catalog, run logs, and audit trail.

## What is included in this repo today
### Phase 1 — Synthetic extracts + Quality Gate (implemented)
The repo includes a working Phase 1 that:
- generates realistic legacy-style extracts using synthetic data
- runs finance-aware validation rules
- produces outputs that separate **clean** vs **rejects** with a **summary report**

This demonstrates the core “shift-left” idea in plain terms:
**catch issues early, measure them, and create evidence before downstream load.**

### Architecture diagram
The architecture diagram included in `docs/diagram/` represents the target operating model for the demo:
- clear left-to-right data flow (data pipeline)
- dotted governance lines (controls / evidence trail)

## Optional: Run the Phase 1 demo locally (for technical review)
This demo can be executed end-to-end on a laptop using synthetic data (no client data required).

### Requirements
- Python 3.x installed
- (Recommended) a virtual environment

### Commands (from repo root)
1) Generate synthetic legacy-style extracts:
```powershell
python .\python\scripts\01_generate_legacy_data.py
2) Run the Quality Gate:
python .\python\scripts\02_quality_gate.py

Outputs (generated locally)
data/raw/ synthetic extracts
data/clean/ validated datasets
data/rejects/ rejected rows + quality_summary_<RUN_ID>.csv



---

## `docs/02_architecture_walkthrough.md`
```md
# Architecture Walkthrough — Future-State Investment Data Hub

## Visual blueprint
Insert the exported diagram here:

`![Architecture Diagram](./diagram/architecture_v1.png)`

(Export recommendation: PNG for README + sharing, SVG for crisp zoom.)

---

## End-to-end flow (what moves where)
**Solid arrows** represent primary data movement.  
**Dotted arrows** represent governance/control evidence flows.

1) Sources produce extracts/feeds  
2) Python Quality Gate validates and separates clean vs rejects  
3) Bronze landing retains raw history for replayability  
4) Canonical ODS standardizes data into a vendor-neutral model  
5) Control suite validates equivalence and flags breaks with structure  
6) Ops Command Center visualizes operational risk and trends  
7) Evidence pack summarizes run health and actions  
8) Governance ties everything together (rule catalog, logs, audit trail)

---


Demo and educational use only

This repository is provided for demonstration and educational purposes. It is not production software and is not a substitute for formal implementation governance, security controls, or platform/vendor documentation.

Do not use this demo with real client or employer data unless appropriate approvals, security controls, and legal clearance are in place.


## Layer-by-layer explanation

### 1) Source layer (Legacy + External Feeds)
**What it is**
- Legacy investment accounting exports
- Legacy positions/transactions feeds
- CSV/Excel extracts used operationally
- Alternatives/private credit style feeds

**Typical issues**
- Inconsistent identifiers across sources
- Missing or invalid dates and terms
- Currency and unit mismatches
- Late arriving corrections (common in close/parallel run windows)

**How this framework handles it**
All incoming sources are treated as **untrusted** until validated.

---

### 2) Python Quality Gate (Validation + Rejects + Run Evidence)
**Role**
A controlled “front door” that prevents bad records from entering downstream layers.

**What it does**
- Applies finance-aware validation rules (format, completeness, consistency, cross-field logic)
- Produces a clean stream and a quarantined reject stream
- Generates run evidence (counts, reason codes, run identifiers)

**Outputs**
- `clean`: records that pass validation
- `rejects`: records isolated for remediation, with reason codes
- `run evidence`: summary outputs to support triage and governance

---

### 3) Bronze landing (Raw/Staging)
**Role**
Preserve what arrived, when it arrived, under which run.

**Why it exists**
- Enables replay without re-extracting from legacy sources
- Supports audit and troubleshooting during parallel run
- Keeps a controlled history of raw inputs separate from standardized outputs

---

### 4) Canonical ODS (Vendor-Neutral Model in Snowflake)
**Role**
A standardized operational data store that becomes the “common language” for analytics and controls.

**Key idea**
Instead of carrying forward vendor-specific table structures, data is normalized into a canonical model designed around investment data concepts (securities, positions, cash, transactions, reference data).

**Why it matters**
- Reduces dependency on source schemas
- Stabilizes downstream reporting and metrics
- Makes reconciliation and testing consistent

---

### 5) Control suite (Reconciliation + Tests in dbt)
**Role**
Repeatable controls that validate equivalence and surface breaks with structure.

**What it does**
- Runs reconciliation checks across key financial integrity surfaces:
  - NAV drivers (positions/valuation)
  - Income components
  - Cash movements
- Produces break outputs that support triage and trend tracking
- Uses a consistent taxonomy to classify breaks (see reconciliation playbook)

---

### 6) Ops Command Center (Power BI)
**Role**
Operational visibility for leadership and execution teams.

**Typical views**
- Quality trends over time (pass rate, top reason codes)
- Break trends (volume and materiality)
- T+1-style monitoring patterns (exceptions requiring same-day attention)
- Progress signals for migration readiness and stabilization

---

### 7) Stakeholder Evidence Pack
**Role**
A consistent summary artifact that supports governance and decision-making.

**Contents**
- Run metadata (date, run-id)
- Quality outcomes (processed, clean, rejected, top reasons)
- Reconciliation status and break categories
- Actions / ownership notes

---

### 8) Governance foundation
Governance is not a separate “extra step.” It is embedded:
- Rule catalog is explicit and reviewable
- Run logs and evidence are generated by default
- Changes to rules/mappings are traceable

This is what makes the process defensible during parallel run and cutover readiness reviews.


