# Demo Walkthrough

This document explains how the Investment Data Quality & T+1 Settlement Risk Diagnostic framework can be used in a practical scenario.

---

# Business Scenario

An investment operations or fund administration team wants to assess whether operational data issues could create settlement risk, reconciliation breaks, or downstream reporting problems.

The team exports masked sample datasets from source systems, including trade, market data, and reference data extracts.

The diagnostic framework is then used to validate these datasets, identify potential issues, and produce operational evidence for review.

---

# End-to-End Workflow

## Step 1 — Client provides masked extracts

Example input files may include:

- trade extracts
- position or transaction extracts
- FX rates
- market prices
- security master data
- fund master data
- holiday calendar data

These are provided in masked CSV or Excel format for diagnostic review.

---

## Step 2 — Python diagnostic layer validates the data

The Python quality gate runs validation logic on the source extracts.

This includes checks such as:

- schema validation
- data type validation
- business rule checks
- reject vs clean record separation
- run manifest generation

At this stage, obvious data quality issues are identified before downstream processing.

---

## Step 3 — Data is prepared for controlled downstream use

Validated data is staged into a raw landing layer and mapped into a canonical investment data model.

This enables downstream controls to operate on a consistent structure rather than directly on source-specific file layouts.

The canonical model includes entities such as:

- FUND
- ASSET
- TRADE
- FX_RATE
- ASSET_PRICE

---

## Step 4 — Control rules identify operational risk

The framework then applies simplified reconciliation and control checks.

Example checks include:

- settlement latency checks
- missing FX on trade date
- price vs market variance
- reference data completeness
- NAV-style and cash-style control logic
- break taxonomy classification

This step converts raw data issues into operationally meaningful break signals.

---

## Step 5 — Evidence pack outputs are generated

The framework produces structured outputs that can be reviewed by operations stakeholders.

Example outputs include:

- Summary.csv
- AUDIT_EVIDENCE_PACK_TPLUS1_SAMPLE.csv
- breaks_with_taxonomy.csv
- recon_nav_like.csv

These outputs help explain:

- what issue was found
- where it was found
- why it matters
- what action may be required

---

## Step 6 — Power BI highlights operational priorities

The Power BI operational cockpit provides a visual summary of diagnostic results.

Example dashboard views may include:

- break trends
- root cause distribution
- T+1 settlement risk indicators
- workload / KPI style summaries

This allows operations teams to move from raw exception data to prioritized review.

---

# Why This Workflow Matters

In real investment operations environments, data quality issues often surface too late, after they have already created settlement pressure, reconciliation noise, or downstream reporting risk.

This framework demonstrates how a lightweight diagnostic layer can help teams:

- detect issues earlier
- improve control visibility
- generate structured evidence
- support operational triage
- strengthen settlement readiness

---

# Intended Use of This Demo

This repository is designed as an NDA-safe demonstration of how investment operations control frameworks can be structured using Python, SQL, canonical data modeling, and operational reporting.

It is intended for:

- architecture discussions
- consulting demonstrations
- control framework design conversations
- portfolio and capability showcasing