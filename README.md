# Investment Data Quality & T+1 Settlement Risk Diagnostic (NDA-Safe Demo)

A practical, vendor-neutral framework demonstrating how investment operations teams can detect data integrity issues before T+1 settlement using Python validation, a canonical investment data model, and operational evidence packs.

This project simulates a real-world operational challenge faced by asset managers and fund administrators: identifying data quality issues early enough to avoid settlement failures, NAV discrepancies, operational incidents, and costly investigations.

All data used in this repository is synthetic and generated for demonstration purposes.

---
# What This Repository Demonstrates

This repository is intended to demonstrate practical capability in several areas that are highly relevant to modern investment operations environments:

• designing data quality control frameworks for investment data  
• structuring canonical data models for downstream controls and analytics  
• identifying operational risk signals related to T+1 settlement readiness  
• generating evidence-pack style outputs for stakeholder review  
• connecting data validation workflows to operational monitoring dashboards  

The objective is not to reproduce a full production platform, but to show how investment operations, data architecture, and control design can be combined into a practical diagnostic framework.

---

# The Real-World Problem

Modern investment operations teams must manage data flowing from many systems:

• legacy investment accounting platforms
• trading systems
• market data feeds (prices / FX)
• reference data systems (security master, fund master)
• external CSV or Excel extracts

Before settlement deadlines (especially under **T+1 settlement cycles**), operations teams must verify that this data is complete, consistent, and accurate.

Common operational risks include:

• trades missing FX rates
• price mismatches between trade price and market price
• incomplete security master data
• settlement date issues
• reconciliation breaks between positions and pricing

When these issues are detected too late, they can cause:

• failed settlements
• incorrect NAV calculations
• operational incidents
• manual investigations and escalations

Many firms still rely on fragmented manual checks across spreadsheets, internal tools, and legacy systems.

This project demonstrates how a **diagnostic control framework** could be implemented to detect such issues earlier and provide structured evidence for operations teams.

---

# Why This Matters Now

Many financial markets have recently transitioned from **T+2 to T+1 settlement cycles**, significantly reducing the time available for investment operations teams to detect and resolve data issues.

Under T+1 timelines, problems in pricing data, FX rates, trade details, or reference data must be identified much earlier in the trade lifecycle.

If these issues remain undetected, firms may face:

• settlement failures  
• incorrect NAV calculations  
• operational escalations  
• regulatory scrutiny  

This shift toward faster settlement cycles is forcing many firms to rethink how they validate and monitor operational data.

The diagnostic framework demonstrated in this repository illustrates how a lightweight validation and control layer could help operations teams detect issues earlier and improve settlement readiness.



---
# What This Demo Shows

This repository simulates a simplified investment data control framework that:

1. Validates raw investment data extracts
2. Identifies data integrity issues
3. Classifies breaks using a rule catalog
4. Produces an operational evidence pack
5. Visualizes operational risk in a dashboard

The goal is to demonstrate how a lightweight diagnostic layer could sit between legacy systems and modern data platforms.

---

# Client Workflow

A typical usage flow for this framework is:

Client sends masked operational extracts  
↓  
Python diagnostic validates source data  
↓  
Control rules identify breaks and risk signals  
↓  
Evidence pack summarizes issues and actions  
↓  
Power BI highlights operational priorities

In practice, this means a client can provide masked trade, market data, and reference data extracts, and quickly receive a structured view of potential settlement-risk or reconciliation issues before they become downstream operational problems.

---

# High-Level Architecture

The system follows a simple but realistic control flow:

```
Legacy Systems / Market Data
        │
        ▼
Python Quality Gate
(validation and reject logic)
        │
        ▼
Bronze Landing / Raw Staging
        │
        ▼
Canonical Investment Data Model
(Snowflake-ready ODS design)
        │
        ▼
Control & Reconciliation Rules
        │
        ▼
Evidence Pack Outputs
        │
        ▼
Operational Monitoring (Power BI)
```

A detailed architecture diagram will be included in:

```
docs/architecture/
```

---

# Example Diagnostic Checks

The demo includes simplified control rules such as:

### Price Variance Check

Detects when trade price deviates significantly from market price.

### Missing FX Check

Identifies trades where FX rates are missing for the trade date.

### Settlement Risk Check

Flags trades that could be at risk under T+1 settlement timelines.

### Reference Data Completeness

Checks whether required security master fields are populated.

These checks simulate the types of operational controls used by investment operations and data management teams.

---

# Evidence Pack Outputs

The diagnostic engine generates structured outputs such as:

```
Summary.csv
AUDIT_EVIDENCE_PACK_TPLUS1_SAMPLE.csv
breaks_with_taxonomy.csv
recon_nav_like.csv
```

These outputs summarize:

• break type
• root cause classification
• severity level
• suggested operational action

This type of evidence pack allows operations teams to quickly identify and triage issues.

---

# Power BI Operational Cockpit

The repository also includes a Power BI dashboard that visualizes:

• break trends
• root cause distribution
• settlement risk indicators
• operational workload

The dashboard simulates how an operations control center might monitor daily data health.

Dashboard assets are located in:

```
powerbi/
```

---

# Repository Structure

```
data/
    Sample data extracts and generated outputs

python/
    Validation logic and diagnostic scripts

snowflake/
    Canonical investment data model (ODS schema)

dbt/
    Reconciliation and transformation logic

tplus1_risk_engine/
    Synthetic T+1 settlement risk simulation

powerbi/
    Operational monitoring dashboard

docs/
    Architecture and documentation
```

---

# Technology Stack

This demonstration uses a simple but realistic stack commonly seen in modern data platforms:

• Python
• SQL / Snowflake data modeling
• dbt transformation logic
• DuckDB for lightweight analytics
• Power BI for operational dashboards

---

# NDA-Safe Demonstration

This project intentionally uses:

• synthetic datasets
• generated extracts
• simplified control rules

No client data, proprietary systems, or confidential logic are included.

The goal is purely educational and demonstrative.

---

# Who This Is For

This demo may be useful for professionals interested in:

• investment operations modernization
• data quality frameworks
• migration from legacy investment systems
• operational control dashboards
• Snowflake-based data architectures

---

# Future Enhancements

Possible extensions include:

• automated rule catalogs
• anomaly detection using machine learning
• AI-assisted break explanations
• automated evidence pack generation
• workflow integration with ticketing systems

---
# Related Operational Challenges

While this demo focuses on settlement readiness and T+1 risk,
similar data validation frameworks are often used for:

• NAV validation and fund accounting controls  
• data migration from legacy investment systems  
• reference data governance  
• reconciliation between accounting and market data platforms  
• operational data quality monitoring  

These types of control layers are commonly implemented when
modernizing investment operations platforms or migrating data
to cloud-based data warehouses.

---

# Author

Debojyoti Sarkar
Investment Operations & Data Architecture

This project is part of an independent exploration of modern data control frameworks for investment operations.
