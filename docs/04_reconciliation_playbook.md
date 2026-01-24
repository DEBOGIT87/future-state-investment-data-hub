# Reconciliation Playbook — Parallel Run Control Pattern

## Objective
During parallel run, the goal is not only to detect differences but to **explain them** and drive convergence with predictable routines.
This playbook defines a structured approach to reconciling legacy outputs vs target outputs across NAV drivers, income, and cash.

---

## What “good” looks like
A controlled reconciliation process produces:
- Repeatable checks on a defined cadence (daily/close windows)
- Break outputs that are categorized (not just “different”)
- Ownership and actions that reduce breaks over time
- Evidence artifacts that support cutover readiness decisions

---

## Reconciliation surfaces (typical)
- **Positions / valuation drivers:** quantities, price date, FX, market value
- **Income drivers:** accrual basis, rates/terms, payment schedules, income recognition
- **Cash drivers:** settlements, corporate actions cash effects, fees, expenses

---

## Variance taxonomy (how breaks are triaged)
Instead of a single “break list,” breaks are categorized to prioritize fixes:

### 1) Timing differences
**Meaning:** the same economic event is recorded on different dates across pipelines (often due to ingestion cadence or late arrivals).  
**Handling:** track and confirm if the break naturally resolves when both sides receive the same event.

### 2) Rounding differences
**Meaning:** differences driven by decimal precision, rounding policy, or calculation granularity.  
**Handling:** define a tolerance policy and suppress non-material differences.

### 3) Mapping / logic defects
**Meaning:** the target calculation differs systematically due to mapping assumptions, business logic, or model design.  
**Handling:** fix logic once, retest across historical windows, and confirm break reduction.

### 4) Data defects (source quality)
**Meaning:** input data is incomplete or incorrect (missing terms, bad identifiers, invalid dates).  
**Handling:** remediate upstream or enforce stricter Quality Gate rules; track recurring root causes.

---

## Operational routine (typical cadence)
A controlled routine is simple and repeatable:

1) Run reconciliation checks and tests on schedule  
2) Review the break summary and top drivers  
3) Assign ownership (data vs logic vs timing)  
4) Apply fixes with change control  
5) Re-run and confirm reduction in break trend

---

## Evidence produced
A mature reconciliation loop leaves behind evidence:
- break counts and materiality by category
- top drivers and recurring root causes
- run logs and test outcomes
- cutover readiness indicators (convergence trend over time)

This turns parallel run from a firefight into an engineered process.
