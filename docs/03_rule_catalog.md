# Rule Catalog — Finance-Aware Validation (Sample)

## Purpose
The Quality Gate applies finance-aware rules to incoming data so that downstream layers (standardization, reconciliation, reporting) operate on trusted inputs.

This catalog is written in business language so it can be reviewed with Finance/Ops and used for change control.
Exact rule implementation and thresholds can vary by client policy, data domain, and asset class.

---

## Rule severity
- **BLOCKER (Reject):** record is quarantined; it cannot safely flow downstream.
- **FLAG (Warn):** record may still load, but is flagged for review (client policy dependent).

---

## Core rule domains

### A) Structural / format controls
**Intent:** ensure data is parseable and consistently typed.
- BLOCKER: required fields missing (IDs, dates, currency, quantity)
- BLOCKER: invalid data types (non-numeric quantity/price, invalid date format)
- BLOCKER: invalid enumerations (unknown asset type, invalid side)

### B) Date logic controls
**Intent:** prevent impossible or inconsistent sequencing.
- BLOCKER: settlement date earlier than trade date (if both provided)
- BLOCKER: effective dates outside allowed bounds for the run window
- FLAG: stale pricing date relative to valuation date (policy threshold based)

### C) Identifier and reference integrity
**Intent:** avoid “orphan” records that cannot be linked.
- BLOCKER: instrument/security id missing or not resolvable
- BLOCKER: portfolio/account id missing
- FLAG: identifier mapping exists but is non-unique (ambiguous mapping)

### D) Financial plausibility controls
**Intent:** catch values that commonly break NAV/income/cash.
- BLOCKER: non-positive price where price is required
- BLOCKER: missing currency where monetary fields exist
- FLAG: unusually large quantity/amount beyond policy thresholds (potential fat-finger)

### E) Cross-field consistency
**Intent:** ensure key relationships hold.
- BLOCKER: cash direction inconsistent with transaction type
- BLOCKER: coupon/accrual terms missing for instruments requiring them (asset class dependent)
- FLAG: missing optional enrichment fields used for reporting (e.g., sector, rating)

---

## Output behavior
Each rejected record should include:
- Rule ID / reason code
- Rule name
- Brief message (what failed)
- Run ID (batch identifier)

This makes remediation and triage operational instead of manual investigation.
