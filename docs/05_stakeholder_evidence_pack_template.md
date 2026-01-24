# Stakeholder Evidence Pack (Template)

**Project:** Investment Data Modernization  
**Run Date:** {{YYYY-MM-DD}}  
**Run ID:** {{RUN_ID}}  

---

## 1) Executive summary
- **Run health:** {{GREEN / AMBER / RED}}
- **Total records processed:** {{COUNT}}
- **Quality pass rate:** {{PERCENT}}%
- **Primary risk signal (if any):** {{ONE LINE}}

---

## 2) Quality Gate outcomes
- **Clean records:** {{COUNT}} (eligible for downstream processing)
- **Rejected records:** {{COUNT}} (quarantined)
- **Top rejection reason:** {{REASON_CODE}} — {{SHORT DESC}}

**Top rejection reasons (sample)**
| Reason code | Count | Notes |
|---|---:|---|
| {{CODE_1}} | {{N}} | {{NOTE}} |
| {{CODE_2}} | {{N}} | {{NOTE}} |
| {{CODE_3}} | {{N}} | {{NOTE}} |

---

## 3) Reconciliation status (parallel run)
| Category | Count | Notes / status |
|---|---:|---|
| Match / zero break | {{N}} | {{OK}} |
| Timing differences | {{N}} | {{TRACK}} |
| Rounding differences | {{N}} | {{TOLERANCE}} |
| Mapping / logic defects | {{N}} | {{ACTION}} |
| Data defects | {{N}} | {{ACTION}} |

---

## 4) Actions and ownership
| Action | Owner | Target date | Status |
|---|---|---|---|
| {{ACTION_1}} | {{NAME/ROLE}} | {{DATE}} | {{OPEN}} |
| {{ACTION_2}} | {{NAME/ROLE}} | {{DATE}} | {{OPEN}} |

---

## 5) Notes and decisions
- {{NOTE_1}}
- {{NOTE_2}}

---

## NDA / handling note
Do not include proprietary client datasets, identifiers, or confidential schemas in shared documents.
Use sanitized examples or metadata-only descriptions when sharing outside approved channels.
