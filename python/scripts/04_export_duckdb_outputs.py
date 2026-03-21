from pathlib import Path
import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
RUN_ID = "20260118_201244_c80818"

db_path = ROOT / "dbt" / "duckdb" / "demo.duckdb"
con = duckdb.connect(str(db_path))

exports = {
  "recon_nav_like": "main_CONTROL.RECON_NAV_LIKE",
  "recon_cash_like": "main_CONTROL.RECON_CASH_LIKE",
  "recon_reference_coverage": "main_CONTROL.RECON_REFERENCE_COVERAGE",
  "breaks_with_taxonomy": "main_CONTROL.BREAKS_WITH_TAXONOMY",
}

out_dir = ROOT / "data" / "exports"
out_dir.mkdir(parents=True, exist_ok=True)

for name, obj in exports.items():
    df = con.execute(f"select * from {obj}").df()
    out = out_dir / f"{name}_{RUN_ID}.csv"
    df.to_csv(out, index=False)
    print("Wrote:", out, "rows=", len(df))

con.close()
