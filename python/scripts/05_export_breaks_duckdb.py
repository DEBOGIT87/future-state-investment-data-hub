from pathlib import Path
import os
import duckdb
import pandas as pd

RUN_ID = "20260118_201244_c80818"

ROOT = Path(__file__).resolve().parents[2]          # ...\Master_Project
DBT_DIR = ROOT / "dbt"
DB_PATH = DBT_DIR / "duckdb" / "demo.duckdb"

# IMPORTANT: dbt-created views use ../data/raw paths, so run from DBT_DIR
os.chdir(DBT_DIR)

con = duckdb.connect(str(DB_PATH))

# List tables/views so we know the exact name
objs = con.execute("show tables").fetchall()
print("DuckDB objects:", objs)

# Try likely names (case can differ)
candidates = [
    "main_CONTROL.BREAKS_WITH_TAXONOMY",
    "main_CONTROL.breaks_with_taxonomy",
    "main_CONTROL.BREAKS",
    "main_CONTROL.breaks",
]

df = None
picked = None
for c in candidates:
    try:
        df = con.execute(f"select * from {c}").df()
        picked = c
        break
    except Exception as e:
        pass

if df is None:
    raise RuntimeError("Could not find BREAKS table/view. Check 'show tables' output above.")

out_dir = ROOT / "data" / "exports"
out_dir.mkdir(parents=True, exist_ok=True)
out_file = out_dir / f"breaks_export_{RUN_ID}.csv"
df.to_csv(out_file, index=False)

print("Exported:", picked)
print("Wrote:", out_file, "rows=", len(df))
