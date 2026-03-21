from __future__ import annotations

import argparse
import os
from pathlib import Path
import duckdb


ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = ROOT / "data" / "raw"
DBT_DIR = ROOT / "dbt"
DB_PATH = DBT_DIR / "duckdb" / "demo.duckdb"


CANDIDATES = [
    "main_CONTROL.BREAKS_WITH_TAXONOMY",
    "main_CONTROL.breaks_with_taxonomy",
    "main_CONTROL.BREAKS",
    "main_CONTROL.breaks",
]


def latest_run_id() -> str:
    files = sorted(RAW_DIR.glob("trade_extract_*.csv"))
    if not files:
        raise FileNotFoundError("No trade_extract_*.csv found in data/raw. Cannot derive RUN_ID.")
    latest = files[-1].name
    return latest.replace("trade_extract_", "").replace(".csv", "")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Export breaks from DuckDB control objects.")
    p.add_argument("--run-id", default=None, help="Optional RUN_ID override. Defaults to latest raw extract run.")
    p.add_argument("--db-path", default=str(DB_PATH), help="Path to DuckDB database file.")
    p.add_argument("--dbt-dir", default=str(DBT_DIR), help="dbt working directory used by relative-path views.")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    run_id = args.run_id or latest_run_id()
    db_path = Path(args.db_path)
    dbt_dir = Path(args.dbt_dir)

    if not db_path.exists():
        raise FileNotFoundError(f"DuckDB database not found: {db_path}")
    if not dbt_dir.exists():
        raise FileNotFoundError(f"dbt directory not found: {dbt_dir}")

    previous_cwd = Path.cwd()
    os.chdir(dbt_dir)
    try:
        con = duckdb.connect(str(db_path))
        try:
            objs = con.execute("show tables").fetchall()
            print("DuckDB objects:", objs)

            df = None
            picked = None
            for candidate in CANDIDATES:
                try:
                    df = con.execute(f"select * from {candidate}").df()
                    picked = candidate
                    break
                except Exception:
                    continue

            if df is None:
                raise RuntimeError("Could not find BREAKS table/view. Check 'show tables' output above.")

            out_dir = ROOT / "data" / "exports"
            out_dir.mkdir(parents=True, exist_ok=True)
            out_file = out_dir / f"breaks_export_{run_id}.csv"
            df.to_csv(out_file, index=False)

            print("Exported:", picked)
            print("Wrote:", out_file, "rows=", len(df))
        finally:
            con.close()
    finally:
        os.chdir(previous_cwd)


if __name__ == "__main__":
    main()
