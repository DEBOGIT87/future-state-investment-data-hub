from __future__ import annotations

import argparse
from pathlib import Path
import duckdb


ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = ROOT / "data" / "raw"
DB_PATH = ROOT / "dbt" / "duckdb" / "demo.duckdb"


EXPORTS = {
    "recon_nav_like": "main_CONTROL.RECON_NAV_LIKE",
    "recon_cash_like": "main_CONTROL.RECON_CASH_LIKE",
    "recon_reference_coverage": "main_CONTROL.RECON_REFERENCE_COVERAGE",
    "breaks_with_taxonomy": "main_CONTROL.BREAKS_WITH_TAXONOMY",
}


def latest_run_id() -> str:
    files = sorted(RAW_DIR.glob("trade_extract_*.csv"))
    if not files:
        raise FileNotFoundError("No trade_extract_*.csv found in data/raw. Cannot derive RUN_ID.")
    latest = files[-1].name
    return latest.replace("trade_extract_", "").replace(".csv", "")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Export reporting/control outputs from DuckDB.")
    p.add_argument("--run-id", default=None, help="Optional RUN_ID override. Defaults to latest raw extract run.")
    p.add_argument("--db-path", default=str(DB_PATH), help="Path to DuckDB database file.")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    run_id = args.run_id or latest_run_id()
    db_path = Path(args.db_path)

    if not db_path.exists():
        raise FileNotFoundError(f"DuckDB database not found: {db_path}")

    out_dir = ROOT / "data" / "exports"
    out_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(db_path))
    try:
        for name, obj in EXPORTS.items():
            df = con.execute(f"select * from {obj}").df()
            out = out_dir / f"{name}_{run_id}.csv"
            df.to_csv(out, index=False)
            print("Wrote:", out, "rows=", len(df))
    finally:
        con.close()


if __name__ == "__main__":
    main()
