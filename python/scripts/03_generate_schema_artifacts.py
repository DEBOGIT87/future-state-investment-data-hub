from pathlib import Path
import csv, re
from collections import OrderedDict

# repo root from python/scripts/*
ROOT = Path(__file__).resolve().parents[2]
INV  = ROOT / "docs" / "csv_headers_inventory.csv"

OUT_BRONZE = ROOT / "snowflake" / "ddl" / "10_bronze_tables.sql"
OUT_SRCYML = ROOT / "dbt" / "models" / "staging" / "_stg_sources.yml"

def base_dataset_name(filename: str) -> str:
    # fund_master_extract_20260118_201244_c80818.csv -> fund_master
    fn = filename.strip().strip('"')
    fn = fn.replace("-", "_")
    fn = re.sub(r"\.csv$", "", fn, flags=re.IGNORECASE)
    fn = re.sub(r"_extract_.*$", "", fn)  # drop run suffix
    fn = re.sub(r"[^0-9a-zA-Z_]+", "_", fn)
    return fn.lower()

def read_inventory(path: Path):
    if not path.exists():
        raise FileNotFoundError(f"Missing inventory file: {path}")

    groups = OrderedDict()  # dataset -> ordered list of columns
    seen = {}               # dataset -> set for dedupe

    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            file = (row.get("file") or "").strip()
            col  = (row.get("column") or "").strip().strip('"')
            if not file or not col:
                continue

            ds = base_dataset_name(file)
            if ds not in groups:
                groups[ds] = []
                seen[ds] = set()

            if col not in seen[ds]:
                groups[ds].append(col)
                seen[ds].add(col)

    return groups

def norm_col(name: str) -> str:
    return re.sub(r"[^0-9a-zA-Z_]+", "_", name).upper()

def write_bronze_sql(groups, out_path: Path):
    lines = []
    lines.append("-- AUTO-GENERATED from docs/csv_headers_inventory.csv")
    lines.append("-- BRONZE tables: land raw CSV columns as STRING + add minimal load metadata")
    lines.append("")

    for ds, cols in groups.items():
        table = ds.upper()
        lines.append(f"CREATE OR REPLACE TABLE BRONZE.{table} (")
        for c in cols:
            lines.append(f"  {norm_col(c)} STRING,")
        lines.append("  RAW_FILE_NAME STRING,")
        lines.append("  LOADED_AT TIMESTAMP_NTZ")
        lines.append(");")
        lines.append("")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(lines), encoding="utf-8")

def write_dbt_sources(groups, out_path: Path):
    lines = []
    lines.append("version: 2")
    lines.append("")
    lines.append("sources:")
    lines.append("  - name: bronze")
    lines.append("    database: \"{{ target.database }}\"")
    lines.append("    schema: BRONZE")
    lines.append("    tables:")

    for ds, cols in groups.items():
        tbl = ds.upper()
        lines.append(f"      - name: {tbl}")
        lines.append("        columns:")
        for c in cols:
            lines.append(f"          - name: {norm_col(c)}")
        lines.append("          - name: RAW_FILE_NAME")
        lines.append("          - name: LOADED_AT")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(lines), encoding="utf-8")

def main():
    groups = read_inventory(INV)
    if not groups:
        raise RuntimeError("No datasets/columns found in docs/csv_headers_inventory.csv")

    write_bronze_sql(groups, OUT_BRONZE)
    write_dbt_sources(groups, OUT_SRCYML)

    print(f"[OK] Generated: {OUT_BRONZE}")
    print(f"[OK] Generated: {OUT_SRCYML}")
    print("[INFO] Datasets found:", ", ".join(groups.keys()))

if __name__ == "__main__":
    main()
