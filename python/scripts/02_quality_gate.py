from __future__ import annotations

from pathlib import Path
import re
import numpy as np
import pandas as pd

# ==========================================================
# PHASE 1B - QUALITY GATE (NDA-safe)
# Reads latest RUN_ID extracts from data/raw and writes:
#   data/clean/*_clean_<RUN_ID>.csv
#   data/rejects/*_rejects_<RUN_ID>.csv
# Also writes:
#   data/rejects/quality_summary_<RUN_ID>.csv
# ==========================================================

BASE = Path(__file__).resolve().parents[2]
print(f"[BASE] {BASE}")
RAW_DIR = BASE / "data" / "raw"
CLEAN_DIR = BASE / "data" / "clean"
REJ_DIR = BASE / "data" / "rejects"
REF_DIR = BASE / "data" / "reference"

CLEAN_DIR.mkdir(parents=True, exist_ok=True)
REJ_DIR.mkdir(parents=True, exist_ok=True)

# --------------------------
# Helpers
# --------------------------
def latest_run_id() -> str:
    # pick latest trade file -> extract RUN_ID
    files = sorted(RAW_DIR.glob("trade_extract_*.csv"))
    if not files:
        raise FileNotFoundError("No trade_extract_*.csv found in data/raw. Run 01_generate_legacy_data.py first.")
    latest = files[-1].name
    # trade_extract_<RUN_ID>.csv
    return latest.replace("trade_extract_", "").replace(".csv", "")

def load_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, dtype=str)  # load everything as string for safe validation

def to_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")

def flag(df: pd.DataFrame, mask: pd.Series, rule_id: str, reason: str) -> pd.DataFrame:
    """Return a frame with rejected rows for a specific rule."""
    out = df.loc[mask].copy()
    if out.empty:
        return out
    out["REJECT_RULE_ID"] = rule_id
    out["REJECT_REASON"] = reason
    return out

def split_clean_reject(df: pd.DataFrame, rejects_all: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Remove all rejected rows from df (based on primary keys present in rejects_all)."""
    if rejects_all.empty:
        return df.copy(), rejects_all.copy()

    # If SOURCE_TXN_ID exists -> use it. Else use ASSET_ID. Else use row index.
    key_cols = None
    if "SOURCE_TXN_ID" in df.columns:
        key_cols = ["SOURCE_TXN_ID"]
    elif "ASSET_ID" in df.columns:
        key_cols = ["ASSET_ID"]
    elif "FUND_ID" in df.columns:
        key_cols = ["FUND_ID"]

    if key_cols:
        rej_keys = rejects_all[key_cols].dropna().drop_duplicates()
        merged = df.merge(rej_keys.assign(_rej=1), on=key_cols, how="left")
        clean = merged[merged["_rej"].isna()].drop(columns=["_rej"])
        return clean, rejects_all
    else:
        # fallback: drop by index
        clean = df.drop(index=rejects_all.index, errors="ignore")
        return clean, rejects_all

def write_outputs(dataset: str, run_id: str, clean_df: pd.DataFrame, rej_df: pd.DataFrame) -> None:
    clean_path = CLEAN_DIR / f"{dataset}_clean_{run_id}.csv"
    rej_path = REJ_DIR / f"{dataset}_rejects_{run_id}.csv"
    clean_df.to_csv(clean_path, index=False)
    rej_df.to_csv(rej_path, index=False)
    print(f"[WRITE] {dataset}: clean={len(clean_df):,} rejects={len(rej_df):,}")

# --------------------------
# Rule checks
# --------------------------
def validate_funds(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, list[dict]]:
    rejects = []
    summary = []

    # F01: FUND_ID missing
    r = flag(df, df["FUND_ID"].isna() | (df["FUND_ID"].str.strip() == ""), "F01", "FUND_ID missing")
    rejects.append(r)

    rej_all = pd.concat(rejects, ignore_index=True) if rejects else pd.DataFrame()
    clean, rej_all = split_clean_reject(df, rej_all)

    summary.append({"dataset": "fund_master", "rule_id": "F01", "reject_count": len(r)})
    return clean, rej_all, summary

def validate_securities(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, list[dict]]:
    rejects = []
    summary = []

    # Normalize date columns
    issue = pd.to_datetime(df["ISSUE_DATE"], errors="coerce")
    mat = pd.to_datetime(df["MATURITY_DATE"], errors="coerce")

    # S01: Bad ISIN (simple rule: exactly 14 chars in our synthetic pattern and alnum)
    isin = df["ID_ISIN"].fillna("")
    bad_isin = ~(isin.str.match(r"^[A-Z0-9]{14}$"))
    r = flag(df, bad_isin, "S01", "Bad ISIN format/length")
    rejects.append(r); summary.append({"dataset": "security_master", "rule_id": "S01", "reject_count": len(r)})

    # S02: Maturity before issue OR invalid dates
    bad_dates = issue.isna() | mat.isna() | (mat < issue)
    r = flag(df, bad_dates, "S02", "Invalid dates or maturity before issue")
    rejects.append(r); summary.append({"dataset": "security_master", "rule_id": "S02", "reject_count": len(r)})

    # S03: Bond missing coupon
    is_bond = df["ASSET_TYPE"].eq("BOND")
    coupon = to_numeric(df["COUPON_RATE"])
    r = flag(df, is_bond & coupon.isna(), "S03", "Bond missing coupon_rate")
    rejects.append(r); summary.append({"dataset": "security_master", "rule_id": "S03", "reject_count": len(r)})

    # S04: Bond missing day_count
    day_count = df["DAY_COUNT"].fillna("").str.strip()
    r = flag(df, is_bond & (day_count == ""), "S04", "Bond missing day_count")
    rejects.append(r); summary.append({"dataset": "security_master", "rule_id": "S04", "reject_count": len(r)})

    # S05: Swap missing reset_frequency
    is_swap = df["ASSET_TYPE"].eq("SWAP")
    reset = df["RESET_FREQUENCY"].fillna("").str.strip()
    r = flag(df, is_swap & (reset == ""), "S05", "Swap missing reset_frequency")
    rejects.append(r); summary.append({"dataset": "security_master", "rule_id": "S05", "reject_count": len(r)})

    rej_all = pd.concat(rejects, ignore_index=True) if rejects else pd.DataFrame()
    clean, rej_all = split_clean_reject(df, rej_all)
    return clean, rej_all, summary

def validate_trades(df: pd.DataFrame, funds_clean: pd.DataFrame, secs_clean: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, list[dict]]:
    rejects = []
    summary = []

    txn_dt = pd.to_datetime(df["TXN_DATE"], errors="coerce")
    settle_dt = pd.to_datetime(df["SETTLE_DATE"], errors="coerce")

    # T01: Settle before trade or invalid dates
    bad_dates = txn_dt.isna() | settle_dt.isna() | (settle_dt < txn_dt)
    r = flag(df, bad_dates, "T01", "Invalid trade/settle dates or settle < trade")
    rejects.append(r); summary.append({"dataset": "trade", "rule_id": "T01", "reject_count": len(r)})

    # T02: Quantity missing / non-numeric
    qty = to_numeric(df["QUANTITY"])
    r = flag(df, qty.isna(), "T02", "Quantity missing or non-numeric")
    rejects.append(r); summary.append({"dataset": "trade", "rule_id": "T02", "reject_count": len(r)})

    # T03: Price missing / token / non-numeric
    price = to_numeric(df["PRICE"])
    r = flag(df, price.isna(), "T03", "Price missing or non-numeric token")
    rejects.append(r); summary.append({"dataset": "trade", "rule_id": "T03", "reject_count": len(r)})

    # T04: Referential integrity (fund)
    valid_funds = set(funds_clean["FUND_ID"].dropna().unique())
    r = flag(df, ~df["FUND_ID"].isin(valid_funds), "T04", "Unknown FUND_ID (not in clean fund master)")
    rejects.append(r); summary.append({"dataset": "trade", "rule_id": "T04", "reject_count": len(r)})

    # T05: Referential integrity (asset)
    valid_assets = set(secs_clean["ASSET_ID"].dropna().unique())
    r = flag(df, ~df["ASSET_ID"].isin(valid_assets), "T05", "Unknown ASSET_ID (not in clean security master)")
    rejects.append(r); summary.append({"dataset": "trade", "rule_id": "T05", "reject_count": len(r)})

    rej_all = pd.concat(rejects, ignore_index=True) if rejects else pd.DataFrame()
    clean, rej_all = split_clean_reject(df, rej_all)
    return clean, rej_all, summary

def validate_fx(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, list[dict]]:
    rejects = []
    summary = []

    fx_date = pd.to_datetime(df["FX_DATE"], errors="coerce")
    rate = to_numeric(df["FX_RATE"])

    # X01: invalid date or rate
    r = flag(df, fx_date.isna() | rate.isna(), "X01", "Invalid FX_DATE or FX_RATE")
    rejects.append(r); summary.append({"dataset": "fx_rates", "rule_id": "X01", "reject_count": len(r)})

    rej_all = pd.concat(rejects, ignore_index=True) if rejects else pd.DataFrame()
    clean, rej_all = split_clean_reject(df, rej_all)
    return clean, rej_all, summary

def validate_prices(df: pd.DataFrame, secs_clean: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, list[dict]]:
    rejects = []
    summary = []

    p_date = pd.to_datetime(df["PRICE_DATE"], errors="coerce")
    price = to_numeric(df["PRICE"])

    # P01: invalid date or price
    r = flag(df, p_date.isna() | price.isna(), "P01", "Invalid PRICE_DATE or PRICE")
    rejects.append(r); summary.append({"dataset": "prices", "rule_id": "P01", "reject_count": len(r)})

    # P02: asset not in clean securities
    valid_assets = set(secs_clean["ASSET_ID"].dropna().unique())
    r = flag(df, ~df["ASSET_ID"].isin(valid_assets), "P02", "Unknown ASSET_ID (not in clean security master)")
    rejects.append(r); summary.append({"dataset": "prices", "rule_id": "P02", "reject_count": len(r)})

    rej_all = pd.concat(rejects, ignore_index=True) if rejects else pd.DataFrame()
    clean, rej_all = split_clean_reject(df, rej_all)
    return clean, rej_all, summary

# --------------------------
# Main pipeline
# --------------------------
def main():
    run_id = latest_run_id()
    print(f"[RUN_ID] {run_id}")

    paths = {
        "fund_master": RAW_DIR / f"fund_master_extract_{run_id}.csv",
        "security_master": RAW_DIR / f"security_master_extract_{run_id}.csv",
        "trade": RAW_DIR / f"trade_extract_{run_id}.csv",
        "fx_rates": RAW_DIR / f"fx_rates_extract_{run_id}.csv",
        "prices": RAW_DIR / f"price_timeseries_extract_{run_id}.csv",
    }

    for k, p in paths.items():
        if not p.exists():
            raise FileNotFoundError(f"Missing {k} file: {p}")

    funds = load_csv(paths["fund_master"])
    secs = load_csv(paths["security_master"])
    trades = load_csv(paths["trade"])
    fx = load_csv(paths["fx_rates"])
    prices = load_csv(paths["prices"])

    summary_rows: list[dict] = []

    funds_clean, funds_rej, s = validate_funds(funds); summary_rows += s
    secs_clean, secs_rej, s = validate_securities(secs); summary_rows += s
    trades_clean, trades_rej, s = validate_trades(trades, funds_clean, secs_clean); summary_rows += s
    fx_clean, fx_rej, s = validate_fx(fx); summary_rows += s
    prices_clean, prices_rej, s = validate_prices(prices, secs_clean); summary_rows += s

    write_outputs("fund_master", run_id, funds_clean, funds_rej)
    write_outputs("security_master", run_id, secs_clean, secs_rej)
    write_outputs("trade", run_id, trades_clean, trades_rej)
    write_outputs("fx_rates", run_id, fx_clean, fx_rej)
    write_outputs("prices", run_id, prices_clean, prices_rej)

    summary_df = pd.DataFrame(summary_rows)
    summary_path = REJ_DIR / f"quality_summary_{run_id}.csv"
    summary_df.to_csv(summary_path, index=False)
    print(f"[SUMMARY] {summary_path}")

    total_rejects = (
        len(funds_rej) + len(secs_rej) + len(trades_rej) + len(fx_rej) + len(prices_rej)
    )
    print(f"[DONE] Total rejects across datasets: {total_rejects:,}")

if __name__ == "__main__":
    main()
