from __future__ import annotations

from pathlib import Path
import pandas as pd

# ==========================================================
# PHASE 1B - QUALITY GATE (NDA-safe)
# Reads latest RUN_ID extracts from data/raw and writes:
#   data/clean/*_clean_<RUN_ID>.csv
#   data/rejects/*_rejects_<RUN_ID>.csv
# Also writes:
#   data/rejects/quality_summary_<RUN_ID>.csv
#
# Design notes:
# - Every source row gets an internal _ROW_ID for precise clean/reject splitting.
# - Rules that identify missing expected coverage (for example missing FX or
#   missing price rows) produce synthetic reject rows with no _ROW_ID.
# - Output files drop internal helper columns before writing.
# ==========================================================

BASE = Path(__file__).resolve().parents[2]
print(f"[BASE] {BASE}")
RAW_DIR = BASE / "data" / "raw"
CLEAN_DIR = BASE / "data" / "clean"
REJ_DIR = BASE / "data" / "rejects"

CLEAN_DIR.mkdir(parents=True, exist_ok=True)
REJ_DIR.mkdir(parents=True, exist_ok=True)

INTERNAL_COLS = ["_ROW_ID"]


# --------------------------
# Helpers
# --------------------------
def latest_run_id() -> str:
    files = sorted(RAW_DIR.glob("trade_extract_*.csv"))
    if not files:
        raise FileNotFoundError("No trade_extract_*.csv found in data/raw. Run 01_generate_legacy_data.py first.")
    latest = files[-1].name
    return latest.replace("trade_extract_", "").replace(".csv", "")


def load_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str)
    df["_ROW_ID"] = range(len(df))
    return df


def to_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def clean_text(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).str.strip()


def business_days_between(start_dt: pd.Timestamp, end_dt: pd.Timestamp) -> pd.DatetimeIndex:
    if pd.isna(start_dt) or pd.isna(end_dt):
        return pd.DatetimeIndex([])
    return pd.date_range(start=start_dt.normalize(), end=end_dt.normalize(), freq="B")


def flag(df: pd.DataFrame, mask: pd.Series, rule_id: str, reason: str) -> pd.DataFrame:
    out = df.loc[mask].copy()
    if out.empty:
        return out
    out["REJECT_RULE_ID"] = rule_id
    out["REJECT_REASON"] = reason
    return out


def build_missing_record_rejects(
    expected_df: pd.DataFrame,
    actual_df: pd.DataFrame,
    key_cols: list[str],
    rule_id: str,
    reason: str,
) -> pd.DataFrame:
    """Create reject rows for expected keys that are absent from the actual data."""
    if expected_df.empty:
        return pd.DataFrame()

    actual_keys = actual_df[key_cols].drop_duplicates()
    missing = expected_df.merge(actual_keys.assign(_present=1), on=key_cols, how="left")
    missing = missing[missing["_present"].isna()].drop(columns=["_present"])
    if missing.empty:
        return missing

    missing["REJECT_RULE_ID"] = rule_id
    missing["REJECT_REASON"] = reason
    return missing


def split_clean_reject(df: pd.DataFrame, rejects_all: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Remove rejected source rows from clean output using internal _ROW_ID."""
    if rejects_all.empty:
        return df.copy(), rejects_all.copy()

    row_ids = (
        rejects_all["_ROW_ID"]
        if "_ROW_ID" in rejects_all.columns
        else pd.Series(dtype="float64")
    )
    row_ids = pd.to_numeric(row_ids, errors="coerce").dropna().astype(int).unique()

    if len(row_ids) == 0:
        return df.copy(), rejects_all.copy()

    clean = df.loc[~df["_ROW_ID"].isin(row_ids)].copy()
    return clean, rejects_all


def prepare_for_write(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    drop_cols = [c for c in INTERNAL_COLS if c in out.columns]
    if drop_cols:
        out = out.drop(columns=drop_cols)
    return out


def write_outputs(dataset: str, run_id: str, clean_df: pd.DataFrame, rej_df: pd.DataFrame) -> None:
    clean_path = CLEAN_DIR / f"{dataset}_clean_{run_id}.csv"
    rej_path = REJ_DIR / f"{dataset}_rejects_{run_id}.csv"
    prepare_for_write(clean_df).to_csv(clean_path, index=False)
    prepare_for_write(rej_df).to_csv(rej_path, index=False)
    print(f"[WRITE] {dataset}: clean={len(clean_df):,} rejects={len(rej_df):,}")


# --------------------------
# Rule checks
# --------------------------
def validate_funds(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, list[dict]]:
    rejects: list[pd.DataFrame] = []
    summary: list[dict] = []

    r = flag(df, clean_text(df["FUND_ID"]) == "", "F01", "FUND_ID missing")
    rejects.append(r)
    summary.append({"dataset": "fund_master", "rule_id": "F01", "reject_count": len(r)})

    rej_all = pd.concat(rejects, ignore_index=True) if rejects else pd.DataFrame()
    clean, rej_all = split_clean_reject(df, rej_all)
    return clean, rej_all, summary


def validate_securities(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, list[dict]]:
    rejects: list[pd.DataFrame] = []
    summary: list[dict] = []

    issue = pd.to_datetime(df["ISSUE_DATE"], errors="coerce")
    mat = pd.to_datetime(df["MATURITY_DATE"], errors="coerce")

    isin = clean_text(df["ID_ISIN"])
    bad_isin = ~isin.str.match(r"^[A-Z0-9]{14}$")
    r = flag(df, bad_isin, "S01", "Bad ISIN format/length")
    rejects.append(r)
    summary.append({"dataset": "security_master", "rule_id": "S01", "reject_count": len(r)})

    bad_dates = issue.isna() | mat.isna() | (mat < issue)
    r = flag(df, bad_dates, "S02", "Invalid dates or maturity before issue")
    rejects.append(r)
    summary.append({"dataset": "security_master", "rule_id": "S02", "reject_count": len(r)})

    is_bond = clean_text(df["ASSET_TYPE"]).eq("BOND")
    coupon = to_numeric(df["COUPON_RATE"])
    r = flag(df, is_bond & coupon.isna(), "S03", "Bond missing coupon_rate")
    rejects.append(r)
    summary.append({"dataset": "security_master", "rule_id": "S03", "reject_count": len(r)})

    day_count = clean_text(df["DAY_COUNT"])
    r = flag(df, is_bond & (day_count == ""), "S04", "Bond missing day_count")
    rejects.append(r)
    summary.append({"dataset": "security_master", "rule_id": "S04", "reject_count": len(r)})

    is_swap = clean_text(df["ASSET_TYPE"]).eq("SWAP")
    reset = clean_text(df["RESET_FREQUENCY"])
    r = flag(df, is_swap & (reset == ""), "S05", "Swap missing reset_frequency")
    rejects.append(r)
    summary.append({"dataset": "security_master", "rule_id": "S05", "reject_count": len(r)})

    rej_all = pd.concat(rejects, ignore_index=True) if rejects else pd.DataFrame()
    clean, rej_all = split_clean_reject(df, rej_all)
    return clean, rej_all, summary


def validate_trades(
    df: pd.DataFrame,
    funds_clean: pd.DataFrame,
    secs_clean: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, list[dict]]:
    rejects: list[pd.DataFrame] = []
    summary: list[dict] = []

    txn_dt = pd.to_datetime(df["TXN_DATE"], errors="coerce")
    settle_dt = pd.to_datetime(df["SETTLE_DATE"], errors="coerce")

    bad_dates = txn_dt.isna() | settle_dt.isna() | (settle_dt < txn_dt)
    r = flag(df, bad_dates, "T01", "Invalid trade/settle dates or settle < trade")
    rejects.append(r)
    summary.append({"dataset": "trade", "rule_id": "T01", "reject_count": len(r)})

    qty = to_numeric(df["QUANTITY"])
    r = flag(df, qty.isna(), "T02", "Quantity missing or non-numeric")
    rejects.append(r)
    summary.append({"dataset": "trade", "rule_id": "T02", "reject_count": len(r)})

    price = to_numeric(df["PRICE"])
    r = flag(df, price.isna(), "T03", "Price missing or non-numeric token")
    rejects.append(r)
    summary.append({"dataset": "trade", "rule_id": "T03", "reject_count": len(r)})

    valid_funds = set(clean_text(funds_clean["FUND_ID"]).replace("", pd.NA).dropna().unique())
    r = flag(df, ~clean_text(df["FUND_ID"]).isin(valid_funds), "T04", "Unknown FUND_ID (not in clean fund master)")
    rejects.append(r)
    summary.append({"dataset": "trade", "rule_id": "T04", "reject_count": len(r)})

    valid_assets = set(clean_text(secs_clean["ASSET_ID"]).replace("", pd.NA).dropna().unique())
    r = flag(df, ~clean_text(df["ASSET_ID"]).isin(valid_assets), "T05", "Unknown ASSET_ID (not in clean security master)")
    rejects.append(r)
    summary.append({"dataset": "trade", "rule_id": "T05", "reject_count": len(r)})

    rej_all = pd.concat(rejects, ignore_index=True) if rejects else pd.DataFrame()
    clean, rej_all = split_clean_reject(df, rej_all)
    return clean, rej_all, summary


def validate_fx(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, list[dict]]:
    rejects: list[pd.DataFrame] = []
    summary: list[dict] = []

    fx_dt = pd.to_datetime(df["FX_DATE"], errors="coerce")
    rate = to_numeric(df["FX_RATE"])

    r = flag(df, fx_dt.isna() | rate.isna(), "X01", "Invalid FX_DATE or FX_RATE")
    rejects.append(r)
    summary.append({"dataset": "fx_rates", "rule_id": "X01", "reject_count": len(r)})

    # X02: Missing FX coverage for expected business-day currency pairs.
    valid_fx = df.loc[~(fx_dt.isna() | rate.isna())].copy()
    valid_fx["FX_DATE_NORM"] = pd.to_datetime(valid_fx["FX_DATE"], errors="coerce")
    valid_fx["FROM_CCY"] = clean_text(valid_fx["FROM_CCY"])
    valid_fx["TO_CCY"] = clean_text(valid_fx["TO_CCY"])
    valid_fx = valid_fx[
        valid_fx["FX_DATE_NORM"].notna()
        & (valid_fx["FROM_CCY"] != "")
        & (valid_fx["TO_CCY"] != "")
    ]

    if not valid_fx.empty:
        days = business_days_between(valid_fx["FX_DATE_NORM"].min(), valid_fx["FX_DATE_NORM"].max())
        pairs = valid_fx[["FROM_CCY", "TO_CCY"]].drop_duplicates().reset_index(drop=True)
        expected = pairs.merge(
            pd.DataFrame({"FX_DATE_NORM": days}),
            how="cross",
        )
        expected["FX_DATE"] = expected["FX_DATE_NORM"].dt.strftime("%Y-%m-%d")
        expected = expected[["FX_DATE", "FROM_CCY", "TO_CCY"]]

        actual_keys = valid_fx.copy()
        actual_keys["FX_DATE"] = actual_keys["FX_DATE_NORM"].dt.strftime("%Y-%m-%d")
        actual_keys = actual_keys[["FX_DATE", "FROM_CCY", "TO_CCY"]]

        r = build_missing_record_rejects(
            expected_df=expected,
            actual_df=actual_keys,
            key_cols=["FX_DATE", "FROM_CCY", "TO_CCY"],
            rule_id="X02",
            reason="Missing FX rate for expected business-day currency pair",
        )
    else:
        r = pd.DataFrame()

    rejects.append(r)
    summary.append({"dataset": "fx_rates", "rule_id": "X02", "reject_count": len(r)})

    rej_all = pd.concat(rejects, ignore_index=True) if rejects else pd.DataFrame()
    clean, rej_all = split_clean_reject(df, rej_all)
    return clean, rej_all, summary


def validate_prices(
    df: pd.DataFrame,
    secs_clean: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, list[dict]]:
    rejects: list[pd.DataFrame] = []
    summary: list[dict] = []

    price_dt = pd.to_datetime(df["PRICE_DATE"], errors="coerce")
    price = to_numeric(df["PRICE"])

    r = flag(df, price_dt.isna() | price.isna(), "P01", "Invalid PRICE_DATE or PRICE")
    rejects.append(r)
    summary.append({"dataset": "price_timeseries", "rule_id": "P01", "reject_count": len(r)})

    valid_assets = set(clean_text(secs_clean["ASSET_ID"]).replace("", pd.NA).dropna().unique())
    r = flag(df, ~clean_text(df["ASSET_ID"]).isin(valid_assets), "P02", "Unknown ASSET_ID (not in clean security master)")
    rejects.append(r)
    summary.append({"dataset": "price_timeseries", "rule_id": "P02", "reject_count": len(r)})

    # P03: Missing price coverage for expected business-day asset/date combinations.
    valid_prices = df.loc[~(price_dt.isna() | price.isna())].copy()
    valid_prices["PRICE_DATE_NORM"] = pd.to_datetime(valid_prices["PRICE_DATE"], errors="coerce")
    valid_prices["ASSET_ID"] = clean_text(valid_prices["ASSET_ID"])
    valid_prices = valid_prices[
        valid_prices["PRICE_DATE_NORM"].notna()
        & valid_prices["ASSET_ID"].isin(valid_assets)
    ]

    if not valid_prices.empty:
        days = business_days_between(valid_prices["PRICE_DATE_NORM"].min(), valid_prices["PRICE_DATE_NORM"].max())
        assets = valid_prices[["ASSET_ID"]].drop_duplicates().reset_index(drop=True)
        expected = assets.merge(
            pd.DataFrame({"PRICE_DATE_NORM": days}),
            how="cross",
        )
        expected["PRICE_DATE"] = expected["PRICE_DATE_NORM"].dt.strftime("%Y-%m-%d")
        expected = expected[["PRICE_DATE", "ASSET_ID"]]

        actual_keys = valid_prices.copy()
        actual_keys["PRICE_DATE"] = actual_keys["PRICE_DATE_NORM"].dt.strftime("%Y-%m-%d")
        actual_keys = actual_keys[["PRICE_DATE", "ASSET_ID"]]

        r = build_missing_record_rejects(
            expected_df=expected,
            actual_df=actual_keys,
            key_cols=["PRICE_DATE", "ASSET_ID"],
            rule_id="P03",
            reason="Missing price for expected business-day asset/date combination",
        )
    else:
        r = pd.DataFrame()

    rejects.append(r)
    summary.append({"dataset": "price_timeseries", "rule_id": "P03", "reject_count": len(r)})

    rej_all = pd.concat(rejects, ignore_index=True) if rejects else pd.DataFrame()
    clean, rej_all = split_clean_reject(df, rej_all)
    return clean, rej_all, summary


# --------------------------
# Main
# --------------------------
def main() -> None:
    run_id = latest_run_id()
    print(f"[RUN_ID] {run_id}")

    files = {
        "fund_master": RAW_DIR / f"fund_master_extract_{run_id}.csv",
        "security_master": RAW_DIR / f"security_master_extract_{run_id}.csv",
        "trade": RAW_DIR / f"trade_extract_{run_id}.csv",
        "fx_rates": RAW_DIR / f"fx_rates_extract_{run_id}.csv",
        "price_timeseries": RAW_DIR / f"price_timeseries_extract_{run_id}.csv",
    }

    for name, path in files.items():
        if not path.exists():
            raise FileNotFoundError(f"Missing expected input file for {name}: {path}")

    funds = load_csv(files["fund_master"])
    secs = load_csv(files["security_master"])
    trades = load_csv(files["trade"])
    fx = load_csv(files["fx_rates"])
    prices = load_csv(files["price_timeseries"])

    fund_clean, fund_rej, s_fund = validate_funds(funds)
    sec_clean, sec_rej, s_sec = validate_securities(secs)
    trade_clean, trade_rej, s_trade = validate_trades(trades, fund_clean, sec_clean)
    fx_clean, fx_rej, s_fx = validate_fx(fx)
    price_clean, price_rej, s_price = validate_prices(prices, sec_clean)

    write_outputs("fund_master", run_id, fund_clean, fund_rej)
    write_outputs("security_master", run_id, sec_clean, sec_rej)
    write_outputs("trade", run_id, trade_clean, trade_rej)
    write_outputs("fx_rates", run_id, fx_clean, fx_rej)
    write_outputs("price_timeseries", run_id, price_clean, price_rej)

    summary_df = pd.DataFrame(s_fund + s_sec + s_trade + s_fx + s_price)
    summary_path = REJ_DIR / f"quality_summary_{run_id}.csv"
    summary_df.to_csv(summary_path, index=False)
    print(f"[WRITE] quality_summary -> {summary_path}")

    total_rejects = len(fund_rej) + len(sec_rej) + len(trade_rej) + len(fx_rej) + len(price_rej)
    print(f"[DONE] Total rejects across datasets: {total_rejects:,}")


if __name__ == "__main__":
    main()
