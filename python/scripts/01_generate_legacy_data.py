from __future__ import annotations

import uuid
from pathlib import Path
from datetime import datetime, timedelta, date
import random

import numpy as np
import pandas as pd
from faker import Faker

# ==========================================================
# PHASE 1A - REALISTIC LEGACY EXTRACT GENERATOR (NDA-safe)
# Output: data/raw/
#   - fund_master_extract_<RUN_ID>.csv
#   - security_master_extract_<RUN_ID>.csv
#   - trade_extract_<RUN_ID>.csv
#   - price_timeseries_extract_<RUN_ID>.csv
#   - fx_rates_extract_<RUN_ID>.csv
# Reference: data/reference/
#   - poison_pills_catalog.csv
# ==========================================================

fake = Faker()
np.random.seed(42)
random.seed(42)

BASE = Path(__file__).resolve().parents[2]
print(f"[BASE] {BASE}")
RAW_DIR = BASE / "data" / "raw"
REF_DIR = BASE / "data" / "reference"
RAW_DIR.mkdir(parents=True, exist_ok=True)
REF_DIR.mkdir(parents=True, exist_ok=True)

RUN_ID = datetime.now().strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:6]

CFG = {
    "n_funds": 25,
    "n_securities": 3000,
    "n_trades": 50000,
    "price_days": 120,   # business days
    "fx_days": 120,      # business days
    "start_date": date(2024, 1, 1),
    "end_date": date(2024, 12, 31),
    "asset_mix": {
        "EQUITY": 0.60,
        "BOND": 0.30,
        "SWAP": 0.07,
        "OPTION": 0.03
    },
    "poison_rates": {
        "bad_isin": 0.005,
        "maturity_before_issue": 0.01,
        "bond_missing_coupon": 0.02,
        "bond_missing_daycount": 0.01,
        "swap_missing_reset": 0.04,
        "trade_settle_before_trade": 0.005,
        "trade_qty_missing": 0.003,
        "trade_price_token": 0.002,
        "fx_missing_rate": 0.01,
        "price_missing": 0.01
    }
}

CURRENCIES = ["USD", "EUR", "GBP", "JPY", "INR", "CHF", "AUD", "CAD"]
BASE_CCY = ["USD", "EUR", "GBP"]

DAY_COUNTS = ["30/360", "ACT/360", "ACT/365"]
PAY_FREQS = ["SEMI", "QUARTERLY", "ANNUAL"]
RESET_FREQS = ["1M", "3M", "6M"]

EQUITY_TICKERS = ["AAPL", "MSFT", "AMZN", "GOOG", "TSLA", "IBM", "ORCL", "NVDA", "VOD", "HSBC"]
RATES_INDEX = ["SOFR", "EURIBOR", "SONIA", "MIBOR"]


def business_days(end: date, n: int) -> list[date]:
    days: list[date] = []
    d = end
    while len(days) < n:
        if d.weekday() < 5:
            days.append(d)
        d = d - timedelta(days=1)
    days.reverse()
    return days


def write_poison_pill_catalog() -> None:
    pill_path = REF_DIR / "poison_pills_catalog.csv"
    if pill_path.exists():
        return
    pd.DataFrame([
        {"pill_id": "P01", "domain": "SECURITY", "rule": "Bond missing coupon_rate",
         "why": "Accrual/income breaks -> NAV mismatch"},
        {"pill_id": "P02", "domain": "SECURITY", "rule": "Swap missing reset_frequency",
         "why": "Cashflow schedule cannot be derived"},
        {"pill_id": "P03", "domain": "SECURITY", "rule": "Maturity date before issue date",
         "why": "Invalid terms -> valuation/bookings break"},
        {"pill_id": "P04", "domain": "SECURITY", "rule": "Bad ISIN format/length",
         "why": "Identifier matching fails across systems"},
        {"pill_id": "P05", "domain": "TRADE", "rule": "Settle date before trade date",
         "why": "Invalid settlement lifecycle"},
        {"pill_id": "P06", "domain": "TRADE", "rule": "Quantity missing",
         "why": "Position roll-forward breaks"},
        {"pill_id": "P07", "domain": "TRADE", "rule": "Price non-numeric token",
         "why": "Valuation aggregation fails"},
        {"pill_id": "P08", "domain": "SECURITY", "rule": "Bond missing day_count",
         "why": "Accrual math differs -> dual-run breaks"},
        {"pill_id": "P09", "domain": "FX", "rule": "FX rate missing for business day",
         "why": "Base currency valuation fails"},
        {"pill_id": "P10", "domain": "PRICE", "rule": "Price missing for a day",
         "why": "PnL/NAV time series breaks"},
    ]).to_csv(pill_path, index=False)


def pick_asset_type() -> str:
    items = list(CFG["asset_mix"].items())
    types = [k for k, _ in items]
    probs = [v for _, v in items]
    return np.random.choice(types, p=probs)


def gen_funds(n: int) -> pd.DataFrame:
    rows = []
    for i in range(n):
        fund_id = f"FUND{i+1:03d}"
        base_ccy = random.choice(BASE_CCY)
        inception = fake.date_between(start_date="-15y", end_date="-2y")
        rows.append({
            "RUN_ID": RUN_ID,
            "SOURCE_SYSTEM": "LEGACY_PLATFORM_A",
            "FUND_ID": fund_id,
            "FUND_NAME": f"{fake.company()} {fund_id}",
            "BASE_CURRENCY": base_ccy,
            "INCEPTION_DATE": str(inception),
            "ACCOUNTING_BASIS": random.choice(["STAT", "GAAP", "IFRS"])
        })
    return pd.DataFrame(rows)


def gen_securities(n: int) -> pd.DataFrame:
    rows = []
    for i in range(n):
        asset_type = pick_asset_type()
        asset_id = f"ASSET{i+100000:06d}"

        issue_date = fake.date_between(start_date="-12y", end_date="-1y")
        maturity_date = fake.date_between(start_date="+1y", end_date="+15y")
        ccy = random.choice(CURRENCIES)

        isin = fake.bothify(text="??##########??").upper()

        row = {
            "RUN_ID": RUN_ID,
            "SOURCE_SYSTEM": "LEGACY_PLATFORM_A",
            "ASSET_ID": asset_id,
            "ASSET_TYPE": asset_type,
            "CURRENCY": ccy,
            "ID_ISIN": isin,
            "ISSUE_DATE": str(issue_date),
            "MATURITY_DATE": str(maturity_date),
            "TICKER": None,
            "COUPON_RATE": None,
            "PAY_FREQ": None,
            "DAY_COUNT": None,
            "RESET_FREQUENCY": None,
            "INDEX_NAME": None,
            "UNDERLYING": None,
            "STRIKE": None
        }

        if asset_type == "EQUITY":
            row["TICKER"] = random.choice(EQUITY_TICKERS)

        elif asset_type == "BOND":
            row["COUPON_RATE"] = round(float(np.random.uniform(0.01, 0.09)), 6)
            row["PAY_FREQ"] = random.choice(PAY_FREQS)
            row["DAY_COUNT"] = random.choice(DAY_COUNTS)

        elif asset_type == "SWAP":
            row["RESET_FREQUENCY"] = random.choice(RESET_FREQS)
            row["INDEX_NAME"] = random.choice(RATES_INDEX)
            row["UNDERLYING"] = row["INDEX_NAME"]

        elif asset_type == "OPTION":
            row["UNDERLYING"] = random.choice(EQUITY_TICKERS)
            row["STRIKE"] = round(float(np.random.uniform(50, 500)), 2)
            row["MATURITY_DATE"] = str(fake.date_between(start_date="+30d", end_date="+2y"))

        rows.append(row)

    df = pd.DataFrame(rows)

    # Poison pills (security)
    bad_isin_n = max(1, int(CFG["poison_rates"]["bad_isin"] * len(df)))
    df.loc[df.sample(n=bad_isin_n, random_state=11).index, "ID_ISIN"] = "BADISIN"

    m_n = max(1, int(CFG["poison_rates"]["maturity_before_issue"] * len(df)))
    idx = df.sample(n=m_n, random_state=12).index
    issue_dt = pd.to_datetime(df.loc[idx, "ISSUE_DATE"])
    df.loc[idx, "MATURITY_DATE"] = (issue_dt - pd.to_timedelta(30, unit="D")).dt.date.astype(str)

    bonds = df[df["ASSET_TYPE"] == "BOND"]
    if len(bonds) > 0:
        miss_coupon_n = max(1, int(CFG["poison_rates"]["bond_missing_coupon"] * len(bonds)))
        miss_dc_n = max(1, int(CFG["poison_rates"]["bond_missing_daycount"] * len(bonds)))
        df.loc[bonds.sample(n=miss_coupon_n, random_state=13).index, "COUPON_RATE"] = np.nan
        df.loc[bonds.sample(n=miss_dc_n, random_state=14).index, "DAY_COUNT"] = None

    swaps = df[df["ASSET_TYPE"] == "SWAP"]
    if len(swaps) > 0:
        miss_reset_n = max(1, int(CFG["poison_rates"]["swap_missing_reset"] * len(swaps)))
        df.loc[swaps.sample(n=miss_reset_n, random_state=15).index, "RESET_FREQUENCY"] = None

    return df


def gen_trades(funds: pd.DataFrame, secs: pd.DataFrame, n: int) -> pd.DataFrame:
    fund_ids = funds["FUND_ID"].tolist()
    asset_ids = secs["ASSET_ID"].tolist()

    start = CFG["start_date"]
    end = CFG["end_date"]
    all_days = pd.date_range(start, end, freq="B").date
    month_end_days = pd.date_range(start, end, freq="BM").date

    rows = []
    for i in range(n):
        txn_id = f"TXN{i+2000000}"

        if np.random.rand() < 0.15:
            txn_date = random.choice(list(month_end_days))
        else:
            txn_date = random.choice(list(all_days))

        lag = np.random.choice([1, 2, 3], p=[0.45, 0.45, 0.10])
        settle_date = (pd.to_datetime(txn_date) + pd.to_timedelta(lag, unit="D")).date()

        qty = round(float(np.random.uniform(10, 10000)), 4)
        price = round(float(np.random.uniform(5, 500)), 6)

        rows.append({
            "RUN_ID": RUN_ID,
            "SOURCE_SYSTEM": "LEGACY_PLATFORM_A",
            "SOURCE_TXN_ID": txn_id,
            "FUND_ID": random.choice(fund_ids),
            "ASSET_ID": random.choice(asset_ids),
            "TXN_TYPE": np.random.choice(["BUY", "SELL"]),
            "TXN_DATE": str(txn_date),
            "SETTLE_DATE": str(settle_date),
            "QUANTITY": qty,
            "PRICE": price,
            "CURRENCY": random.choice(CURRENCIES)
        })

    df = pd.DataFrame(rows)

    # Poison pills (trades)
    s_n = max(1, int(CFG["poison_rates"]["trade_settle_before_trade"] * len(df)))
    idx = df.sample(n=s_n, random_state=21).index
    tdt = pd.to_datetime(df.loc[idx, "TXN_DATE"])
    df.loc[idx, "SETTLE_DATE"] = (tdt - pd.to_timedelta(1, unit="D")).dt.date.astype(str)

    q_n = max(1, int(CFG["poison_rates"]["trade_qty_missing"] * len(df)))
    df.loc[df.sample(n=q_n, random_state=22).index, "QUANTITY"] = np.nan

    p_n = max(1, int(CFG["poison_rates"]["trade_price_token"] * len(df)))
    df.loc[df.sample(n=p_n, random_state=23).index, "PRICE"] = "SYS_ERROR_NULL"

    return df


def gen_fx_rates(n_days: int) -> pd.DataFrame:
    days = business_days(CFG["end_date"], n_days)
    rows = []
    for d in days:
        for ccy in CURRENCIES:
            rate = 1.0 if ccy == "USD" else round(float(np.random.uniform(0.2, 1.8)), 6)
            rows.append({
                "RUN_ID": RUN_ID,
                "SOURCE_SYSTEM": "LEGACY_PLATFORM_A",
                "FX_DATE": str(d),
                "FROM_CCY": ccy,
                "TO_CCY": "USD",
                "FX_RATE": rate
            })
    df = pd.DataFrame(rows)

    miss = max(1, int(CFG["poison_rates"]["fx_missing_rate"] * len(df)))
    df = df.drop(df.sample(n=miss, random_state=31).index).reset_index(drop=True)
    return df


def gen_prices(secs: pd.DataFrame, n_days: int) -> pd.DataFrame:
    days = business_days(CFG["end_date"], n_days)

    # Keep demo fast: price only for a subset of securities
    priced_assets = secs["ASSET_ID"].sample(n=min(1500, len(secs)), random_state=41).tolist()

    rows = []
    for asset_id in priced_assets:
        base = round(float(np.random.uniform(20, 200)), 4)
        for d in days:
            base = max(0.5, base * (1 + np.random.normal(0, 0.01)))
            rows.append({
                "RUN_ID": RUN_ID,
                "SOURCE_SYSTEM": "LEGACY_PLATFORM_A",
                "PRICE_DATE": str(d),
                "ASSET_ID": asset_id,
                "PRICE": round(float(base), 6),
                "PRICE_CCY": "USD"
            })

    df = pd.DataFrame(rows)

    miss = max(1, int(CFG["poison_rates"]["price_missing"] * len(df)))
    df = df.drop(df.sample(n=miss, random_state=42).index).reset_index(drop=True)
    return df


def main():
    print(f"[RUN_ID] {RUN_ID}")
    print("[INFO] Generating Realistic Pack v2...")

    write_poison_pill_catalog()

    funds = gen_funds(CFG["n_funds"])
    secs = gen_securities(CFG["n_securities"])
    trds = gen_trades(funds, secs, CFG["n_trades"])
    fx = gen_fx_rates(CFG["fx_days"])
    prices = gen_prices(secs, CFG["price_days"])

    paths = {
        "funds": RAW_DIR / f"fund_master_extract_{RUN_ID}.csv",
        "securities": RAW_DIR / f"security_master_extract_{RUN_ID}.csv",
        "trades": RAW_DIR / f"trade_extract_{RUN_ID}.csv",
        "fx": RAW_DIR / f"fx_rates_extract_{RUN_ID}.csv",
        "prices": RAW_DIR / f"price_timeseries_extract_{RUN_ID}.csv",
    }

    funds.to_csv(paths["funds"], index=False)
    secs.to_csv(paths["securities"], index=False)
    trds.to_csv(paths["trades"], index=False)
    fx.to_csv(paths["fx"], index=False)
    prices.to_csv(paths["prices"], index=False)

    print("[DONE] Files created:")
    for k, p in paths.items():
        print(f"  - {k}: {p}")

    print("[STATS]")
    print(f"  funds={len(funds):,} securities={len(secs):,} trades={len(trds):,} fx={len(fx):,} prices={len(prices):,}")


if __name__ == "__main__":
    main()
