import argparse
import csv
import json
import os
import random
from datetime import date, datetime, timedelta
from typing import Dict, List, Set, Tuple

from faker import Faker


def parse_args():
    p = argparse.ArgumentParser(description="Generate synthetic T+1 settlement risk dataset (trades + fx + holidays).")
    p.add_argument("--rows", type=int, default=50000)
    p.add_argument("--seed", type=int, default=87)
    p.add_argument("--out", type=str, default="data_out")
    return p.parse_args()


def daterange(start: date, end: date) -> List[date]:
    days = (end - start).days
    return [start + timedelta(days=i) for i in range(days + 1)]


def is_weekend(d: date) -> bool:
    return d.weekday() >= 5


def business_day_add(start_d: date, n: int, holidays: Set[date]) -> date:
    """Add n business days after start_d, skipping weekends and given holidays."""
    x = start_d
    added = 0
    while added < n:
        x += timedelta(days=1)
        if (not is_weekend(x)) and (x not in holidays):
            added += 1
    return x


def make_security_id(rng: random.Random) -> str:
    return f"SEC{rng.randint(100000, 999999)}"


def make_fund_id(rng: random.Random) -> str:
    return f"FND{rng.randint(1000, 9999)}"


def write_csv(path: str, fieldnames: List[str], rows: List[dict]):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


def generate_holiday_calendar(start: date, end: date, rng: random.Random) -> Tuple[List[dict], Dict[str, Set[date]]]:
    """
    Creates two calendars:
      - UK_LSE
      - EU_XETRA

    Intentionally makes EU calendar incomplete by removing ~35% of its holidays.
    Returns:
      rows: holiday_calendar.csv rows
      hol_by_cal: dict calendar_code -> set(holiday_date)
    """
    all_days = [d for d in daterange(start, end) if not is_weekend(d)]
    rng.shuffle(all_days)

    # seed basic "known" holiday-like date
    uk_hols = {date(2026, 1, 1)}
    eu_hols = {date(2026, 1, 1)}

    # add random holidays in-range
    uk_hols |= set(all_days[:8])
    eu_hols |= set(all_days[8:16])

    rows = []
    for cal, hols in [("UK_LSE", uk_hols), ("EU_XETRA", eu_hols)]:
        for d in sorted([x for x in hols if start <= x <= end]):
            rows.append({
                "CALENDAR_CODE": cal,
                "HOLIDAY_DATE": d.isoformat(),
                "HOLIDAY_NAME": f"HOL_{cal}_{d.isoformat()}",
                "IS_HOLIDAY": "TRUE"
            })

    # Intentional holiday gaps: remove ~35% EU holidays (making calendar incomplete)
    eu_rows = [r for r in rows if r["CALENDAR_CODE"] == "EU_XETRA"]
    drop_n = max(1, int(len(eu_rows) * 0.35))
    rng.shuffle(eu_rows)
    drop_set = set((r["CALENDAR_CODE"], r["HOLIDAY_DATE"]) for r in eu_rows[:drop_n])

    rows = [r for r in rows if (r["CALENDAR_CODE"], r["HOLIDAY_DATE"]) not in drop_set]

    # rebuild holiday sets from remaining rows
    hol_by_cal: Dict[str, Set[date]] = {"UK_LSE": set(), "EU_XETRA": set()}
    for r in rows:
        hol_by_cal[r["CALENDAR_CODE"]].add(date.fromisoformat(r["HOLIDAY_DATE"]))

    return rows, hol_by_cal


def generate_fx_rates(start: date, end: date, rng: random.Random) -> List[dict]:
    """
    Creates FX rates for GBP to EUR/USD/JPY for business days.
    Intentionally removes some USD/JPY rates on random dates to simulate FX gaps.
    """
    base_ccy = "GBP"
    quote_ccys = ["EUR", "USD", "JPY"]
    all_days = [d for d in daterange(start, end) if not is_weekend(d)]

    rows = []
    for d in all_days:
        for q in quote_ccys:
            rows.append({
                "FX_DATE": d.isoformat(),
                "BASE_CCY": base_ccy,
                "QUOTE_CCY": q,
                "FX_RATE": f"{rng.uniform(0.5, 1.8):.6f}",
                "RATE_SOURCE": "SYNTH_VENDOR"
            })

    # Intentional FX gaps: remove some (date, quote_ccy)
    unique_dates = sorted(list({r["FX_DATE"] for r in rows}))
    rng.shuffle(unique_dates)

    gap_dates = set(unique_dates[:max(3, int(len(unique_dates) * 0.03))])  # ~3% dates
    gap_ccys = {"USD", "JPY"}  # bias gaps to these

    rows = [r for r in rows if not (r["FX_DATE"] in gap_dates and r["QUOTE_CCY"] in gap_ccys)]
    return rows


def generate_trades(
    n: int,
    start: date,
    end: date,
    rng: random.Random,
    fake: Faker,
    hol_by_cal: Dict[str, Set[date]]
) -> List[dict]:
    """
    trades_tplus1.csv:
      - Trade Date
      - Target Settle Date (T+1 business day using calendar)
      - Actual Settle Date (some late T+3/T+4)
      - Calendar code (some missing/invalid)
      - Currency (some non-base more likely to create FX gaps)
    """
    markets = ["UK", "EU"]
    instrument_types = ["EQUITY", "BOND"]
    sides = ["BUY", "SELL"]

    all_days = daterange(start, end)
    rows = []

    for _ in range(n):
        market = rng.choice(markets)
        cal_code = "UK_LSE" if market == "UK" else "EU_XETRA"

        trade_dt = rng.choice(all_days)
        hols = hol_by_cal.get(cal_code, set())

        target_settle = business_day_add(trade_dt, 1, hols)
        actual_settle = target_settle

        instr = rng.choice(instrument_types)
        side = rng.choice(sides)

        qty = rng.randint(10, 5000)
        price = round(rng.uniform(5, 250), 4)
        notional = round(qty * price, 2)

        # currency mix
        if instr == "BOND":
            ccy = rng.choice(["EUR", "USD", "GBP"])
        else:
            ccy = rng.choice(["GBP", "EUR", "GBP", "EUR", "GBP"])

        rows.append({
            "TRADE_ID": f"TRD{rng.randint(10_000_000, 99_999_999)}",
            "MARKET": market,
            "CALENDAR_CODE": cal_code,
            "TRADE_DATE": trade_dt.isoformat(),
            "TARGET_SETTLE_DATE": target_settle.isoformat(),
            "ACTUAL_SETTLE_DATE": actual_settle.isoformat(),
            "INSTRUMENT_TYPE": instr,
            "SIDE": side,
            "SECURITY_ID": make_security_id(rng),
            "FUND_ID": make_fund_id(rng),
            "CCY": ccy,
            "QTY": str(qty),
            "PRICE": f"{price:.4f}",
            "NOTIONAL": f"{notional:.2f}",
            "COUNTERPARTY": fake.company().replace(",", ""),
            "BOOK": f"BK{rng.randint(100, 999)}"
        })

    # Inject breaks:

    # 1) Latency risk: ~8% settle late T+3/T+4 vs target
    pct_latency = 0.08
    latency_n = int(n * pct_latency)
    latency_idxs = rng.sample(range(n), latency_n)
    for ix in latency_idxs:
        target = date.fromisoformat(rows[ix]["TARGET_SETTLE_DATE"])
        late_days = rng.choice([2, 3])  # target +2 => T+3, +3 => T+4
        rows[ix]["ACTUAL_SETTLE_DATE"] = (target + timedelta(days=late_days)).isoformat()

    # 2) Holiday gaps: ~5% reference a missing calendar code
    pct_hgap = 0.05
    hgap_n = int(n * pct_hgap)
    hgap_idxs = rng.sample(range(n), hgap_n)
    for ix in hgap_idxs:
        rows[ix]["CALENDAR_CODE"] = "EU_MISSING_CAL"

    # 3) FX gaps: ~6% force USD/JPY (more likely missing FX on gap dates)
    pct_fxgap = 0.06
    fxgap_n = int(n * pct_fxgap)
    fxgap_idxs = rng.sample(range(n), fxgap_n)
    for ix in fxgap_idxs:
        rows[ix]["CCY"] = rng.choice(["USD", "JPY"])

    return rows


def main():
    args = parse_args()
    os.makedirs(args.out, exist_ok=True)

    rng = random.Random(args.seed)
    fake = Faker()
    Faker.seed(args.seed)

    # Demo range
    start_d = date(2026, 1, 1)
    end_d = date(2026, 3, 31)

    holiday_rows, hol_by_cal = generate_holiday_calendar(start_d, end_d, rng)
    fx_rows = generate_fx_rates(start_d, end_d, rng)
    trade_rows = generate_trades(args.rows, start_d, end_d, rng, fake, hol_by_cal)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    trades_file = os.path.join(args.out, f"trades_tplus1_{ts}.csv")
    fx_file = os.path.join(args.out, f"fx_rates_{ts}.csv")
    hol_file = os.path.join(args.out, f"holiday_calendar_{ts}.csv")
    manifest_file = os.path.join(args.out, f"manifest_{ts}.json")

    write_csv(
        trades_file,
        [
            "TRADE_ID","MARKET","CALENDAR_CODE","TRADE_DATE","TARGET_SETTLE_DATE","ACTUAL_SETTLE_DATE",
            "INSTRUMENT_TYPE","SIDE","SECURITY_ID","FUND_ID","CCY","QTY","PRICE","NOTIONAL","COUNTERPARTY","BOOK"
        ],
        trade_rows
    )
    write_csv(
        fx_file,
        ["FX_DATE","BASE_CCY","QUOTE_CCY","FX_RATE","RATE_SOURCE"],
        fx_rows
    )
    write_csv(
        hol_file,
        ["CALENDAR_CODE","HOLIDAY_DATE","HOLIDAY_NAME","IS_HOLIDAY"],
        holiday_rows
    )

    manifest = {
        "generated_at": ts,
        "seed": args.seed,
        "rows_trades": len(trade_rows),
        "rows_fx": len(fx_rows),
        "rows_holidays": len(holiday_rows),
        "files": {
            "trades": os.path.basename(trades_file),
            "fx_rates": os.path.basename(fx_file),
            "holiday_calendar": os.path.basename(hol_file)
        }
    }
    with open(manifest_file, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)

    print("✅ Generated files:")
    print(" -", trades_file)
    print(" -", fx_file)
    print(" -", hol_file)
    print(" -", manifest_file)


if __name__ == "__main__":
    main()
