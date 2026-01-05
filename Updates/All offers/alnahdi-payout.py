import os
import re
from typing import Tuple

import pandas as pd

OFFER_ID = 1362
STATUS_DEFAULT = "pending"
FALLBACK_AFFILIATE_ID = "1"
DEFAULT_GEO = "ksa"
CURRENCY_RATE_SAR_TO_USD = 3.75
REVENUE_PCT_OF_SALE = 0.05  # reported revenue is 5% of sale amount

DATA_XLSX = "Affiliate Digizag X Nahdi from 20 to 27 December.xlsx"
DATA_SHEET = "DIGIZAG"
COUPON_BOOK = "Offers Coupons.xlsx"
COUPON_SHEET = "ALNAHDI PHARMACY"
OUTPUT_CSV = "alnahdi.csv"
DATE_COL = "Calendar Date"
COUPON_COL = "Online Coupon Code"
REVENUE_COL = "Sum of $ Revenue (After C Discount)"

script_dir = os.path.dirname(os.path.abspath(__file__))
input_dir = os.path.join(script_dir, "..", "input data")
output_dir = os.path.join(script_dir, "..", "output data")
os.makedirs(output_dir, exist_ok=True)

data_path = os.path.join(input_dir, DATA_XLSX)
coupon_path = os.path.join(input_dir, COUPON_BOOK)
output_path = os.path.join(output_dir, OUTPUT_CSV)


def normalize_coupon(value: str) -> str:
    """Uppercase and strip non-alphanumeric tokens used around the real code."""
    if pd.isna(value):
        return ""
    cleaned = re.sub(r"[^A-Za-z0-9]", "", str(value)).upper()
    return cleaned


def load_coupon_mapping(path: str, sheet: str) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Coupon workbook not found: {path}")

    raw = pd.read_excel(path, sheet_name=sheet)
    df = raw.copy()
    df["code_norm"] = df.get("Code", "").apply(normalize_coupon)

    def fmt_affiliate(x) -> str:
        if pd.isna(x):
            return ""
        try:
            xf = float(x)
            if xf.is_integer():
                return str(int(xf))
        except Exception:
            pass
        return str(x).strip()

    df["affiliate_id"] = df.get("ID", "").apply(fmt_affiliate)
    df["type_norm"] = (
        df.get("type", "revenue").astype(str).str.strip().str.lower().replace({"": "revenue"})
    )

    def pct(series) -> pd.Series:
        numeric = pd.to_numeric(series, errors="coerce")
        return numeric.apply(lambda v: v / 100 if pd.notna(v) and v > 1 else v)

    payout_new = pct(df.get("new customer payout"))
    payout_old = pct(df.get("old customer payout"))
    df["payout_rate"] = payout_new.combine_first(payout_old).fillna(0.0)

    df["geo_norm"] = (
        df.get("Geo", DEFAULT_GEO).fillna(DEFAULT_GEO).astype(str).str.strip().str.lower()
    )

    df = df[df["code_norm"] != ""].copy()
    df = df[["code_norm", "affiliate_id", "type_norm", "payout_rate", "geo_norm"]]
    return df.drop_duplicates(subset=["code_norm"], keep="last")


def compute_payouts() -> Tuple[pd.DataFrame, pd.Series]:
    if not os.path.exists(data_path):
        raise FileNotFoundError(f"Data file not found: {data_path}")

    coupons = load_coupon_mapping(coupon_path, COUPON_SHEET)
    # Load the specified sheet (or fallback to the first sheet if not found)
    try:
        source = pd.read_excel(data_path, sheet_name=DATA_SHEET)
    except ValueError:
        source = pd.read_excel(data_path, sheet_name=0)

    source.columns = [str(c).strip() for c in source.columns]
    required = [DATE_COL, COUPON_COL, REVENUE_COL]
    missing_cols = [c for c in required if c not in source.columns]
    if missing_cols:
        raise KeyError(f"Missing columns in data file: {missing_cols}")

    # Drop total rows and forward-fill dates down the group
    total_mask = source[DATE_COL].astype(str).str.contains("total", case=False, na=False)
    grand_mask = source[DATE_COL].astype(str).str.contains("grand", case=False, na=False)
    source = source[~(total_mask | grand_mask)].copy()

    source["date"] = pd.to_datetime(source[DATE_COL], errors="coerce")
    source["date"] = source["date"].ffill()
    source = source.dropna(subset=["date"]).copy()

    source["coupon_norm"] = source[COUPON_COL].apply(normalize_coupon)
    total_coupon_mask = source[COUPON_COL].astype(str).str.contains("total", case=False, na=False)
    source = source[~total_coupon_mask].copy()
    source = source[source["coupon_norm"] != ""].copy()

    # Column holds sale amount in SAR; convert to USD and compute revenue as 5% of sale.
    sale_sar = pd.to_numeric(source.get(REVENUE_COL), errors="coerce").fillna(0.0)
    sale_usd = sale_sar / CURRENCY_RATE_SAR_TO_USD
    source["sale amount"] = sale_usd
    source["revenue"] = sale_usd * REVENUE_PCT_OF_SALE

    merged = source.merge(coupons, how="left", left_on="coupon_norm", right_on="code_norm")
    merged["affiliate_id"] = merged["affiliate_id"].replace("", pd.NA).fillna(
        FALLBACK_AFFILIATE_ID
    )
    merged["geo_norm"] = merged["geo_norm"].replace("", pd.NA).fillna(DEFAULT_GEO)
    merged["type_norm"] = merged["type_norm"].fillna("revenue").str.lower()
    merged["payout_rate"] = pd.to_numeric(merged["payout_rate"], errors="coerce").fillna(0.0)

    payout = merged["revenue"] * merged["payout_rate"]
    sale_mask = merged["type_norm"].eq("sale")
    payout.loc[sale_mask] = (
        merged.loc[sale_mask, "sale amount"] * merged.loc[sale_mask, "payout_rate"]
    )
    payout = payout.round(2)

    output = pd.DataFrame(
        {
            "offer": OFFER_ID,
            "affiliate_id": merged["affiliate_id"],
            "date": merged["date"].dt.strftime("%m-%d-%Y"),
            "status": STATUS_DEFAULT,
            "payout": payout,
            "revenue": merged["revenue"].round(2),
            "sale amount": merged["sale amount"].round(2),
            "coupon": merged["coupon_norm"],
            "geo": merged["geo_norm"],
        }
    )

    missing_aff = merged["affiliate_id"] == FALLBACK_AFFILIATE_ID
    return output, missing_aff


def main() -> None:
    output_df, missing_aff = compute_payouts()
    output_df.to_csv(output_path, index=False)

    print(f"Saved: {output_path}")
    print(
        f"Rows: {len(output_df)} | Fallback affiliates (set to {FALLBACK_AFFILIATE_ID}): {int(missing_aff.sum())}"
    )
    if not output_df.empty:
        print(f"Date range: {output_df['date'].min()} to {output_df['date'].max()}")


if __name__ == "__main__":
    main()
