import os
import re
from datetime import datetime, timedelta
from numbers import Number

import pandas as pd

# =======================
# CONFIG
# =======================
OFFER_ID = 1357
STATUS_DEFAULT = "pending"
GEO_DEFAULT = "no-geo"
AED_TO_USD_DIVISOR = 3.67
FALLBACK_AFFILIATE_ID = "1"
DAYS_BACK = 60  # include sales from the last N days (excluding today)

INPUT_CSV = "ounass99.csv"
OUTPUT_CSV = "ounass.csv"

# =======================
# PATHS
# =======================
script_dir = os.path.dirname(os.path.abspath(__file__))
updates_dir = os.path.dirname(os.path.dirname(script_dir))
input_dir = os.path.join(updates_dir, "Input data")
output_dir = os.path.join(updates_dir, "output data")
os.makedirs(output_dir, exist_ok=True)

input_path = os.path.join(input_dir, INPUT_CSV)
output_path = os.path.join(output_dir, OUTPUT_CSV)

# =======================
# DATE WINDOW
# =======================
today = datetime.now().date()
start_date = today - timedelta(days=DAYS_BACK)
print(f"Processing window: {start_date} ≤ date < {today}")


# =======================
# HELPERS
# =======================
def normalize_id(value) -> str:
    """Partner IDs sometimes arrive as floats; coerce to clean strings."""
    if pd.isna(value):
        return ""
    text = str(value).strip()
    if not text:
        return ""
    try:
        return str(int(float(text)))
    except (TypeError, ValueError):
        return text


def normalize_coupon(value: str) -> str:
    """Uppercase first token if multiple voucher codes are supplied."""
    if pd.isna(value):
        return ""
    text = str(value).strip().upper()
    if not text:
        return ""
    parts = re.split(r"[;,\s]+", text)
    return parts[0] if parts else text


# =======================
# LOAD & PREP DATA
# =======================
df_raw = pd.read_csv(input_path)

country_series = (
    df_raw["country"] if "country" in df_raw.columns else pd.Series([GEO_DEFAULT] * len(df_raw))
)
voucher_series = (
    df_raw["voucher_codes"]
    if "voucher_codes" in df_raw.columns
    else pd.Series([""] * len(df_raw))
)

df = pd.DataFrame(
    {
        "date": pd.to_datetime(df_raw["conversion_date"], errors="coerce"),
        "partner_id": df_raw["publisher_reference"].apply(normalize_id),
        "customer_type": df_raw["customer_type"].astype(str),
        "category": df_raw["category"].astype(str),
        "sale_aed": pd.to_numeric(df_raw["item_value"], errors="coerce").fillna(0.0),
        "country": country_series,
        "voucher_codes": voucher_series,
    }
)

df = df.dropna(subset=["date"]).copy()
df = df[(df["date"].dt.date >= start_date) & (df["date"].dt.date < today)].copy()

if df.empty:
    print("No rows within the requested date window.")

df["sale_amount"] = df["sale_aed"] / AED_TO_USD_DIVISOR
df["geo"] = df["country"].fillna(GEO_DEFAULT).astype(str).str.strip()
df["coupon_norm"] = df["voucher_codes"].apply(normalize_coupon)

# Identify customer groups
beauty_mask = df["category"].str.contains("beauty", case=False, na=False)
is_new_customer = df["customer_type"].str.contains("new", case=False, na=False)

pct = pd.Series(0.04, index=df.index)
pct.loc[is_new_customer] = 0.07
pct.loc[beauty_mask] = 0.05  # overrides new/existing when category is beauty

df["revenue"] = (df["sale_amount"] * pct).round(4)

# Payout logic: Partner 14298 receives 80% of revenue; others flagged for manual review
df["payout"] = "back to anas"
mask_main_partner = df["partner_id"] == "14298"
df.loc[mask_main_partner, "payout"] = (df.loc[mask_main_partner, "revenue"] * 0.8).round(4)

missing_aff_mask = df["partner_id"] == ""
if missing_aff_mask.any():
    print(f"Rows missing partner ID: {int(missing_aff_mask.sum())}")
    df.loc[missing_aff_mask, "partner_id"] = FALLBACK_AFFILIATE_ID
    df.loc[missing_aff_mask, "revenue"] = 0.0
    df.loc[missing_aff_mask, "payout"] = "back to anas"

output_df = pd.DataFrame(
    {
        "offer": OFFER_ID,
        "affiliate_id": df["partner_id"],
        "date": df["date"].dt.strftime("%m-%d-%Y"),
        "status": STATUS_DEFAULT,
        "payout": df["payout"].apply(lambda v: round(v, 2) if isinstance(v, Number) else v),
        "revenue": df["revenue"].round(2),
        "sale amount": df["sale_amount"].round(2),
        "coupon": df["coupon_norm"],
        "geo": df["geo"].replace("", GEO_DEFAULT),
    }
)

output_df.to_csv(output_path, index=False)

print(f"Saved {len(output_df)} rows to {output_path}")
