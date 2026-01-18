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
DEFAULT_PCT_IF_MISSING = 0.0

INPUT_CSV = "ounass99.csv"
OUTPUT_CSV = "ounass.csv"
AFFILIATE_XLSX = "Offers Coupons.xlsx"
AFFILIATE_SHEET = "Ounass-Links"

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
affiliate_map_path = os.path.join(input_dir, AFFILIATE_XLSX)

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


def load_affiliate_mapping_from_xlsx(xlsx_path: str, sheet_name: str) -> pd.DataFrame:
    """
    Load affiliate-level payout settings from the Offers Coupons workbook.
    Supports revenue/sale/fixed types with new/old payout columns.
    """
    df_sheet = pd.read_excel(xlsx_path, sheet_name=sheet_name, dtype=str)
    cols_lower = {str(c).lower().strip(): c for c in df_sheet.columns}

    aff_col = cols_lower.get("id") or cols_lower.get("affiliate_id")
    type_col = cols_lower.get("type")
    payout_col = cols_lower.get("payout")
    new_col = cols_lower.get("new customer payout")
    old_col = cols_lower.get("old customer payout")

    if not aff_col:
        raise ValueError(f"[{sheet_name}] must contain an 'ID' (or 'affiliate_ID') column.")
    if not (payout_col or new_col or old_col):
        raise ValueError(
            f"[{sheet_name}] must contain at least one payout column "
            "(e.g., 'payout', 'new customer payout', or 'old customer payout')."
        )

    def extract_numeric(col_name: str) -> pd.Series:
        if not col_name:
            return pd.Series([pd.NA] * len(df_sheet), dtype="Float64")
        raw = df_sheet[col_name].astype(str).str.replace("%", "", regex=False).str.strip()
        return pd.to_numeric(raw, errors="coerce")

    payout_any = extract_numeric(payout_col)
    payout_new_raw = extract_numeric(new_col).fillna(payout_any)
    payout_old_raw = extract_numeric(old_col).fillna(payout_any)

    type_norm = (
        df_sheet[type_col].astype(str).str.strip().str.lower()
        if type_col
        else pd.Series(["revenue"] * len(df_sheet))
    )
    type_norm = type_norm.replace("", "revenue").fillna("revenue")

    def pct_from(values: pd.Series, type_series: pd.Series) -> pd.Series:
        pct = values.where(type_series.isin(["revenue", "sale"]))
        return pct.apply(
            lambda v: (v / 100.0) if pd.notna(v) and v > 1 else (v if pd.notna(v) else pd.NA)
        )

    def fixed_from(values: pd.Series, type_series: pd.Series) -> pd.Series:
        return values.where(type_series.eq("fixed"))

    pct_new = pct_from(payout_new_raw, type_norm)
    pct_old = pct_from(payout_old_raw, type_norm)
    pct_new = pct_new.fillna(pct_old)
    pct_old = pct_old.fillna(pct_new)

    fixed_new = fixed_from(payout_new_raw, type_norm)
    fixed_old = fixed_from(payout_old_raw, type_norm)
    fixed_new = fixed_new.fillna(fixed_old)
    fixed_old = fixed_old.fillna(fixed_new)

    mapping = pd.DataFrame(
        {
            "affiliate_id_norm": df_sheet[aff_col].apply(normalize_id),
            "type_norm": type_norm,
            "pct_new": pd.to_numeric(pct_new, errors="coerce").fillna(DEFAULT_PCT_IF_MISSING),
            "pct_old": pd.to_numeric(pct_old, errors="coerce").fillna(DEFAULT_PCT_IF_MISSING),
            "fixed_new": pd.to_numeric(fixed_new, errors="coerce"),
            "fixed_old": pd.to_numeric(fixed_old, errors="coerce"),
        }
    )

    mapping = mapping[mapping["affiliate_id_norm"].astype(str).str.strip() != ""]
    return mapping.drop_duplicates(subset=["affiliate_id_norm"], keep="last")


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

if not os.path.exists(affiliate_map_path):
    raise FileNotFoundError(f"Affiliate workbook not found: {affiliate_map_path}")

affiliate_map = load_affiliate_mapping_from_xlsx(affiliate_map_path, AFFILIATE_SHEET)
df = df.merge(affiliate_map, how="left", left_on="partner_id", right_on="affiliate_id_norm")

# Normalize mapping columns
df["type_norm"] = df["type_norm"].fillna("revenue").astype(str).str.lower()
df["pct_new"] = pd.to_numeric(df["pct_new"], errors="coerce").fillna(DEFAULT_PCT_IF_MISSING)
df["pct_old"] = pd.to_numeric(df["pct_old"], errors="coerce").fillna(DEFAULT_PCT_IF_MISSING)
df["fixed_new"] = pd.to_numeric(df["fixed_new"], errors="coerce")
df["fixed_old"] = pd.to_numeric(df["fixed_old"], errors="coerce")

missing_aff_mask = df["partner_id"].eq("") | df["affiliate_id_norm"].isna() | (
    df["affiliate_id_norm"].astype(str).str.strip() == ""
)
if missing_aff_mask.any():
    print(f"Rows missing affiliate mapping: {int(missing_aff_mask.sum())}")
    df.loc[missing_aff_mask, "partner_id"] = df.loc[missing_aff_mask, "partner_id"].replace("", FALLBACK_AFFILIATE_ID)

pct_effective = df["pct_new"].where(is_new_customer, df["pct_old"])
pct_effective = pd.to_numeric(pct_effective, errors="coerce").fillna(DEFAULT_PCT_IF_MISSING)

# Compute payout by type
payout = pd.Series(0.0, index=df.index, dtype=float)
mask_rev = df["type_norm"].eq("revenue")
mask_sale = df["type_norm"].eq("sale")
mask_fixed = df["type_norm"].eq("fixed")

payout.loc[mask_rev] = (df.loc[mask_rev, "revenue"] * pct_effective.loc[mask_rev]).fillna(0.0)
payout.loc[mask_sale] = (df.loc[mask_sale, "sale_amount"] * pct_effective.loc[mask_sale]).fillna(0.0)
payout.loc[mask_fixed] = df.loc[mask_fixed, "fixed_new"].where(
    is_new_customer, df.loc[mask_fixed, "fixed_old"]
).fillna(0.0)

payout.loc[missing_aff_mask] = 0.0

df["payout"] = payout.round(4)
df["affiliate_output"] = df["partner_id"].where(~missing_aff_mask, FALLBACK_AFFILIATE_ID)

output_df = pd.DataFrame(
    {
        "offer": OFFER_ID,
        "affiliate_id": df["affiliate_output"],
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
