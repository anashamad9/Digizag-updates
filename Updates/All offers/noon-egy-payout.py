import pandas as pd
from datetime import datetime, timedelta
import os
import re

# =======================
# CONFIG
# =======================
days_back = 4
OFFER_ID = 1282
STATUS_DEFAULT = "pending"
DEFAULT_PCT_IF_MISSING = 0.0
FALLBACK_AFFILIATE_ID = "1"
GEO = "egy"

# Files
REPORT_CSV_PATTERN = r"^EG DigiZag Coupon Dashboard_Affiliate Summary_Table.*\.csv$"
REPORT_CSV_DEFAULT = "EG DigiZag Coupon Dashboard_Affiliate Summary_Table (2).csv"
AFFILIATE_XLSX  = "Offers Coupons.xlsx"
AFFILIATE_SHEET = "Noon Egypt"   # coupons sheet for this offer

# =======================
# PATHS
# =======================
script_dir = os.path.dirname(os.path.abspath(__file__))
input_dir = os.path.join(script_dir, '..', 'input data')
output_dir = os.path.join(script_dir, '..', 'output data')
os.makedirs(output_dir, exist_ok=True)

affiliate_xlsx_path = os.path.join(input_dir, AFFILIATE_XLSX)
output_file = os.path.join(output_dir, 'noon_egypt.csv')

# =======================
# HELPERS
# =======================
def pick_latest_report():
    """Use the default filename if present; else pick newest file matching the pattern."""
    default_path = os.path.join(input_dir, REPORT_CSV_DEFAULT)
    if os.path.exists(default_path):
        return default_path
    rx = re.compile(REPORT_CSV_PATTERN, re.IGNORECASE)
    cands = [f for f in os.listdir(input_dir) if rx.match(f)]
    if not cands:
        raise FileNotFoundError(
            "Could not find report CSV. Looked for default "
            f"'{REPORT_CSV_DEFAULT}' or any file matching pattern '{REPORT_CSV_PATTERN}'."
        )
    best = max(cands, key=lambda f: os.path.getmtime(os.path.join(input_dir, f)))
    return os.path.join(input_dir, best)

def normalize_coupon(x: str) -> str:
    if pd.isna(x):
        return ""
    s = str(x).replace("\u00A0", " ").strip().upper()  # normalize NBSP too
    parts = re.split(r"[;,\s]+", s)
    return parts[0] if parts else s

def load_affiliate_mapping_from_xlsx(xlsx_path: str, sheet_name: str) -> pd.DataFrame:
    """
    Return mapping with columns:
      code_norm, affiliate_ID (from 'ID' or 'affiliate_ID'), type_norm,
      pct_fraction (for 'revenue'/'sale'), fixed_amount (for 'fixed')
    """
    df_sheet = pd.read_excel(xlsx_path, sheet_name=sheet_name, dtype=str)
    cols_lower = {str(c).lower().strip(): c for c in df_sheet.columns}

    def need(name):
        col = cols_lower.get(name)
        if not col:
            raise ValueError(f"[{sheet_name}] must contain '{name}' column.")
        return col

    code_col = need("code")
    aff_col  = cols_lower.get("id") or cols_lower.get("affiliate_id")
    type_col = need("type")
    payout_col = (cols_lower.get("payout")
                  or cols_lower.get("new customer payout")
                  or cols_lower.get("old customer payout"))

    if not aff_col:
        raise ValueError(f"[{sheet_name}] must contain an 'ID' (or 'affiliate_ID') column.")
    if not payout_col:
        raise ValueError(f"[{sheet_name}] must contain a payout column (e.g., 'payout').")

    payout_raw = df_sheet[payout_col].astype(str).str.replace("%", "", regex=False).str.strip()
    payout_num = pd.to_numeric(payout_raw, errors="coerce")
    type_norm  = df_sheet[type_col].astype(str).str.strip().str.lower().replace({"": None})

    pct_fraction = payout_num.where(type_norm.isin(["revenue", "sale"])).apply(
        lambda v: (v/100.0) if pd.notna(v) and v > 1 else (v if pd.notna(v) else DEFAULT_PCT_IF_MISSING)
    )
    fixed_amount = payout_num.where(type_norm.eq("fixed"))

    out = pd.DataFrame({
        "code_norm": df_sheet[code_col].apply(normalize_coupon),
        "affiliate_ID": df_sheet[aff_col].fillna("").astype(str).str.strip(),
        "type_norm": type_norm,
        "pct_fraction": pct_fraction,
        "fixed_amount": fixed_amount
    }).dropna(subset=["code_norm"])

    return out.drop_duplicates(subset=["code_norm"], keep="last")

def get_revenue_per_order(tier: str) -> float:
    """
    Map the GMV tier label (gmv_tag_usd) to a fixed revenue per order.
    Matching is whitespace-insensitive and case-insensitive.
    """
    if tier is None or (isinstance(tier, float) and pd.isna(tier)):
        return 0.0
    t = re.sub(r"\s+", " ", str(tier)).strip().lower()

    table = {
        "4.75 - 14.25": 0.30,
        "14.26 - 23.85": 0.70,
        "23.86 - 37.24": 1.30,
        "37.25 - 59.40": 2.20,
        "59.41 - 72.00": 3.25,
        "72.01 - 110.00": 4.25,
        "above 110.01": 7.00,
    }
    for k, v in table.items():
        if re.sub(r"\s+", " ", k).strip().lower() in t:
            return v
    return 0.0

def get_col(df: pd.DataFrame, *candidates: str) -> str:
    """Find a column by case-insensitive, space-normalized name."""
    low = {re.sub(r"\s+", " ", str(c)).strip().lower(): c for c in df.columns}
    for cand in candidates:
        key = re.sub(r"\s+", " ", cand).strip().lower()
        if key in low:
            return low[key]
    raise KeyError(f"None of the expected columns found: {candidates}")

# =======================
# LOAD & FILTER REPORT
# =======================
end_date = datetime.now().date() + timedelta(days=1)     # include "today"
start_date = end_date - timedelta(days=days_back + 1)
today = datetime.now().date()
print(f"Current date: {today}, Start date (days_back={days_back}): {start_date}")

input_file = pick_latest_report()
print(f"Using input file: {os.path.basename(input_file)}")

df = pd.read_csv(input_file)

# Resolve columns robustly
date_col   = get_col(df, "egy_date")
orders_col = get_col(df, "orders")
gmv_col    = get_col(df, "gmv_usd", "gmv usd", "gmv (usd)")
tier_col   = get_col(df, "gmv_tag_usd", "gmv tag usd")
code_col   = get_col(df, "coupon code", "coupon", "code")

# Parse & date filter
df[date_col] = pd.to_datetime(df[date_col], format='%b %d, %Y', errors='coerce')
df = df.dropna(subset=[date_col])
df = df[(df[date_col].dt.date >= start_date) & (df[date_col].dt.date < end_date)].copy()

# =======================
# EXPAND & DERIVE
# =======================
orders = pd.to_numeric(df[orders_col], errors='coerce').fillna(0).astype(int).clip(lower=0)
df_expanded = df.loc[df.index.repeat(orders)].reset_index(drop=True)

# Per-order values
den = pd.to_numeric(df_expanded[orders_col], errors='coerce').replace(0, pd.NA).fillna(1)
gmv_usd = pd.to_numeric(df_expanded[gmv_col], errors='coerce').fillna(0.0)

df_expanded['sale_amount'] = (gmv_usd / den).astype(float)
df_expanded['revenue'] = df_expanded[tier_col].apply(get_revenue_per_order)

# Prepare join keys
df_expanded['coupon_norm'] = df_expanded[code_col].apply(normalize_coupon)

# =======================
# JOIN AFFILIATE MAPPING (type-aware)
# =======================
map_df = load_affiliate_mapping_from_xlsx(affiliate_xlsx_path, AFFILIATE_SHEET)
df_joined = df_expanded.merge(map_df, how="left", left_on="coupon_norm", right_on="code_norm")

# Missing affiliate?
missing_aff_mask = df_joined['affiliate_ID'].isna() | (df_joined['affiliate_ID'].astype(str).str.strip() == "")

# Normalize mapping fields
df_joined['affiliate_ID'] = df_joined['affiliate_ID'].fillna("").astype(str).str.strip()
df_joined['type_norm'] = df_joined['type_norm'].fillna("revenue")
df_joined['pct_fraction'] = df_joined['pct_fraction'].fillna(DEFAULT_PCT_IF_MISSING)

# =======================
# PAYOUT
# =======================
payout = pd.Series(0.0, index=df_joined.index)

mask_rev = df_joined['type_norm'].str.lower().eq('revenue')
payout.loc[mask_rev] = df_joined.loc[mask_rev, 'revenue'] * df_joined.loc[mask_rev, 'pct_fraction']

mask_sale = df_joined['type_norm'].str.lower().eq('sale')
payout.loc[mask_sale] = df_joined.loc[mask_sale, 'sale_amount'] * df_joined.loc[mask_sale, 'pct_fraction']

mask_fixed = df_joined['type_norm'].str.lower().eq('fixed')
payout.loc[mask_fixed] = df_joined.loc[mask_fixed, 'fixed_amount'].fillna(0.0)

# Enforce fallback for no affiliate
payout.loc[missing_aff_mask] = 0.0
df_joined.loc[missing_aff_mask, 'affiliate_ID'] = FALLBACK_AFFILIATE_ID

df_joined['payout'] = payout.round(2)

# =======================
# BUILD OUTPUT (NEW STRUCTURE)
# =======================
output_df = pd.DataFrame({
    'offer': OFFER_ID,
    'affiliate_id': df_joined['affiliate_ID'],
    'date': df_joined[date_col].dt.strftime('%m-%d-%Y'),
    'status': STATUS_DEFAULT,
    'payout': df_joined['payout'],
    'revenue': df_joined['revenue'].round(2),
    'sale amount': df_joined['sale_amount'].round(2),
    'coupon': df_joined['coupon_norm'],
    'geo': GEO,
})

# =======================
# SAVE
# =======================
output_df.to_csv(output_file, index=False)

print(f"Saved: {output_file}")
print(
    f"Rows: {len(output_df)} | "
    f"No-affiliate coupons (set aff={FALLBACK_AFFILIATE_ID}, payout=0): {int(missing_aff_mask.sum())}"
)
print(f"Date range processed: {start_date} to {end_date - timedelta(days=1)}")
