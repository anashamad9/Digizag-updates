import pandas as pd
from datetime import datetime, timedelta
import os
import re

# =======================
# CONFIG
# =======================
<<<<<<< HEAD
days_back = 13
=======
days_back = 60
>>>>>>> 0d89299 (D)
OFFER_ID = 1166
STATUS_DEFAULT = "pending"
DEFAULT_PCT_IF_MISSING = 0.0
FALLBACK_AFFILIATE_ID = "1"

# Files
REPORT_XLSX_DEFAULT = "sales (12).xlsx"
REPORT_XLSX_PATTERN = r"^sales \(\d+\)\.xlsx$"  # fallback: newest matching file
AFFILIATE_XLSX  = "Offers Coupons.xlsx"
AFFILIATE_SHEET = "Noon GCC"

# =======================
# PATHS
# =======================
script_dir = os.path.dirname(os.path.abspath(__file__))
updates_dir = os.path.dirname(os.path.dirname(script_dir))
input_dir = os.path.join(updates_dir, 'Input data')
output_dir = os.path.join(updates_dir, 'output data')
os.makedirs(output_dir, exist_ok=True)

affiliate_xlsx_path = os.path.join(input_dir, AFFILIATE_XLSX)
output_file = os.path.join(output_dir, 'noon.csv')

# =======================
# HELPERS
# =======================
def pick_report_path() -> str:
    """Use the default report if present; otherwise pick the newest 'sales (N).xlsx'."""
    default_path = os.path.join(input_dir, REPORT_XLSX_DEFAULT)
    if os.path.exists(default_path):
        return default_path
    rx = re.compile(REPORT_XLSX_PATTERN, re.IGNORECASE)
    cands = [f for f in os.listdir(input_dir) if rx.match(f)]
    if not cands:
        raise FileNotFoundError(
            f"No report found. Expected '{REPORT_XLSX_DEFAULT}' or files matching '{REPORT_XLSX_PATTERN}'."
        )
    newest = max(cands, key=lambda f: os.path.getmtime(os.path.join(input_dir, f)))
    return os.path.join(input_dir, newest)

def normalize_coupon(x: str) -> str:
    if pd.isna(x):
        return ""
    s = str(x).replace("\u00A0", " ").strip().upper()
    parts = re.split(r"[;,\s]+", s)
    return parts[0] if parts else s

def get_col(df: pd.DataFrame, *candidates: str) -> str:
    """Find a column by case-insensitive, space-normalized name; raise if none found."""
    low = {re.sub(r"\s+", " ", str(c)).strip().lower(): c for c in df.columns}
    for cand in candidates:
        key = re.sub(r"\s+", " ", cand).strip().lower()
        if key in low:
            return low[key]
    raise KeyError(f"None of the expected columns found: {candidates}")

def load_affiliate_mapping_from_xlsx(xlsx_path: str, sheet_name: str) -> pd.DataFrame:
    """
    Returns mapping with new/old payout values:
      code_norm, affiliate_ID, type_norm, pct_new, pct_old, fixed_new, fixed_old
    Accepts 'ID' or 'affiliate_ID' and parses payout columns as % (for revenue/sale) or
    fixed amounts (for fixed).
    """
    df_sheet = pd.read_excel(xlsx_path, sheet_name=sheet_name, dtype=str)
    cols_lower = {str(c).lower().strip(): c for c in df_sheet.columns}

    def need(name: str) -> str:
        col = cols_lower.get(name)
        if not col:
            raise ValueError(f"[{sheet_name}] must contain '{name}' column.")
        return col

    code_col = need("code")
    aff_col = cols_lower.get("id") or cols_lower.get("affiliate_id")
    type_col = need("type")
    payout_col = cols_lower.get("payout")
    new_col = cols_lower.get("new customer payout")
    old_col = cols_lower.get("old customer payout")

    if not aff_col:
        raise ValueError(f"[{sheet_name}] must contain an 'ID' (or 'affiliate_ID') column.")
    if not (payout_col or new_col or old_col):
        raise ValueError(
            f"[{sheet_name}] must contain at least one payout column (e.g., 'payout', 'new customer payout')."
        )

    def extract_numeric(col_name: str) -> pd.Series:
        if not col_name:
            return pd.Series([pd.NA] * len(df_sheet), dtype="Float64")
        raw = df_sheet[col_name].astype(str).str.replace("%", "", regex=False).str.strip()
        return pd.to_numeric(raw, errors="coerce")

    payout_any = extract_numeric(payout_col)
    payout_new_raw = extract_numeric(new_col).fillna(payout_any)
    payout_old_raw = extract_numeric(old_col).fillna(payout_any)

    type_norm = df_sheet[type_col].astype(str).str.strip().str.lower().replace({"": None})
    type_norm = type_norm.fillna("revenue")

    def pct_from(values: pd.Series, type_series: pd.Series) -> pd.Series:
        pct = values.where(type_series.isin(["revenue", "sale"]))
        return pct.apply(
            lambda v: (v / 100.0) if pd.notna(v) and v > 1 else (v if pd.notna(v) else pd.NA)
        )

    def fixed_from(values: pd.Series, type_series: pd.Series) -> pd.Series:
        return values.where(type_series.eq("fixed"))

    pct_new = pct_from(payout_new_raw, type_norm)
    pct_old = pct_from(payout_old_raw, type_norm)

    # If only one payout value exists, reuse it for the missing side.
    pct_new = pct_new.fillna(pct_old)
    pct_old = pct_old.fillna(pct_new)

    pct_new = pd.to_numeric(pct_new, errors='coerce')
    pct_old = pd.to_numeric(pct_old, errors='coerce')

    fixed_new = fixed_from(payout_new_raw, type_norm)
    fixed_old = fixed_from(payout_old_raw, type_norm)
    fixed_new = fixed_new.fillna(fixed_old)
    fixed_old = fixed_old.fillna(fixed_new)

    fixed_new = pd.to_numeric(fixed_new, errors='coerce')
    fixed_old = pd.to_numeric(fixed_old, errors='coerce')

    out = pd.DataFrame({
        "code_norm": df_sheet[code_col].apply(normalize_coupon),
        "affiliate_ID": df_sheet[aff_col].fillna("").astype(str).str.strip(),
        "type_norm": type_norm,
        "pct_new": pct_new.fillna(DEFAULT_PCT_IF_MISSING),
        "pct_old": pct_old.fillna(DEFAULT_PCT_IF_MISSING),
        "fixed_new": fixed_new,
        "fixed_old": fixed_old,
    }).dropna(subset=["code_norm"])

    return out.drop_duplicates(subset=["code_norm"], keep="last")

# =======================
# DATE WINDOW
# =======================
end_date = datetime.now().date() + timedelta(days=1)  # include 'today'
start_date = end_date - timedelta(days=days_back + 1)
today = datetime.now().date()
print(f"Current date: {today}, Start date (days_back={days_back}): {start_date}")

# =======================
# LOAD REPORT
# =======================
input_file = pick_report_path()
print(f"Using input file: {os.path.basename(input_file)}")

df_raw = pd.read_excel(input_file)

# Resolve columns (robust to small header changes)
adv_col      = get_col(df_raw, "advertiser")
date_col     = get_col(df_raw, "order date")
ftu_orders_c = get_col(df_raw, "ftu orders")
ftu_value_c  = get_col(df_raw, "ftu order values", "ftu order value", "ftu order amount")
rtu_orders_c = get_col(df_raw, "rtu orders")
rtu_value_c  = get_col(df_raw, "rtu order value", "rtu order values", "rtu order amount")
coupon_col   = get_col(df_raw, "coupon code", "coupon", "code")
country_col  = get_col(df_raw, "country")

# Filter for Noon only
df = df_raw[df_raw[adv_col].astype(str).str.strip().str.lower() == "noon"].copy()

# Date filter
df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
df = df.dropna(subset=[date_col])
df = df[(df[date_col].dt.date >= start_date) & (df[date_col].dt.date < end_date)].copy()

# =======================
# EXPAND FTU / RTU
# =======================
# FTU expansion
ftu_rep = pd.to_numeric(df[ftu_orders_c], errors='coerce').fillna(0).astype(int).clip(lower=0)
ftu = df.loc[df.index.repeat(ftu_rep)].copy()
ftu = ftu[ftu[ftu_orders_c] > 0]
ftu['sale_amount'] = (
    pd.to_numeric(ftu[ftu_value_c], errors='coerce').fillna(0.0) /
    pd.to_numeric(ftu[ftu_orders_c], errors='coerce').replace(0, pd.NA).fillna(1).astype(float)
) / 3.67
ftu['revenue'] = 4.08
ftu['order_date'] = ftu[date_col]
ftu['coupon_code'] = ftu[coupon_col]
ftu['Country'] = ftu[country_col]
ftu['customer_type'] = 'new'

# RTU expansion
rtu_rep = pd.to_numeric(df[rtu_orders_c], errors='coerce').fillna(0).astype(int).clip(lower=0)
rtu = df.loc[df.index.repeat(rtu_rep)].copy()
rtu = rtu[rtu[rtu_orders_c] > 0]
rtu['sale_amount'] = (
    pd.to_numeric(rtu[rtu_value_c], errors='coerce').fillna(0.0) /
    pd.to_numeric(rtu[rtu_orders_c], errors='coerce').replace(0, pd.NA).fillna(1).astype(float)
) / 3.67
rtu['revenue'] = 2.72
rtu['order_date'] = rtu[date_col]
rtu['coupon_code'] = rtu[coupon_col]
rtu['Country'] = rtu[country_col]
rtu['customer_type'] = 'old'

# Combine
df_expanded = pd.concat([ftu, rtu], ignore_index=True)
df_expanded['coupon_norm'] = df_expanded['coupon_code'].apply(normalize_coupon)

# =======================
# GEO MAP
# =======================
geo_mapping = {'SA': 'ksa', 'AE': 'uae', 'BH': 'bhr', 'KW': 'kwt'}
df_expanded['geo'] = df_expanded['Country'].map(geo_mapping).fillna(df_expanded['Country'])

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
for col in ['pct_new', 'pct_old']:
    df_joined[col] = pd.to_numeric(df_joined[col], errors='coerce').fillna(DEFAULT_PCT_IF_MISSING)
for col in ['fixed_new', 'fixed_old']:
    df_joined[col] = pd.to_numeric(df_joined[col], errors='coerce')

is_new_customer = df_joined['customer_type'].astype(str).str.lower().eq('new')
pct_effective = df_joined['pct_new'].where(is_new_customer, df_joined['pct_old'])
pct_effective = pd.to_numeric(pct_effective, errors='coerce').fillna(DEFAULT_PCT_IF_MISSING)
fixed_effective = df_joined['fixed_new'].where(is_new_customer, df_joined['fixed_old'])
fixed_effective = pd.to_numeric(fixed_effective, errors='coerce').fillna(0.0)

# =======================
# PAYOUT CALC
# =======================
payout = pd.Series(0.0, index=df_joined.index, dtype=float)

mask_rev = df_joined['type_norm'].str.lower().eq('revenue')
payout.loc[mask_rev] = df_joined.loc[mask_rev, 'revenue'] * pct_effective.loc[mask_rev]

mask_sale = df_joined['type_norm'].str.lower().eq('sale')
payout.loc[mask_sale] = df_joined.loc[mask_sale, 'sale_amount'] * pct_effective.loc[mask_sale]

mask_fixed = df_joined['type_norm'].str.lower().eq('fixed')
payout.loc[mask_fixed] = fixed_effective.loc[mask_fixed]

# Enforce no-match rule
payout.loc[missing_aff_mask] = 0.0
df_joined.loc[missing_aff_mask, 'affiliate_ID'] = FALLBACK_AFFILIATE_ID

df_joined['payout'] = payout.round(2)

# =======================
# BUILD OUTPUT (NEW STRUCTURE)
# =======================
output_df = pd.DataFrame({
    'offer': OFFER_ID,
    'affiliate_id': df_joined['affiliate_ID'],
    'date': pd.to_datetime(df_joined['order_date']).dt.strftime('%m-%d-%Y'),
    'status': STATUS_DEFAULT,
    'payout': df_joined['payout'],
    'revenue': df_joined['revenue'].round(2),
    'sale amount': df_joined['sale_amount'].round(2),
    'coupon': df_joined['coupon_norm'],
    'geo': df_joined['geo'],
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
