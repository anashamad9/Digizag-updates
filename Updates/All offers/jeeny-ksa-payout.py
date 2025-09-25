import pandas as pd
from datetime import datetime, timedelta
import os
import re

# =======================
# CONFIG
# =======================
days_back = 5
OFFER_ID = 1260
STATUS_DEFAULT = "pending"          # always pending
DEFAULT_PCT_IF_MISSING = 0.0        # fallback fraction if percent missing
FALLBACK_AFFILIATE_ID = "1"         # when no affiliate match: set "1" and payout = 0
GEO = "ksa"

# Local files
AFFILIATE_XLSX  = "Offers Coupons.xlsx"    # multi-sheet Excel
AFFILIATE_SHEET = "Jeeny Ksa"              # coupons sheet for this offer
REPORT_PREFIX   = "ksa-digizag-report-"
REPORT_EXT      = ".csv"

# =======================
# PATHS
# =======================
script_dir = os.path.dirname(os.path.abspath(__file__))
input_dir = os.path.join(script_dir, '..', 'input data')
output_dir = os.path.join(script_dir, '..', 'output data')
os.makedirs(output_dir, exist_ok=True)

# =======================
# HELPERS
# =======================
def normalize_coupon(x: str) -> str:
    """Uppercase, trim, take the first token if multiple codes separated by ; , or whitespace."""
    if pd.isna(x):
        return ""
    s = str(x).strip().upper()
    parts = re.split(r"[;,\s]+", s)
    return parts[0] if parts else s

def extract_date(filename):
    """From 'ksa-digizag-report-YYYY-MM-DD.csv' return a datetime for sorting."""
    m = re.search(r'ksa-digizag-report-(\d{4}-\d{2}-\d{2})', filename)
    if m:
        try:
            return datetime.strptime(m.group(1), '%Y-%m-%d')
        except Exception:
            return datetime.min
    return datetime.min

def parse_date_column(series: pd.Series) -> pd.Series:
    """
    Robust date parsing:
    1) Try %d-%b-%Y (e.g., 21-Sep-2025)
    2) Fill remaining with %d-%b-%y (e.g., 21-Sep-25)
    3) Fill remaining with pandas inference (dayfirst)
    """
    s = pd.to_datetime(series, format='%d-%b-%Y', errors='coerce')
    need2 = s.isna()
    if need2.any():
        s2 = pd.to_datetime(series[need2], format='%d-%b-%y', errors='coerce')
        s.loc[need2] = s2
    need3 = s.isna()
    if need3.any():
        s3 = pd.to_datetime(series[need3], dayfirst=True, errors='coerce')
        s.loc[need3] = s3
    return s

def load_affiliate_mapping_from_xlsx(xlsx_path: str, sheet_name: str) -> pd.DataFrame:
    """
    Return mapping with columns:
      code_norm, affiliate_ID (from 'ID' or 'affiliate_ID'), type_norm,
      pct_fraction (for 'revenue'/'sale'), fixed_amount (for 'fixed')
    """
    df_sheet = pd.read_excel(xlsx_path, sheet_name=sheet_name, dtype=str)
    cols_lower = {c.lower().strip(): c for c in df_sheet.columns}

    code_col = cols_lower.get("code")
    aff_col  = cols_lower.get("id") or cols_lower.get("affiliate_id")  # accept 'ID' or 'affiliate_ID'
    type_col = cols_lower.get("type")
    payout_col = (cols_lower.get("payout")
                  or cols_lower.get("new customer payout")
                  or cols_lower.get("old customer payout"))

    if not code_col:
        raise ValueError(f"[{sheet_name}] must contain a 'Code' column.")
    if not aff_col:
        raise ValueError(f"[{sheet_name}] must contain an 'ID' (or 'affiliate_ID') column.")
    if not type_col:
        raise ValueError(f"[{sheet_name}] must contain a 'type' column with values 'revenue'/'sale'/'fixed'.")
    if not payout_col:
        raise ValueError(f"[{sheet_name}] must contain a payout column (e.g., 'payout').")

    payout_raw = df_sheet[payout_col].astype(str).str.replace("%", "", regex=False).str.strip()
    payout_num = pd.to_numeric(payout_raw, errors="coerce")

    type_norm = df_sheet[type_col].astype(str).str.strip().str.lower().replace({"": None})

    # % for revenue/sale
    pct_fraction = payout_num.where(type_norm.isin(["revenue", "sale"])).apply(
        lambda v: (v / 100.0) if pd.notna(v) and v > 1 else (v if pd.notna(v) else DEFAULT_PCT_IF_MISSING)
    )
    # fixed amount
    fixed_amount = payout_num.where(type_norm.eq("fixed"))

    out = pd.DataFrame({
        "code_norm": df_sheet[code_col].apply(normalize_coupon),
        "affiliate_ID": df_sheet[aff_col].fillna("").astype(str).str.strip(),
        "type_norm": type_norm,
        "pct_fraction": pct_fraction,
        "fixed_amount": fixed_amount
    }).dropna(subset=["code_norm"])

    # last occurrence wins for duplicate codes
    return out.drop_duplicates(subset=["code_norm"], keep="last")

# =======================
# LOAD LATEST REPORT
# =======================
end_date = datetime.now().date()
start_date = end_date - timedelta(days=days_back)
today = datetime.now().date()
print(f"Current date: {today}, Start date (days_back={days_back}): {start_date}")

ksa_files = [f for f in os.listdir(input_dir) if f.startswith(REPORT_PREFIX) and f.endswith(REPORT_EXT)]
if not ksa_files:
    raise FileNotFoundError(f"No files starting with '{REPORT_PREFIX}' found in the input directory.")

latest_file = max(ksa_files, key=extract_date)
input_file = os.path.join(input_dir, latest_file)
print(f"Using input file: {latest_file}")

df = pd.read_csv(input_file)

# =======================
# CLEAN & EXPAND
# =======================
# 1) Robust date parsing (fix for 2-digit year like `21-Sep-25`)
if 'Date' not in df.columns:
    raise ValueError("CSV is missing required 'Date' column.")
df['Date'] = parse_date_column(df['Date'])

# Drop rows that failed to parse
df = df.dropna(subset=['Date'])

if df.empty:
    raise ValueError("All dates failed to parse—check CSV date format (e.g., %d-%b-%y vs %d-%b-%Y).")

# Exclude the current day
df = df[df['Date'].dt.date < today]

# Optional window (keep last N days inclusive of end_date)
df = df[(df['Date'].dt.date >= start_date) & (df['Date'].dt.date <= end_date)]

if df.empty:
    print("No rows within the requested date window (after excluding today).")

# Ensure required columns
if 'Usage' not in df.columns:
    print("WARNING: 'Usage' column missing; defaulting each row to 1.")
    df['Usage'] = 1

if 'coupon' not in df.columns:
    raise ValueError("CSV is missing required 'coupon' column.")

# Expand rows by Usage count
usage = pd.to_numeric(df['Usage'], errors='coerce').fillna(0).astype(int).clip(lower=0)
if (usage <= 0).all():
    print("WARNING: All 'Usage' values are 0/invalid; expansion will produce 0 rows.")
df_expanded = df.loc[df.index.repeat(usage)].reset_index(drop=True)

if df_expanded.empty:
    print("Expanded data is empty after applying 'Usage'. The output CSV will be empty.")

# Derive columns
if not df_expanded.empty:
    df_expanded['date_str'] = df_expanded['Date'].dt.strftime('%m-%d-%Y')
else:
    df_expanded['date_str'] = pd.Series(dtype=str)

df_expanded['sale_amount'] = 0.0
df_expanded['revenue'] = 2.0
df_expanded['coupon_norm'] = df_expanded['coupon'].apply(normalize_coupon) if not df_expanded.empty else pd.Series(dtype=str)

# =======================
# JOIN AFFILIATE MAPPING (type-aware)
# =======================
affiliate_xlsx_path = os.path.join(input_dir, AFFILIATE_XLSX)
map_df = load_affiliate_mapping_from_xlsx(affiliate_xlsx_path, AFFILIATE_SHEET)

df_joined = df_expanded.merge(map_df, how="left", left_on="coupon_norm", right_on="code_norm")

# Missing affiliate?
missing_aff_mask = df_joined['affiliate_ID'].isna() | (df_joined['affiliate_ID'].astype(str).str.strip() == "")

# Normalize fields
df_joined['affiliate_ID'] = df_joined['affiliate_ID'].fillna("").astype(str).str.strip()
df_joined['type_norm'] = df_joined['type_norm'].fillna("revenue")
df_joined['pct_fraction'] = df_joined['pct_fraction'].fillna(DEFAULT_PCT_IF_MISSING)

# =======================
# COMPUTE PAYOUT (by type)
# =======================
payout = pd.Series(0.0, index=df_joined.index)

mask_rev = df_joined['type_norm'].str.lower().eq('revenue')
payout.loc[mask_rev] = df_joined.loc[mask_rev, 'revenue'] * df_joined.loc[mask_rev, 'pct_fraction']

mask_sale = df_joined['type_norm'].str.lower().eq('sale')
payout.loc[mask_sale] = df_joined.loc[mask_sale, 'sale_amount'] * df_joined.loc[mask_sale, 'pct_fraction']  # = 0

mask_fixed = df_joined['type_norm'].str.lower().eq('fixed')
payout.loc[mask_fixed] = df_joined.loc[mask_fixed, 'fixed_amount'].fillna(0.0)

# Enforce: if no affiliate match, set affiliate_id="1", payout=0
payout.loc[missing_aff_mask] = 0.0
df_joined.loc[missing_aff_mask, 'affiliate_ID'] = FALLBACK_AFFILIATE_ID

df_joined['payout'] = payout.round(2)

# =======================
# BUILD FINAL OUTPUT
# =======================
output_df = pd.DataFrame({
    'offer': OFFER_ID,
    'affiliate_id': df_joined['affiliate_ID'],
    'date': df_joined['date_str'],
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
output_file = os.path.join(output_dir, 'jeeny_ksa.csv')
output_df.to_csv(output_file, index=False)

print(f"Saved: {output_file}")
print(
    f"Rows: {len(output_df)} | "
    f"Coupons with no affiliate (set aff={FALLBACK_AFFILIATE_ID}, payout=0): {int(missing_aff_mask.sum())} | "
    f"Type counts -> revenue: {int(mask_rev.sum())}, sale: {int(mask_sale.sum())}, fixed: {int(mask_fixed.sum())}"
)
rng_min = output_df['date'].min() if not output_df.empty else 'N/A'
rng_max = output_df['date'].max() if not output_df.empty else 'N/A'
print(f"Date range processed: {rng_min} to {rng_max}")
