import pandas as pd
from datetime import datetime, timedelta
import os
import re

# =======================
# CONFIG
# =======================
days_back = 80
OFFER_ID = 1279
STATUS_DEFAULT = "pending"          # always "pending"
DEFAULT_PCT_IF_MISSING = 0.0        # fallback fraction when percent missing (0.30 == 30%)
FALLBACK_AFFILIATE_ID = "1"         # when coupon has no affiliate, use "1" and payout=0

# Local files
AFFILIATE_XLSX  = "Offers Coupons.xlsx"    # multi-sheet Excel
AFFILIATE_SHEET = "Cartlow"                # <-- change if your sheet is differently named

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

def extract_timestamp(filename: str):
    """
    From filename like:
    digizag_2025-YYYY-MM-DDTHH_MM_SS.ssssssZ.csv
    return a datetime for sorting. If no match, return min datetime.
    """
    m = re.search(r'digizag_2025-\d{4}-\d{2}-\d{2}T\d{2}_\d{2}_\d{2}\.\d{6}Z', filename)
    if not m:
        return datetime.min
    stamp = m.group(0).replace('digizag_', '').replace('T', ' ')
    # convert HH_MM_SS to HH:MM:SS
    date_part, time_part = stamp.split(' ')
    time_part = time_part.replace('_', ':')
    # strip trailing Z
    if time_part.endswith('Z'):
        time_part = time_part[:-1]
    try:
        return datetime.strptime(f"{date_part} {time_part}", "%Y-%m-%d %H:%M:%S.%f")
    except Exception:
        return datetime.min

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

    payout_raw = (
        df_sheet[payout_col]
        .astype(str)
        .str.replace("%", "", regex=False)
        .str.strip()
    )
    payout_num = pd.to_numeric(payout_raw, errors="coerce")

    type_norm = (
        df_sheet[type_col]
        .astype(str)
        .str.strip()
        .str.lower()
        .replace({"": None})
    )

    # % for revenue/sale
    pct_fraction = payout_num.where(type_norm.isin(["revenue", "sale"]))
    pct_fraction = pct_fraction.apply(
        lambda v: (v / 100.0) if pd.notna(v) and v > 1 else (v if pd.notna(v) else DEFAULT_PCT_IF_MISSING)
    )
    # fixed
    fixed_amount = payout_num.where(type_norm.eq("fixed"))

    out = pd.DataFrame({
        "code_norm": df_sheet[code_col].apply(normalize_coupon),
        "affiliate_ID": df_sheet[aff_col].fillna("").astype(str).str.strip(),
        "type_norm": type_norm,
        "pct_fraction": pct_fraction,
        "fixed_amount": fixed_amount
    }).dropna(subset=["code_norm"])

    return out.drop_duplicates(subset=["code_norm"], keep="last")

# =======================
# LOAD LATEST INPUT FILE
# =======================
end_date = datetime.now().date()
start_date = end_date - timedelta(days=days_back)
today = datetime.now().date()
print(f"Current date: {today}, Start date (days_back={days_back}): {start_date}")

digizag_files = [f for f in os.listdir(input_dir) if f.startswith('digizag_2025-') and f.endswith('.csv')]
if not digizag_files:
    raise FileNotFoundError("No files starting with 'digizag_2025-' found in the input directory.")

latest_file = max(digizag_files, key=extract_timestamp)
input_file = os.path.join(input_dir, latest_file)
print(f"Using input file: {latest_file}")

df = pd.read_csv(input_file)

# =======================
# CLEAN & FILTER
# =======================
# Parse Order Date
df['Order Date'] = pd.to_datetime(df['Order Date'], errors='coerce')
df = df.dropna(subset=['Order Date'])

# Filter: last N days, not Cancelled, exclude today
df_filtered = df[
    (df['Order Date'].dt.date >= start_date) &
    (~df['Order Status'].astype(str).str.contains('Cancelled', case=False, na=False)) &
    (df['Order Date'].dt.date < today)
].copy()

# =======================
# DERIVED FIELDS (sale_amount & revenue)
# =======================
def convert_amount(row, colname):
    currency = str(row.get('Currency', '')).upper()
    value = row.get(colname)
    if pd.isna(value):
        return 0.0
    try:
        v = float(value)
    except Exception:
        return 0.0
    if currency == 'SAR':
        return v / 3.75
    elif currency == 'AED':
        return v / 3.67
    else:
        # default to AED rate if unrecognized
        return v / 3.67

df_filtered['sale_amount'] = df_filtered.apply(lambda r: convert_amount(r, 'Sale Amount'), axis=1)
df_filtered['revenue'] = df_filtered.apply(lambda r: convert_amount(r, 'Payout'), axis=1)

# GEO mapping
geo_mapping = {'UAE': 'uae', 'KSA': 'ksa'}
df_filtered['geo'] = df_filtered['Geo'].map(geo_mapping).fillna('no-geo')

# Normalize coupon for joining
df_filtered['coupon_norm'] = df_filtered['Coupon Code'].apply(normalize_coupon)

# =======================
# JOIN AFFILIATE MAPPING (type-aware)
# =======================
affiliate_xlsx_path = os.path.join(input_dir, AFFILIATE_XLSX)
map_df = load_affiliate_mapping_from_xlsx(affiliate_xlsx_path, AFFILIATE_SHEET)

df_joined = df_filtered.merge(map_df, how="left", left_on="coupon_norm", right_on="code_norm")

# Missing affiliate?
missing_aff_mask = df_joined['affiliate_ID'].isna() | (df_joined['affiliate_ID'].astype(str).str.strip() == "")

# Normalize fields
df_joined['affiliate_ID'] = df_joined['affiliate_ID'].fillna("").astype(str).str.strip()
df_joined['type_norm'] = df_joined['type_norm'].fillna("revenue")
df_joined['pct_fraction'] = df_joined['pct_fraction'].fillna(DEFAULT_PCT_IF_MISSING)

# =======================
# COMPUTE PAYOUT BASED ON TYPE
# =======================
payout = pd.Series(0.0, index=df_joined.index)

# revenue-based %
mask_rev = df_joined['type_norm'].str.lower().eq('revenue')
payout.loc[mask_rev] = (df_joined.loc[mask_rev, 'revenue'] * df_joined.loc[mask_rev, 'pct_fraction'])

# sale-based %
mask_sale = df_joined['type_norm'].str.lower().eq('sale')
payout.loc[mask_sale] = (df_joined.loc[mask_sale, 'sale_amount'] * df_joined.loc[mask_sale, 'pct_fraction'])

# fixed amount
mask_fixed = df_joined['type_norm'].str.lower().eq('fixed')
payout.loc[mask_fixed] = df_joined.loc[mask_fixed, 'fixed_amount'].fillna(0.0)

# Enforce rule: if no affiliate match, set affiliate_id="1", payout=0
payout.loc[missing_aff_mask] = 0.0
df_joined.loc[missing_aff_mask, 'affiliate_ID'] = FALLBACK_AFFILIATE_ID

df_joined['payout'] = payout.round(2)

# =======================
# BUILD FINAL OUTPUT (NEW STRUCTURE)
# =======================
output_df = pd.DataFrame({
    'offer': OFFER_ID,
    'affiliate_id': df_joined['affiliate_ID'],
    'date': df_joined['Order Date'].dt.strftime('%m-%d-%Y'),
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
output_file = os.path.join(output_dir, 'Cartlow.csv')
output_df.to_csv(output_file, index=False)

print(f"Saved: {output_file}")
print(
    f"Rows: {len(output_df)} | "
    f"Coupons with no affiliate (set aff={FALLBACK_AFFILIATE_ID}, payout=0): {int(missing_aff_mask.sum())} | "
    f"Type counts -> revenue: {int(mask_rev.sum())}, sale: {int(mask_sale.sum())}, fixed: {int(mask_fixed.sum())}"
)
print(f"Date range processed: {output_df['date'].min() if not output_df.empty else 'N/A'} to {output_df['date'].max() if not output_df.empty else 'N/A'}")
