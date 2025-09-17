import pandas as pd
from datetime import datetime, timedelta
import os
import re

# =======================
# CONFIG
# =======================
days_back = 4
OFFER_ID = 1189
STATUS_DEFAULT = "pending"
DEFAULT_PCT_IF_MISSING = 0.0
FALLBACK_AFFILIATE_ID = "1"

# Files
AFFILIATE_XLSX  = "Offers Coupons.xlsx"
AFFILIATE_SHEET = "Namshi"

# =======================
# PATHS
# =======================
script_dir = os.path.dirname(os.path.abspath(__file__))
input_dir = os.path.join(script_dir, '..', 'input data')
output_dir = os.path.join(script_dir, '..', 'output data')
os.makedirs(output_dir, exist_ok=True)

affiliate_xlsx_path = os.path.join(input_dir, AFFILIATE_XLSX)
output_file = os.path.join(output_dir, 'namshi.csv')

# =======================
# HELPERS
# =======================
def find_latest_sales_xlsx(directory: str) -> str:
    """
    Pick the most recently modified Excel file whose name matches:
      - sales.xlsx
      - sales (N).xlsx
      - or generally starts with 'sales' (case-insensitive)
    """
    # strict pattern first: sales or sales (number).xlsx
    strict = re.compile(r"^sales(?:\s*\(\d+\))?\.xlsx$", re.IGNORECASE)
    strict_matches = [
        f for f in os.listdir(directory)
        if strict.match(f) and f.lower().endswith(".xlsx")
    ]
    if strict_matches:
        return os.path.join(directory, max(strict_matches, key=lambda f: os.path.getmtime(os.path.join(directory, f))))

    # fallback: any .xlsx that starts with "sales"
    fallback = [
        f for f in os.listdir(directory)
        if f.lower().endswith(".xlsx") and os.path.splitext(f)[0].lower().startswith("sales")
    ]
    if fallback:
        return os.path.join(directory, max(fallback, key=lambda f: os.path.getmtime(os.path.join(directory, f))))

    raise FileNotFoundError("No 'sales*.xlsx' file found in the input data folder.")

def normalize_coupon(x: str) -> str:
    if pd.isna(x):
        return ""
    s = str(x).strip().upper()
    parts = re.split(r"[;,\s]+", s)
    return parts[0] if parts else s

def load_affiliate_mapping_from_xlsx(xlsx_path: str, sheet_name: str) -> pd.DataFrame:
    """
    Returns mapping with: code_norm, affiliate_ID, type_norm, pct_fraction, fixed_amount
    - Accepts 'ID' or 'affiliate_ID'
    - Accepts payout in % (for revenue/sale) or fixed numbers (for fixed)
    """
    df_sheet = pd.read_excel(xlsx_path, sheet_name=sheet_name, dtype=str)
    cols_lower = {c.lower().strip(): c for c in df_sheet.columns}

    code_col = cols_lower.get("code")
    aff_col  = cols_lower.get("id") or cols_lower.get("affiliate_id")
    type_col = cols_lower.get("type")
    payout_col = (cols_lower.get("payout")
                  or cols_lower.get("new customer payout")
                  or cols_lower.get("old customer payout"))

    if not code_col:
        raise ValueError(f"[{sheet_name}] must contain a 'Code' column.")
    if not aff_col:
        raise ValueError(f"[{sheet_name}] must contain an 'ID' (or 'affiliate_ID') column.")
    if not type_col:
        raise ValueError(f"[{sheet_name}] must contain a 'type' column (revenue/sale/fixed).")
    if not payout_col:
        raise ValueError(f"[{sheet_name}] must contain a payout column (e.g., 'payout').")

    payout_raw = df_sheet[payout_col].astype(str).str.replace("%", "", regex=False).str.strip()
    payout_num = pd.to_numeric(payout_raw, errors="coerce")

    type_norm = df_sheet[type_col].astype(str).str.strip().str.lower().replace({"": None})

    # Percent for revenue/sale
    pct_fraction = payout_num.where(type_norm.isin(["revenue", "sale"])).apply(
        lambda v: (v / 100.0) if pd.notna(v) and v > 1 else (v if pd.notna(v) else DEFAULT_PCT_IF_MISSING)
    )
    # Fixed for 'fixed'
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
# LOAD REPORT
# =======================
today = datetime.now().date()
# window includes "today" by making end_date = today + 1 and filtering < end_date
end_date = today + timedelta(days=1)
start_date = end_date - timedelta(days=days_back + 1)

print(f"Current date: {today}, Start date (days_back={days_back}): {start_date}")

input_file = find_latest_sales_xlsx(input_dir)
print(f"Using input file: {os.path.basename(input_file)}")

df = pd.read_excel(input_file)

# Filter for Namshi only (robust to stray spaces/case)
df = df[df['Advertiser'].astype(str).str.strip().str.casefold() == 'namshi'].copy()

# Date filter: include dates >= start_date and < end_date
df['Order Date'] = pd.to_datetime(df['Order Date'], errors='coerce')
df = df.dropna(subset=['Order Date'])
df = df[(df['Order Date'].dt.date >= start_date) & (df['Order Date'].dt.date < end_date)].copy()

# =======================
# EXPAND FTU / RTU
# =======================
# FTU
ftu_repeat_idx = pd.to_numeric(df['FTU Orders'], errors='coerce').fillna(0).astype(int)
ftu = df.loc[df.index.repeat(ftu_repeat_idx)].copy()
ftu = ftu[ftu['FTU Orders'] > 0]
ftu['sale_amount'] = (
    pd.to_numeric(ftu['FTU Order Values'], errors='coerce').fillna(0.0) /
    pd.to_numeric(ftu['FTU Orders'], errors='coerce').replace(0, pd.NA).fillna(1).astype(float)
) / 3.67
ftu['revenue'] = ftu['sale_amount'] * 0.08
ftu['order_date'] = ftu['Order Date']
ftu['coupon_code'] = ftu['Coupon Code']
ftu['Country'] = ftu['Country']

# RTU
rtu_repeat_idx = pd.to_numeric(df['RTU Orders'], errors='coerce').fillna(0).astype(int)
rtu = df.loc[df.index.repeat(rtu_repeat_idx)].copy()
rtu = rtu[rtu['RTU Orders'] > 0]
rtu['sale_amount'] = (
    pd.to_numeric(rtu['RTU Order Value'], errors='coerce').fillna(0.0) /
    pd.to_numeric(rtu['RTU Orders'], errors='coerce').replace(0, pd.NA).fillna(1).astype(float)
) / 3.67
rtu['revenue'] = rtu['sale_amount'] * 0.025
rtu['order_date'] = rtu['Order Date']
rtu['coupon_code'] = rtu['Coupon Code']
rtu['Country'] = rtu['Country']

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
df_joined['affiliate_ID']  = df_joined['affiliate_ID'].fillna("").astype(str).str.strip()
df_joined['type_norm']     = df_joined['type_norm'].fillna("revenue")
df_joined['pct_fraction']  = df_joined['pct_fraction'].fillna(DEFAULT_PCT_IF_MISSING)

# =======================
# PAYOUT CALC
# =======================
payout = pd.Series(0.0, index=df_joined.index)

mask_rev   = df_joined['type_norm'].str.lower().eq('revenue')
mask_sale  = df_joined['type_norm'].str.lower().eq('sale')
mask_fixed = df_joined['type_norm'].str.lower().eq('fixed')

payout.loc[mask_rev]   = df_joined.loc[mask_rev, 'revenue']     * df_joined.loc[mask_rev, 'pct_fraction']
payout.loc[mask_sale]  = df_joined.loc[mask_sale, 'sale_amount'] * df_joined.loc[mask_sale, 'pct_fraction']
payout.loc[mask_fixed] = df_joined.loc[mask_fixed, 'fixed_amount'].fillna(0.0)

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
