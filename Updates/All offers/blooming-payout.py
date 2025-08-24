import pandas as pd
from datetime import datetime, timedelta
import os
import re

# =======================
# CONFIG
# =======================
days_back = 30
OFFER_ID = 1106
STATUS_DEFAULT = "approved"
DEFAULT_PCT_IF_MISSING = 0.0  # fraction fallback when percent missing (0.30 == 30%)

# Local files
AFFILIATE_XLSX = "Offers Coupons.xlsx"
AFFILIATE_SHEET = "Bloomingdales"
REPORT_CSV     = "BLM _ DigiZag Report_Page 1_Table.csv"

# Currency rate & commission
AED_TO_USD = 3.67
COMMISSION_ON_SALE = 0.06  # 6%

# Country → geo mapping
COUNTRY_TO_GEO = {"AE": "uae", "KW": "kwt"}

# =======================
# PATHS
# =======================
script_dir = os.path.dirname(os.path.abspath(__file__))
input_dir = os.path.join(script_dir, '..', 'input data')
output_dir = os.path.join(script_dir, '..', 'output data')
os.makedirs(output_dir, exist_ok=True)

input_file = os.path.join(input_dir, REPORT_CSV)
affiliate_xlsx_path = os.path.join(input_dir, AFFILIATE_XLSX)
output_file = os.path.join(output_dir, 'blooming.csv')

# =======================
# HELPERS
# =======================
def normalize_coupon(x: str) -> str:
    """Uppercase, trim, and take the first token if multiple codes separated by ; , or whitespace."""
    if pd.isna(x):
        return ""
    s = str(x).strip().upper()
    parts = re.split(r"[;,\s]+", s)
    return parts[0] if parts else s

def load_affiliate_mapping_from_xlsx(xlsx_path: str, sheet_name: str) -> pd.DataFrame:
    """
    Load affiliate mapping for a given sheet and return:
      code_norm, affiliate_ID (from 'ID' or 'affiliate_ID'), type_norm,
      pct_fraction (for 'revenue'/'sale' types), fixed_amount (for 'fixed')
    """
    df_sheet = pd.read_excel(xlsx_path, sheet_name=sheet_name, dtype=str)

    # Case-insensitive column resolver
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

    # Parse payout cell: remove '%', coerce numeric
    payout_raw = (
        df_sheet[payout_col]
        .astype(str)
        .str.replace("%", "", regex=False)
        .str.strip()
    )
    payout_num = pd.to_numeric(payout_raw, errors="coerce")

    # Normalize type
    type_norm = (
        df_sheet[type_col]
        .astype(str)
        .str.strip()
        .str.lower()
        .replace({"": None})
    )

    # Percent for revenue/sale
    pct_fraction = payout_num.where(type_norm.isin(["revenue", "sale"]))
    pct_fraction = pct_fraction.apply(
        lambda v: (v / 100.0) if pd.notna(v) and v > 1 else (v if pd.notna(v) else DEFAULT_PCT_IF_MISSING)
    )

    # Fixed amount
    fixed_amount = payout_num.where(type_norm.eq("fixed"))

    out = pd.DataFrame({
        "code_norm": df_sheet[code_col].apply(normalize_coupon),
        "affiliate_ID": df_sheet[aff_col].fillna("").astype(str).str.strip(),
        "type_norm": type_norm,
        "pct_fraction": pct_fraction,
        "fixed_amount": fixed_amount
    }).dropna(subset=["code_norm"])

    # Deduplicate by code (last wins)
    out = out.drop_duplicates(subset=["code_norm"], keep="last")
    return out

# =======================
# LOAD MAIN REPORT
# =======================
print(f"Current date: {datetime.now().date()}, Start date (days_back={days_back}): {(datetime.now().date() - timedelta(days=days_back))}")

df = pd.read_csv(input_file)

# Convert 'created_date' to datetime (format like 'Aug 22, 2025')
df['created_date'] = pd.to_datetime(df['created_date'], format='%b %d, %Y', errors='coerce')
before = len(df)
df = df.dropna(subset=['created_date'])
print(f"Total rows before filtering: {before}")
print(f"Rows with invalid dates dropped: {before - len(df)}")

end_date = datetime.now().date()
start_date = end_date - timedelta(days=days_back)

# Filter by date range (inclusive of end_date)
df_filtered = df[(df['created_date'].dt.date >= start_date) & (df['created_date'].dt.date <= end_date)].copy()
print(f"Rows after filtering date range: {len(df_filtered)}")

# Geo mapping
df_filtered['geo'] = df_filtered['country'].map(COUNTRY_TO_GEO)

# USD sale amount and revenue
df_filtered['sale_amount'] = (df_filtered['AED_net_amount'] / AED_TO_USD)
df_filtered['revenue'] = df_filtered['sale_amount'] * COMMISSION_ON_SALE

# Normalize coupon for joining
df_filtered['coupon_norm'] = df_filtered['Coupon'].apply(normalize_coupon)

# =======================
# JOIN AFFILIATE MAPPING (type-aware)
# =======================
map_df = load_affiliate_mapping_from_xlsx(affiliate_xlsx_path, AFFILIATE_SHEET)
df_joined = df_filtered.merge(map_df, how="left", left_on="coupon_norm", right_on="code_norm")

# Ensure required fields exist
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

# Force payout = 0 when affiliate_id is missing/empty
mask_no_aff = (df_joined['affiliate_ID'] == "")
payout.loc[mask_no_aff] = 0.0

df_joined['payout'] = payout.round(2)

# =======================
# BUILD FINAL OUTPUT (NEW STRUCTURE)
# =======================
output_df = pd.DataFrame({
    'offer': OFFER_ID,
    'affiliate_id': df_joined['affiliate_ID'],
    'date': df_joined['created_date'].dt.strftime('%m-%d-%Y'),
    'status': STATUS_DEFAULT,
    'payout': df_joined['payout'],
    'revenue': df_joined['revenue'].round(2),
    'sale amount': df_joined['sale_amount'].round(2),
    'coupon': df_joined['coupon_norm'],
    'geo': df_joined['geo'],
})

# Save
output_df.to_csv(output_file, index=False)

print(f"Saved: {output_file}")
print(
    f"Rows: {len(output_df)} | "
    f"Coupons without affiliate_id (payout forced to 0): {int(mask_no_aff.sum())} | "
    f"Type counts -> revenue: {int(mask_rev.sum())}, sale: {int(mask_sale.sum())}, fixed: {int(mask_fixed.sum())}"
)
