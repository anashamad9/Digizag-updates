import pandas as pd
from datetime import datetime, timedelta
import os
import re
from helpers import *

# =======================
# CONFIG
# =======================
days_back = 50
OFFER_ID = 1283
GEO = "ksa"
STATUS_DEFAULT = "pending"
DEFAULT_PCT_IF_MISSING = 0.0  # fraction fallback when percent missing (0.30 == 30%)

# Local files
AFFILIATE_XLSX = "Offers Coupons.xlsx"   # multi-sheet Excel you uploaded
REPORT_PREFIX  = "Individual-Item-Report"  # any CSV starting with this will match

# Offer -> worksheet name mapping
OFFER_SHEET_BY_ID = {
    1283: "Adidas",
    # add others later if needed
}

# =======================
# PATHS
# =======================
script_dir = os.path.dirname(os.path.abspath(__file__))
input_dir = os.path.join(script_dir, '..', 'input data')
output_dir = os.path.join(script_dir, '..', 'output data')
os.makedirs(output_dir, exist_ok=True)

affiliate_xlsx_path = os.path.join(input_dir, AFFILIATE_XLSX)
output_file = os.path.join(output_dir, 'adidasssss_with_payout.csv')

# Find the changing-named report file dynamically
input_file = find_matching_csv(input_dir, REPORT_PREFIX)

# =======================
# LOAD MAIN REPORT
# =======================
df = pd.read_csv(input_file, skiprows=range(4))

# Convert 'Transaction Date' to datetime, drop NaT
df['Transaction Date'] = pd.to_datetime(df['Transaction Date'], format='%m/%d/%y', errors='coerce')
df = df.dropna(subset=['Transaction Date'])

end_date = datetime.now().date()
start_date = end_date - timedelta(days=days_back)
today = datetime.now().date()

# Filter for Adidas KSA within range, excluding current day
df_filtered = df[
    (df['Advertiser Name'] == 'Adidas KSA') &
    (df['Transaction Date'].dt.date >= start_date) &
    (df['Transaction Date'].dt.date < today)
]

# Split rows with # of Items > 1 into per-item rows
split_rows = []
for _, row in df_filtered.iterrows():
    items = int(row['# of Items']) if pd.notnull(row['# of Items']) else 1 # If null assign 'items' with 1
    total_sales = float(row['Sales']) if pd.notnull(row['Sales']) else 0.0 # If null assign 'sales' with 0
    sales_per_item = (total_sales / items) if items > 0 else 0.0 # TODO: redundant ternary operation ('items' is inherently > 0)
    for _ in range(items):
        split_rows.append({
            'Order Coupon Code(s)': row.get('Order Coupon Code(s)', ''),
            'Transaction Date': row['Transaction Date'],
            'Sales': sales_per_item,
            '# of Items': 1
        })

df_split = pd.DataFrame(split_rows)

# Compute sale_amount and revenue
df_split['sale_amount'] = df_split['Sales'] * 1.17
df_split['revenue'] = df_split['sale_amount'] * 0.07

# Normalize coupon for joining
df_split['coupon_norm'] = df_split['Order Coupon Code(s)'].apply(normalize_coupon)

# =======================
# JOIN AFFILIATE MAPPING (type-aware)
# =======================
map_df = load_affiliate_mapping_from_xlsx(affiliate_xlsx_path, OFFER_ID)
df_joined = df_split.merge(map_df, how="left", left_on="coupon_norm", right_on="code_norm")

# Ensure required fields exist and derive effective payouts
df_joined['affiliate_ID'] = df_joined['affiliate_ID'].fillna("").astype(str).str.strip()
df_joined['type_norm'] = df_joined['type_norm'].fillna("revenue")
for col in ['pct_new', 'pct_old']:
    df_joined[col] = pd.to_numeric(df_joined.get(col), errors='coerce').fillna(DEFAULT_PCT_IF_MISSING)
for col in ['fixed_new', 'fixed_old']:
    df_joined[col] = pd.to_numeric(df_joined.get(col), errors='coerce')
is_new_customer = infer_is_new_customer(df_joined)
pct_effective = df_joined['pct_new'].where(is_new_customer, df_joined['pct_old'])
df_joined['pct_fraction'] = pd.to_numeric(pct_effective, errors='coerce').fillna(DEFAULT_PCT_IF_MISSING)
fixed_effective = df_joined['fixed_new'].where(is_new_customer, df_joined['fixed_old'])
df_joined['fixed_amount'] = pd.to_numeric(fixed_effective, errors='coerce')

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
    'date': df_joined['Transaction Date'].dt.strftime('%m-%d-%Y'),
    'status': STATUS_DEFAULT,
    'payout': df_joined['payout'],
    'revenue': df_joined['revenue'].round(2),
    'sale amount': df_joined['sale_amount'].round(2),
    'coupon': df_joined['coupon_norm'],
    'geo': GEO,
})

# Save
output_df.to_csv(output_file, index=False)

print(f"Using report file: {input_file}")
print(f"Saved: {output_file}")
print(
    f"Rows: {len(output_df)} | "
    f"Coupons without affiliate_id (payout forced to 0): {int(mask_no_aff.sum())} | "
    f"Type counts -> revenue: {int(mask_rev.sum())}, sale: {int(mask_sale.sum())}, fixed: {int(mask_fixed.sum())}"
)