import pandas as pd
from datetime import datetime, timedelta
import os
import re

# =======================
# CONFIG
# =======================
OFFER_ID = 1101
STATUS_DEFAULT = "pending"
DEFAULT_PCT_IF_MISSING = 0.0
FALLBACK_AFFILIATE_ID = "1"
AFFILIATE_XLSX  = "Offers Coupons.xlsx"
AFFILIATE_SHEET = "Sun & Sands"   # change if your tab is named differently (e.g., "SSS")
REPORT_CSV      = "SSS- Digizag Daily Report_Untitled Page_Table (5).csv"
OUTPUT_CSV      = "sun_sand.csv"

# Date window (yesterday only; excludes today)
end_date = datetime.now().date()
start_date = end_date - timedelta(days=100)
print(f"Current date: {end_date}, Start date (1 day back): {start_date}")

# =======================
# PATHS
# =======================
script_dir = os.path.dirname(os.path.abspath(__file__))
input_dir = os.path.join(script_dir, '..', 'input data')
output_dir = os.path.join(script_dir, '..', 'output data')
os.makedirs(output_dir, exist_ok=True)

input_file = os.path.join(input_dir, REPORT_CSV)
affiliate_xlsx_path = os.path.join(input_dir, AFFILIATE_XLSX)
output_file = os.path.join(output_dir, OUTPUT_CSV)

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
    Returns mapping with: code_norm, affiliate_ID, type_norm, pct_fraction, fixed_amount
    Accepts 'ID' or 'affiliate_ID'. 'payout' may be % (for revenue/sale) or fixed (for fixed).
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
    type_norm  = df_sheet[type_col].astype(str).str.strip().str.lower().replace({"": None})

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

    return out.drop_duplicates(subset=["code_norm"], keep="last")

# =======================
# LOAD REPORT
# =======================
df = pd.read_csv(input_file)

# Ensure Date datetime & filter window (exclude today)
df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
df = df.dropna(subset=['Date'])
print(f"Unique dates in dataset: {sorted(df['Date'].dt.date.unique())}")

df = df[
    (df['Date'].dt.date >= start_date) &
    (df['Date'].dt.date < end_date) &
    (df['final_status'].astype(str) != 'Returned') &
    (df['final_status'].astype(str) != 'Cancelled')
].copy()

print(f"Filtered rows: {len(df)}")
print(f"Filtered dates: {sorted(df['Date'].dt.date.unique())}")

# =======================
# DERIVED FIELDS
# =======================
# Convert sale amount to USD (AED -> USD)
df['sale_amount'] = pd.to_numeric(df['net_sales'], errors='coerce').fillna(0.0) / 3.67

# Revenue: 6% flat
df['revenue'] = df['sale_amount'] * 0.06

# Normalize coupon for join
df['coupon_norm'] = df['coupon_code'].apply(normalize_coupon)

# =======================
# JOIN AFFILIATE MAPPING (type-aware)
# =======================
map_df = load_affiliate_mapping_from_xlsx(affiliate_xlsx_path, AFFILIATE_SHEET)
df_joined = df.merge(map_df, how="left", left_on="coupon_norm", right_on="code_norm")

# Missing affiliate?
missing_aff_mask = df_joined['affiliate_ID'].isna() | (df_joined['affiliate_ID'].astype(str).str.strip() == "")

# Normalize mapping fields
df_joined['affiliate_ID'] = df_joined['affiliate_ID'].fillna("").astype(str).str.strip()
df_joined['type_norm'] = df_joined['type_norm'].fillna("revenue")
df_joined['pct_fraction'] = df_joined['pct_fraction'].fillna(DEFAULT_PCT_IF_MISSING)

# =======================
# PAYOUT CALC
# =======================
payout = pd.Series(0.0, index=df_joined.index)

mask_rev = df_joined['type_norm'].str.lower().eq('revenue')
payout.loc[mask_rev] = df_joined.loc[mask_rev, 'revenue'] * df_joined.loc[mask_rev, 'pct_fraction']

mask_sale = df_joined['type_norm'].str.lower().eq('sale')
payout.loc[mask_sale] = df_joined.loc[mask_sale, 'sale_amount'] * df_joined.loc[mask_sale, 'pct_fraction']

mask_fixed = df_joined['type_norm'].str.lower().eq('fixed')
payout.loc[mask_fixed] = df_joined.loc[mask_fixed, 'fixed_amount'].fillna(0.0)

# Enforce: if no affiliate match, set affiliate_id="1", payout=0
payout.loc[missing_aff_mask] = 0.0
df_joined.loc[missing_aff_mask, 'affiliate_ID'] = FALLBACK_AFFILIATE_ID

df_joined['payout'] = payout.round(2)

# Optional: User type sort (New before Repeat) if you still want that behavior downstream
df_joined['user_tag_rank'] = df_joined.get('user_tag', pd.Series(index=df_joined.index)).map({'New': 0, 'Repeat': 1})
df_joined = df_joined.sort_values(by=['user_tag_rank'], na_position='last')

# =======================
# BUILD OUTPUT (NEW STRUCTURE)
# =======================
output_df = pd.DataFrame({
    'offer': OFFER_ID,
    'affiliate_id': df_joined['affiliate_ID'],
    'date': df_joined['Date'].dt.strftime('%m-%d-%Y'),
    'status': STATUS_DEFAULT,
    'payout': df_joined['payout'],
    'revenue': df_joined['revenue'].round(2),
    'sale amount': df_joined['sale_amount'].round(2),
    'coupon': df_joined['coupon_norm'],
    'geo': df_joined['Store'],
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
print(f"Date range processed: {output_df['date'].min() if not output_df.empty else 'N/A'} to {output_df['date'].max() if not output_df.empty else 'N/A'}")
