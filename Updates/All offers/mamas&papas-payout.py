import pandas as pd
from datetime import datetime, timedelta
import os
import re

# =======================
# CONFIG
# =======================
days_back = 14
OFFER_ID = 1107
STATUS_DEFAULT = "pending"          # always "pending"
DEFAULT_PCT_IF_MISSING = 0.0        # fallback fraction for % values (0.30 == 30%)
FALLBACK_AFFILIATE_ID = "1"         # when no affiliate match: set to "1" and payout=0

# Local files
AFFILIATE_XLSX  = "Offers Coupons.xlsx"
AFFILIATE_SHEET = "Mamas&Papas"     # coupons sheet name for this offer
REPORT_CSV      = "MNP _ DigiZag Report_Page 1_Table (4).csv"

# Country → geo mapping
COUNTRY_GEO = {"SA": "ksa", "AE": "uae", "KW": "kwt"}

# =======================
# PATHS
# =======================
script_dir = os.path.dirname(os.path.abspath(__file__))
input_dir = os.path.join(script_dir, '..', 'input data')
output_dir = os.path.join(script_dir, '..', 'output data')
os.makedirs(output_dir, exist_ok=True)

input_file = os.path.join(input_dir, REPORT_CSV)
affiliate_xlsx_path = os.path.join(input_dir, AFFILIATE_XLSX)
output_file = os.path.join(output_dir, 'mamas_papas.csv')

# =======================
# HELPERS
# =======================
def normalize_coupon(x: str) -> str:
    """Uppercase, trim, first token if multiple codes separated by ; , or whitespace."""
    if pd.isna(x):
        return ""
    s = str(x).replace("\u00A0", " ").strip().upper()  # NBSP -> space
    parts = re.split(r"[;,\s]+", s)
    return parts[0] if parts else s

def load_affiliate_mapping_from_xlsx(xlsx_path: str, sheet_name: str) -> pd.DataFrame:
    """
    Returns mapping with: code_norm, affiliate_ID, type_norm, pct_fraction, fixed_amount
    - Robust to non-string headers (casts to str)
    - Flexible header matching (accepts common variants)
    """
    df_sheet = pd.read_excel(xlsx_path, sheet_name=sheet_name, dtype=str)

    # Safe lookup: {lower_stripped: original_name}
    cols_lower = {}
    for c in df_sheet.columns:
        key = str(c).strip().lower()
        if key and key != "nan":
            cols_lower[key] = c

    def pick(*candidates):
        for cand in candidates:
            if cand in cols_lower:
                return cols_lower[cand]
        return None

    code_col = pick("code", "coupon", "coupon code", "coupon_code")
    aff_col  = pick("id", "affiliate_id", "affiliate id")
    type_col = pick("type", "payout type", "commission type")
    payout_col = pick("payout", "new customer payout", "old customer payout", "commission", "rate")

    if not code_col:
        raise ValueError(f"[{sheet_name}] must contain a 'Code' column (e.g., Code / Coupon Code).")
    if not aff_col:
        raise ValueError(f"[{sheet_name}] must contain an affiliate id column (ID / affiliate_ID).")
    if not type_col:
        raise ValueError(f"[{sheet_name}] must contain a 'type' column with values revenue/sale/fixed.")
    if not payout_col:
        raise ValueError(f"[{sheet_name}] must contain a payout column (payout / new customer payout / old customer payout).")

    # Parse payout numbers (supports 73, 73%, 0.73)
    payout_raw = (
        df_sheet[payout_col]
        .astype(str)
        .str.replace("%", "", regex=False)
        .str.replace("\u00A0", " ", regex=False)
        .str.strip()
    )
    payout_num = pd.to_numeric(payout_raw, errors="coerce")

    type_norm = (
        df_sheet[type_col].astype(str)
        .str.strip().str.lower()
        .replace({"": None})
    )

    # Percent for revenue/sale; fixed for fixed
    pct_fraction = payout_num.where(type_norm.isin(["revenue", "sale"])).apply(
        lambda v: (v/100.0) if pd.notna(v) and v > 1 else (v if pd.notna(v) else DEFAULT_PCT_IF_MISSING)
    )
    fixed_amount = payout_num.where(type_norm.eq("fixed"))

    out = (
        pd.DataFrame({
            "code_norm": df_sheet[code_col].apply(normalize_coupon),
            "affiliate_ID": df_sheet[aff_col].fillna("").astype(str).str.strip(),
            "type_norm": type_norm.fillna("revenue"),
            "pct_fraction": pct_fraction.fillna(DEFAULT_PCT_IF_MISSING),
            "fixed_amount": fixed_amount
        })
        .dropna(subset=["code_norm"])
    )

    # Prefer rows that actually have an affiliate_ID if duplicates exist
    out["has_aff"] = out["affiliate_ID"].astype(str).str.len() > 0
    out = (
        out.sort_values(by=["code_norm", "has_aff"], ascending=[True, False])
           .drop_duplicates(subset=["code_norm"], keep="first")
           .drop(columns=["has_aff"])
    )
    return out

# =======================
# LOAD REPORT
# =======================
end_date = datetime.now().date()
start_date = end_date - timedelta(days=days_back)
print(f"Current date: {end_date}, Start date (days_back={days_back}): {start_date}")

df = pd.read_csv(input_file)

# Convert 'created_date'
df['created_date'] = pd.to_datetime(df['created_date'], format='%b %d, %Y', errors='coerce')
before = len(df)
df = df.dropna(subset=['created_date'])
print(f"Total rows before filtering: {before}")
print(f"Rows with invalid dates dropped: {before - len(df)}")

# Geo mapping
df['geo'] = df['country'].map(COUNTRY_GEO).fillna(df['country'])

# Date filter (inclusive)
df_filtered = df[
    (df['created_date'].dt.date >= start_date) &
    (df['created_date'].dt.date <= end_date)
].copy()
print(f"Rows after filtering date range: {len(df_filtered)}")

# =======================
# DERIVED FIELDS
# =======================
# sale_amount (AED -> USD)
df_filtered['sale_amount'] = df_filtered['AED_net_amount'] / 3.67

# revenue 6% of sale_amount
df_filtered['revenue'] = df_filtered['sale_amount'] * 0.06

# coupon normalization
df_filtered['coupon_norm'] = df_filtered['aff_coupon'].apply(normalize_coupon)

# =======================
# JOIN AFFILIATE MAPPING (type-aware)
# =======================
map_df = load_affiliate_mapping_from_xlsx(affiliate_xlsx_path, AFFILIATE_SHEET)
df_joined = df_filtered.merge(map_df, how="left", left_on="coupon_norm", right_on="code_norm")

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
payout.loc[mask_sale] = df_joined.loc[mask_sale, 'sale_amount'] * df_joined.loc[mask_sale, 'pct_fraction']

mask_fixed = df_joined['type_norm'].str.lower().eq('fixed')
payout.loc[mask_fixed] = df_joined.loc[mask_fixed, 'fixed_amount'].fillna(0.0)

# Enforce: if no affiliate match, set affiliate_id="1", payout=0
payout.loc[missing_aff_mask] = 0.0
df_joined.loc[missing_aff_mask, 'affiliate_ID'] = FALLBACK_AFFILIATE_ID

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
    f"Coupons with no affiliate (set aff={FALLBACK_AFFILIATE_ID}, payout=0): {int(missing_aff_mask.sum())} | "
    f"Type counts -> revenue: {int(mask_rev.sum())}, sale: {int(mask_sale.sum())}, fixed: {int(mask_fixed.sum())}"
)
print(f"Date range processed: {output_df['date'].min() if not output_df.empty else 'N/A'} to {output_df['date'].max() if not output_df.empty else 'N/A'}")
