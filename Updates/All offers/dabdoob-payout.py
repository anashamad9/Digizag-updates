import pandas as pd
from datetime import datetime, timedelta
import os
import re

# =======================
# CONFIG
# =======================
days_back = 80
OFFER_ID = 1329
STATUS_DEFAULT = "pending"          # always "pending"
DEFAULT_PCT_IF_MISSING = 0.0        # fraction fallback when percent missing (0.30 == 30%)
FALLBACK_AFFILIATE_ID = "1"         # when coupon has no affiliate, use "1" and payout=0

# Local files
AFFILIATE_XLSX  = "Offers Coupons.xlsx"    # multi-sheet Excel
AFFILIATE_SHEET = "Dabdoob"                # <-- coupons sheet name for this offer
REPORT_PREFIX   = "Orders_Coupons_Report_Digizag"  # dynamic filename start
REPORT_SHEET    = "Sheet1"

# FX helpers (to USD)
USD_PER_SAR = 1 / 3.75
USD_PER_AED = 1 / 3.67
USD_PER_BHD = 2.65
USD_PER_KWD = 3.26

# =======================
# PATHS
# =======================
script_dir = os.path.dirname(os.path.abspath(__file__))
input_dir = os.path.join(script_dir, '..', 'input data')
output_dir = os.path.join(script_dir, '..', 'output data')
os.makedirs(output_dir, exist_ok=True)

affiliate_xlsx_path = os.path.join(input_dir, AFFILIATE_XLSX)
output_file = os.path.join(output_dir, 'dabdoub.csv')  # keeping your filename

# =======================
# HELPERS
# =======================
def find_matching_xlsx(directory: str, prefix: str) -> str:
    """
    Find an .xlsx in `directory` whose base filename starts with `prefix` (case-insensitive).
    - Ignores temporary files like '~$...'
    - Prefers exact '<prefix>.xlsx' if present
    - Otherwise returns the newest by modified time
    """
    prefix_lower = prefix.lower()
    candidates = []
    for fname in os.listdir(directory):
        if fname.startswith("~$"):
            continue
        if not fname.lower().endswith(".xlsx"):
            continue
        base = os.path.splitext(fname)[0].lower()
        if base.startswith(prefix_lower):
            candidates.append(os.path.join(directory, fname))

    if not candidates:
        available = [f for f in os.listdir(directory) if f.lower().endswith(".xlsx")]
        raise FileNotFoundError(
            f"No .xlsx file starting with '{prefix}' found in: {directory}\n"
            f"Available .xlsx files: {available}"
        )

    exact = [p for p in candidates if os.path.basename(p).lower() == (prefix_lower + ".xlsx")]
    if exact:
        return exact[0]

    return max(candidates, key=os.path.getmtime)

def normalize_coupon(x: str) -> str:
    """Uppercase, trim, and take the first token if multiple codes separated by ; , or whitespace."""
    if pd.isna(x):
        return ""
    s = str(x).strip().upper()
    parts = re.split(r"[;,\s]+", s)
    return parts[0] if parts else s

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

    # percent for revenue/sale
    pct_fraction = payout_num.where(type_norm.isin(["revenue", "sale"]))
    pct_fraction = pct_fraction.apply(
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
# LOAD REPORT (dynamic filename)
# =======================
today = datetime.now().date()
end_date = today
start_date = end_date - timedelta(days=days_back)
print(f"Current date: {today}, Start date (days_back={days_back}): {start_date}")

input_file = find_matching_xlsx(input_dir, REPORT_PREFIX)
print(f"Using report file: {input_file}")

df = pd.read_excel(input_file, sheet_name=REPORT_SHEET)

# Parse order datetime
df['Order Date (Full date)'] = pd.to_datetime(df['Order Date (Full date)'], format='%d %b, %Y %H:%M:%S', errors='coerce')
df = df.dropna(subset=['Order Date (Full date)'])

# Filter: last N days, not Cancelled, exclude today
df_filtered = df[
    (df['Order Date (Full date)'].dt.date >= start_date) &
    (df['Status Of Order'].astype(str).str.strip().str.lower() != 'cancelled') &
    (df['Order Date (Full date)'].dt.date < today)
].copy()

# =======================
# DERIVED FIELDS (sale_amount & revenue)
# =======================
def compute_sale_amount(country: str, subtotal):
    try:
        v = float(subtotal)
    except Exception:
        return 0.0
    if country == 'Saudi Arabia':
        return v * USD_PER_SAR
    elif country == 'UAE':
        return v * USD_PER_AED
    elif country == 'Bahrain':
        return v * USD_PER_BHD
    elif country == 'Kuwait':
        return v * USD_PER_KWD
    else:
        # default to AED rate if unrecognized
        return v * USD_PER_AED

df_filtered['sale_amount'] = df_filtered.apply(lambda r: compute_sale_amount(str(r.get('Country', '')), r.get('Subtotal')), axis=1)

# Base revenue 10% of sale_amount
df_filtered['revenue'] = df_filtered['sale_amount'] * 0.10

# GEO mapping
geo_mapping = {'Saudi Arabia': 'ksa', 'UAE': 'uae', 'Bahrain': 'bhr', 'Kuwait': 'kwt'}
df_filtered['geo'] = df_filtered['Country'].map(geo_mapping).fillna('no-geo')

# Normalize coupon for joining
df_filtered['coupon_norm'] = df_filtered['Coupon'].apply(normalize_coupon)

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
    'date': df_joined['Order Date (Full date)'].dt.strftime('%m-%d-%Y'),
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
