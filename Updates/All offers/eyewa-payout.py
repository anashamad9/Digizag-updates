import pandas as pd
from datetime import datetime, timedelta
import os
import re
from typing import Optional

# =======================
# CONFIG
# =======================
days_back = 18
OFFER_ID = 1204
STATUS_DEFAULT = "pending"          # always "pending"
DEFAULT_PCT_IF_MISSING = 0.0        # fallback fraction for % values (0.30 == 30%)
FALLBACK_AFFILIATE_ID = "1"         # when no affiliate match: set to "1" and payout=0

# Local files
AFFILIATE_XLSX  = "Offers Coupons.xlsx"
AFFILIATE_SHEET = "Eyewa"           # coupons sheet name for this offer
REPORT_PREFIX   = "ConversionsExport_"  # dynamic CSV name start

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
    """Uppercase, trim, first token if multiple codes separated by ; , or whitespace."""
    if pd.isna(x):
        return ""
    s = str(x).strip().upper()
    parts = re.split(r"[;,\s]+", s)
    return parts[0] if parts else s

# ConversionsExport_YYYY-MM-DD_YYYY-MM-DD.csv  (we read the first date as the 'start')
_START_DATE_RE = re.compile(r'ConversionsExport_(\d{4}-\d{2}-\d{2})_\d{4}-\d{2}-\d{2}', re.IGNORECASE)

def extract_start_date_from_name(filename: str) -> Optional[datetime]:
    m = _START_DATE_RE.search(os.path.basename(filename))
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1), '%Y-%m-%d')
    except Exception:
        return None

def find_latest_conversions_export(directory: str, prefix: str) -> str:
    """
    Return path to the 'best' ConversionsExport CSV:
      1) consider only *.csv with basename starting with `prefix` (case-insensitive)
      2) prefer the one with the newest embedded start date
      3) fallback to newest by modification time if none parse cleanly
    """
    candidates = []
    for fname in os.listdir(directory):
        if fname.startswith("~$"):
            continue
        if not fname.lower().endswith(".csv"):
            continue
        base = os.path.splitext(fname)[0]
        if base.lower().startswith(prefix.lower()):
            candidates.append(os.path.join(directory, fname))

    if not candidates:
        available = [f for f in os.listdir(directory) if f.lower().endswith(".csv")]
        raise FileNotFoundError(
            f"No .csv files starting with '{prefix}' found in: {directory}\n"
            f"Available .csv files: {available}"
        )

    dated = []
    for p in candidates:
        dt = extract_start_date_from_name(p)
        if dt:
            dated.append((dt, p))

    if dated:
        dated.sort(key=lambda t: t[0])
        return dated[-1][1]  # newest start date

    # fallback: newest by modification time
    return max(candidates, key=os.path.getmtime)

def load_affiliate_mapping_from_xlsx(xlsx_path: str, sheet_name: str) -> pd.DataFrame:
    """
    Returns mapping with: code_norm, affiliate_ID, type_norm, pct_fraction, fixed_amount
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
        raise ValueError(f"[{sheet_name}] must contain a 'type' column with values 'revenue'/'sale'/'fixed'.")
    if not payout_col:
        raise ValueError(f"[{sheet_name}] must contain a payout column (e.g., 'payout').")

    payout_raw = df_sheet[payout_col].astype(str).str.replace("%", "", regex=False).str.strip()
    payout_num = pd.to_numeric(payout_raw, errors="coerce")

    type_norm = df_sheet[type_col].astype(str).str.strip().str.lower().replace({"": None})

    pct_fraction = payout_num.where(type_norm.isin(["revenue", "sale"])).apply(
        lambda v: (v / 100.0) if pd.notna(v) and v > 1 else (v if pd.notna(v) else DEFAULT_PCT_IF_MISSING)
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

def map_geo(geo):
    geo = str(geo).strip() if pd.notnull(geo) else ''
    if geo == 'Saudi Arabia':
        return 'ksa'
    elif geo == 'Kuwait':
        return 'kwt'
    elif geo == 'Qatar':
        return 'qtr'
    elif geo == 'ARE':
        return 'egy'  # keeping your original mapping
    elif geo == 'UAE':
        return 'uae'
    return geo

def calculate_revenue(row):
    sale_amount = float(row['sale_amount']) if pd.notnull(row['sale_amount']) else 0.0
    adv1 = str(row['adv1']).strip() if pd.notnull(row['adv1']) else ''
    if adv1 == '3P':
        return sale_amount * 0.05
    elif adv1 == 'HB Frames':
        return sale_amount * 0.15
    elif adv1 == 'HB Lense':
        return sale_amount * 0.10
    return 0.0

# =======================
# LOAD LATEST REPORT (dynamic)
# =======================
end_date = datetime.now().date()
start_date = end_date - timedelta(days=days_back)
today = datetime.now().date()
print(f"Current date: {end_date}, Start date (days_back={days_back}): {start_date}")

input_file = find_latest_conversions_export(input_dir, REPORT_PREFIX)
print(f"Using input file: {os.path.basename(input_file)}")

df = pd.read_csv(input_file)

# Convert 'date' and exclude current day
df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
df = df.dropna(subset=['date'])
df = df[df['date'].dt.date < today]
print(f"Total rows after dropping invalid dates: {len(df)}")

# Offer filter
df_eyewa = df[df['offer_name'] == 'Eyewa Affiliates Program'].copy()
print(f"Rows with Eyewa Affiliates Program: {len(df_eyewa)}")

# Date filter (exclude end_date to match your prior style)
df_filtered = df_eyewa[
    (df_eyewa['date'].dt.date >= start_date) &
    (df_eyewa['date'].dt.date < end_date)
].copy()

# =======================
# DERIVED FIELDS
# =======================
# revenue by adv1 rule
df_filtered['revenue'] = df_filtered.apply(calculate_revenue, axis=1)

# geo mapping
df_filtered['geo'] = df_filtered['adv2'].apply(map_geo)

# coupon normalization
df_filtered['coupon_norm'] = df_filtered['coupon_code'].apply(normalize_coupon)

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
# COMPUTE PAYOUT (by type)
# =======================
# sale_amount already exists in the input data; ensure numeric
df_joined['sale_amount'] = pd.to_numeric(df_joined['sale_amount'], errors='coerce').fillna(0.0)
df_joined['revenue'] = pd.to_numeric(df_joined['revenue'], errors='coerce').fillna(0.0)

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
# BUILD FINAL OUTPUT
# =======================
output_df = pd.DataFrame({
    'offer': OFFER_ID,
    'affiliate_id': df_joined['affiliate_ID'],
    'date': df_joined['date'].dt.strftime('%m-%d-%Y'),
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
output_file = os.path.join(output_dir, 'eweya.csv')
output_df.to_csv(output_file, index=False)

print(f"Saved: {output_file}")
print(
    f"Rows: {len(output_df)} | "
    f"Coupons with no affiliate (set aff={FALLBACK_AFFILIATE_ID}, payout=0): {int(missing_aff_mask.sum())}"
)
print(f"Date range processed: {output_df['date'].min() if not output_df.empty else 'N/A'} to {output_df['date'].max() if not output_df.empty else 'N/A'}")
if len(output_df) < len(df_joined):
    print("Warning: Some rows were excluded during output creation.")
