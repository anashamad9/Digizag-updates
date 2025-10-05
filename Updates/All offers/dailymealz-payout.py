import pandas as pd
from datetime import datetime, timedelta
import os
import re

# =======================
# CONFIG
# =======================
days_back = 18
OFFER_ID = 1324
STATUS_DEFAULT = "pending"          # always pending
DEFAULT_PCT_IF_MISSING = 0.0        # fallback fraction if percent missing
FALLBACK_AFFILIATE_ID = "1"         # when no affiliate match: set to "1" and payout=0
GEO = "no-geo"

# Local files
AFFILIATE_XLSX  = "Offers Coupons.xlsx"    # multi-sheet Excel
AFFILIATE_SHEET = "DailyMeals"             # coupons sheet for this offer
REPORT_PREFIX   = "Dailymealz - DigiZag (commission report)_Detailed Code usages_Table"  # dynamic CSV name start

# =======================
# PATHS
# =======================
script_dir = os.path.dirname(os.path.abspath(__file__))
input_dir = os.path.join(script_dir, '..', 'input data')
output_dir = os.path.join(script_dir, '..', 'output data')
os.makedirs(output_dir, exist_ok=True)

affiliate_xlsx_path = os.path.join(input_dir, AFFILIATE_XLSX)
output_file = os.path.join(output_dir, 'dailymealz.csv')

# =======================
# HELPERS
# =======================
def find_matching_csv(directory: str, prefix: str) -> str:
    """
    Find a .csv in `directory` whose base filename starts with `prefix` (case-insensitive).
    - Ignores temporary files like '~$...'
    - Prefers exact '<prefix>.csv' if present
    - Otherwise returns the newest by modified time
    """
    prefix_lower = prefix.lower()
    candidates = []
    for fname in os.listdir(directory):
        if fname.startswith("~$"):
            continue
        if not fname.lower().endswith(".csv"):
            continue
        base = os.path.splitext(fname)[0].lower()
        if base.startswith(prefix_lower):
            candidates.append(os.path.join(directory, fname))

    if not candidates:
        available = [f for f in os.listdir(directory) if f.lower().endswith(".csv")]
        raise FileNotFoundError(
            f"No .csv file starting with '{prefix}' found in: {directory}\n"
            f"Available .csv files: {available}"
        )

    exact = [p for p in candidates if os.path.basename(p).lower() == (prefix_lower + ".csv")]
    if exact:
        return exact[0]
    return max(candidates, key=os.path.getmtime)

def normalize_coupon(x: str) -> str:
    """Uppercase, trim, take the first token if multiple codes separated by ; , or whitespace."""
    if pd.isna(x):
        return ""
    s = str(x).strip().upper()
    parts = re.split(r"[;,\s]+", s)
    return parts[0] if parts else s

def infer_is_new_customer(df: pd.DataFrame) -> pd.Series:
    """Infer a boolean new-customer flag from common columns; default False when no signal."""
    if df.empty:
        return pd.Series(False, index=df.index, dtype=bool)

    candidates = [
        'customer_type',
        'customer type',
        'customer_type',
        'customer type',
        'customer segment',
        'customersegment',
        'new_vs_old',
        'new vs old',
        'new/old',
        'new old',
        'new_vs_existing',
        'new vs existing',
        'user_type',
        'user type',
        'usertype',
        'type_customer',
        'type customer',
        'audience',
    ]

    new_tokens = {
        'new', 'newuser', 'newusers', 'newcustomer', 'newcustomers',
        'ftu', 'first', 'firstorder', 'firsttime', 'acquisition', 'prospect'
    }
    old_tokens = {
        'old', 'olduser', 'oldcustomer', 'existing', 'existinguser', 'existingcustomer',
        'return', 'returning', 'repeat', 'rtu', 'retention', 'loyal', 'existingusers'
    }

    columns_map = {str(c).strip().lower(): c for c in df.columns}
    result = pd.Series(False, index=df.index, dtype=bool)
    resolved = pd.Series(False, index=df.index, dtype=bool)

    def tokenize(value) -> set:
        if pd.isna(value):
            return set()
        text = ''.join(ch if ch.isalnum() else ' ' for ch in str(value).lower())
        return {tok for tok in text.split() if tok}

    for key in candidates:
        actual = columns_map.get(key)
        if not actual:
            continue
        tokens_series = df[actual].apply(tokenize)
        is_new = tokens_series.apply(lambda toks: bool(toks & new_tokens))
        is_old = tokens_series.apply(lambda toks: bool(toks & old_tokens))
        recognized = (is_new | is_old) & ~resolved
        if recognized.any():
            result.loc[recognized] = is_new.loc[recognized]
            resolved.loc[recognized] = True
        if resolved.all():
            break
    return result



def load_affiliate_mapping_from_xlsx(xlsx_path: str, sheet_name: str) -> pd.DataFrame:
    """Return mapping with columns code_norm, affiliate_ID, type_norm, pct_new, pct_old, fixed_new, fixed_old."""
    df_sheet = pd.read_excel(xlsx_path, sheet_name=sheet_name, dtype=str)
    cols_lower = {str(c).lower().strip(): c for c in df_sheet.columns}

    def need(name: str) -> str:
        col = cols_lower.get(name)
        if not col:
            raise ValueError(f"[{sheet_name}] must contain a '{name}' column.")
        return col

    code_col = need('code')
    aff_col = cols_lower.get('id') or cols_lower.get('affiliate_id')
    type_col = need('type')
    payout_col = cols_lower.get('payout')
    new_col = cols_lower.get('new customer payout')
    old_col = cols_lower.get('old customer payout')

    if not aff_col:
        raise ValueError(f"[{sheet_name}] must contain an 'ID' (or 'affiliate_ID') column.")
    if not (payout_col or new_col or old_col):
        raise ValueError(f"[{sheet_name}] must contain at least one payout column (e.g., 'payout').")

    def extract_numeric(col_name: str) -> pd.Series:
        if not col_name:
            return pd.Series([pd.NA] * len(df_sheet), dtype='Float64')
        raw = df_sheet[col_name].astype(str).str.replace('%', '', regex=False).str.strip()
        return pd.to_numeric(raw, errors='coerce')

    payout_any = extract_numeric(payout_col)
    payout_new_raw = extract_numeric(new_col).fillna(payout_any)
    payout_old_raw = extract_numeric(old_col).fillna(payout_any)

    type_norm = (
        df_sheet[type_col]
        .astype(str)
        .str.strip()
        .str.lower()
        .replace({'': None})
        .fillna('revenue')
    )

    def pct_from(values: pd.Series) -> pd.Series:
        pct = values.where(type_norm.isin(['revenue', 'sale']))
        return pct.apply(lambda v: (v / 100.0) if pd.notna(v) and v > 1 else (v if pd.notna(v) else pd.NA))

    def fixed_from(values: pd.Series) -> pd.Series:
        return values.where(type_norm.eq('fixed'))

    pct_new = pct_from(payout_new_raw)
    pct_old = pct_from(payout_old_raw)
    pct_new = pct_new.fillna(pct_old)
    pct_old = pct_old.fillna(pct_new)

    fixed_new = fixed_from(payout_new_raw)
    fixed_old = fixed_from(payout_old_raw)
    fixed_new = fixed_new.fillna(fixed_old)
    fixed_old = fixed_old.fillna(fixed_new)

    out = pd.DataFrame({
        'code_norm': df_sheet[code_col].apply(normalize_coupon),
        'affiliate_ID': df_sheet[aff_col].fillna('').astype(str).str.strip(),
        'type_norm': type_norm,
        'pct_new': pd.to_numeric(pct_new, errors='coerce').fillna(DEFAULT_PCT_IF_MISSING),
        'pct_old': pd.to_numeric(pct_old, errors='coerce').fillna(DEFAULT_PCT_IF_MISSING),
        'fixed_new': pd.to_numeric(fixed_new, errors='coerce'),
        'fixed_old': pd.to_numeric(fixed_old, errors='coerce'),
    }).dropna(subset=['code_norm'])

    return out.drop_duplicates(subset=['code_norm'], keep='last')


# =======================
# LOAD REPORT (dynamic)
# =======================
end_date = datetime.now().date()
start_date = end_date - timedelta(days=days_back)
print(f"Current date: {end_date}, Start date (days_back={days_back}): {start_date}")

input_file = find_matching_csv(input_dir, REPORT_PREFIX)
print(f"Using report file: {input_file}")

df = pd.read_csv(input_file)

# Parse 'Voucher_applied_date'
df['Voucher_applied_date'] = pd.to_datetime(
    df['Voucher_applied_date'],
    format='%b %d, %Y, %I:%M %p',
    errors='coerce'
)
before = len(df)
df = df.dropna(subset=['Voucher_applied_date'])
print(f"Total rows before filtering: {before}")
print(f"Rows with invalid dates dropped: {before - len(df)}")

# Filter accepted & date range (inclusive of end_date)
df_filtered = df[df['status'].astype(str).str.upper() == 'ACCEPTED'].copy()
df_filtered = df_filtered[
    (df_filtered['Voucher_applied_date'].dt.date >= start_date) &
    (df_filtered['Voucher_applied_date'].dt.date <= end_date)
].copy()
print(f"Rows after filtering cancelled and date range: {len(df_filtered)}")

# =======================
# DERIVED FIELDS
# =======================
# sale_amount (SAR -> USD)
df_filtered['sale_amount'] = df_filtered['total_price'] / 3.75

# revenue: fixed per user type (NEW USER -> 16, else 4)
df_filtered['revenue'] = df_filtered.apply(
    lambda r: 16 if str(r.get('new_vs_old', '')).strip().upper() == 'NEW USER' else 4,
    axis=1
)

# Normalize coupon for joining
df_filtered['coupon_norm'] = df_filtered['code'].apply(normalize_coupon)

# =======================
# JOIN AFFILIATE MAPPING (type-aware)
# =======================
map_df = load_affiliate_mapping_from_xlsx(affiliate_xlsx_path, AFFILIATE_SHEET)
df_joined = df_filtered.merge(map_df, how="left", left_on="coupon_norm", right_on="code_norm")

# Missing affiliate?
missing_aff_mask = df_joined['affiliate_ID'].isna() | (df_joined['affiliate_ID'].astype(str).str.strip() == "")

# Normalize fields
df_joined['affiliate_ID'] = df_joined['affiliate_ID'].fillna('').astype(str).str.strip()
df_joined['type_norm'] = df_joined['type_norm'].fillna('revenue')
for col in ['pct_new', 'pct_old']:
    df_joined[col] = pd.to_numeric(df_joined.get(col), errors='coerce').fillna(DEFAULT_PCT_IF_MISSING)
for col in ['fixed_new', 'fixed_old']:
    df_joined[col] = pd.to_numeric(df_joined.get(col), errors='coerce')
is_new_customer = infer_is_new_customer(df_joined)
pct_effective = df_joined['pct_new'].where(is_new_customer, df_joined['pct_old'])
df_joined['pct_fraction'] = pd.to_numeric(pct_effective, errors='coerce').fillna(DEFAULT_PCT_IF_MISSING)
fixed_effective = df_joined['fixed_new'].where(is_new_customer, df_joined['fixed_old'])
df_joined['fixed_amount'] = pd.to_numeric(fixed_effective, errors='coerce')
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
    'date': df_joined['Voucher_applied_date'].dt.strftime('%m-%d-%Y'),
    'status': STATUS_DEFAULT,
    'payout': df_joined['payout'],
    'revenue': df_joined['revenue'].round(2),
    'sale amount': df_joined['sale_amount'].round(2),
    'coupon': df_joined['coupon_norm'],
    'geo': GEO,
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
if len(output_df) < len(df_joined):
    print("Warning: Some rows were excluded during output creation.")