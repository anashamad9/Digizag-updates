import pandas as pd
from datetime import datetime, timedelta
import os
import re
import unicodedata

# =======================
# CONFIG
# =======================
# Choose how many days back to include (rows from [today - days_back, today), i.e., exclude today)
days_back = 30

OFFER_ID = 1192
STATUS_DEFAULT = "pending"
DEFAULT_PCT_IF_MISSING = 0.0
FALLBACK_AFFILIATE_ID = "1"

# Local files
AFFILIATE_XLSX   = "Offers Coupons.xlsx"
AFFILIATE_SHEET  = "Mumzworld"
REPORT_PREFIX    = "DigiZag Dashboard_Commission Dashboard_Table"  # suffix like " (1).csv" is OK
OUTPUT_CSV       = "mumzworld.csv"

# =======================
# PATHS
# =======================
script_dir = os.path.dirname(os.path.abspath(__file__))
input_dir  = os.path.join(script_dir, '..', 'input data')
output_dir = os.path.join(script_dir, '..', 'output data')
os.makedirs(output_dir, exist_ok=True)

affiliate_xlsx_path = os.path.join(input_dir, AFFILIATE_XLSX)
output_file         = os.path.join(output_dir, OUTPUT_CSV)

# =======================
# HELPERS
# =======================
def normalize_coupon(s: str) -> str:
    """
    Aggressive normalizer so sheet & report codes match:
    - cast to str, replace NBSP, strip
    - Unicode NFKC normalize
    - uppercase
    - keep only A–Z and 0–9 (remove dashes, spaces, emojis, etc.)
    """
    if pd.isna(s):
        return ""
    s = str(s).replace("\u00A0", " ").strip()
    s = unicodedata.normalize("NFKC", s)
    s = s.upper()
    s = re.sub(r"[^A-Z0-9]+", "", s)
    return s

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


def _as_pct_fraction(series: pd.Series) -> pd.Series:
    """
    Accept 73, 73%, or 0.73 → returns fraction in [0..1]
    """
    raw = series.astype(str).str.replace("%", "", regex=False).str.strip()
    num = pd.to_numeric(raw, errors="coerce")
    return num.apply(lambda v: (v/100.0) if pd.notna(v) and v > 1 else (v if pd.notna(v) else DEFAULT_PCT_IF_MISSING))


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


def find_latest_csv_by_prefix(directory: str, prefix: str) -> str:
    """
    Return the path to the most recently modified CSV whose *base name* starts with `prefix`.
    Matches e.g. 'DigiZag Dashboard_Commission Dashboard_Table.csv' or '... (3).csv'
    """
    prefix_norm = prefix.lower().strip()
    candidates = []
    for f in os.listdir(directory):
        if not f.lower().endswith(".csv"):
            continue
        base = os.path.splitext(f)[0].lower().strip()
        if base.startswith(prefix_norm):
            candidates.append(os.path.join(directory, f))
    if not candidates:
        avail = [f for f in os.listdir(directory) if f.lower().endswith(".csv")]
        raise FileNotFoundError(
            f"No CSV starting with '{prefix}' in {directory}. Available CSVs: {avail}"
        )
    return max(candidates, key=os.path.getmtime)

# =======================
# LOAD & PREP REPORT
# =======================
end_date = datetime.now().date()             # exclusive upper bound (we exclude "today")
start_date = end_date - timedelta(days=days_back)
print(f"Window: {start_date} ≤ date < {end_date} (exclude today)")

input_file = find_latest_csv_by_prefix(input_dir, REPORT_PREFIX)
print(f"Using input file: {os.path.basename(input_file)}")

df = pd.read_csv(input_file)

# Ensure Date_ordered is datetime & within the window (exclude today)
df['Date_ordered'] = pd.to_datetime(df['Date_ordered'], format='%b %d, %Y', errors='coerce')
df = df.dropna(subset=['Date_ordered'])
df = df[(df['Date_ordered'].dt.date >= start_date) & (df['Date_ordered'].dt.date < end_date)]

# Expand rows by # Orders New/Repeat and compute per-order sale_amount + platform revenue
expanded = []
for _, row in df.iterrows():
    new_orders    = int(row.get('# Orders New Customers', 0) or 0)
    repeat_orders = int(row.get('# Orders Repeat Customers', 0) or 0)
    order_date    = row['Date_ordered']  # keep datetime
    coupon_raw    = row.get('follower_code')

    # New
    if new_orders > 0 and pd.notnull(row.get('New Cust Revenue')):
        try:
            total_new_rev = float(row['New Cust Revenue'])
        except Exception:
            total_new_rev = 0.0
        sale_per = (total_new_rev / new_orders) if new_orders else 0.0
        for _ in range(new_orders):
            expanded.append({
                'order_date': order_date,
                'country': row.get('Country'),
                'user_type': 'New',
                'sale_amount': sale_per,
                'coupon_code': coupon_raw,
                # platform revenue (per your earlier logic)
                'revenue': sale_per * 0.08
            })

    # Repeat
    if repeat_orders > 0 and pd.notnull(row.get('Repeat Cust Revenue')):
        try:
            total_rep_rev = float(row['Repeat Cust Revenue'])
        except Exception:
            total_rep_rev = 0.0
        sale_per = (total_rep_rev / repeat_orders) if repeat_orders else 0.0
        for _ in range(repeat_orders):
            expanded.append({
                'order_date': order_date,
                'country': row.get('Country'),
                'user_type': 'Repeat',
                'sale_amount': sale_per,
                'coupon_code': coupon_raw,
                'revenue': sale_per * 0.03
            })

df_expanded = pd.DataFrame(expanded)
if df_expanded.empty:
    # Nothing to output; still create an empty file with headers
    pd.DataFrame(columns=[
        'offer','affiliate_id','date','status','payout','revenue','sale amount','coupon','geo'
    ]).to_csv(output_file, index=False)
    print("No rows after expansion; empty file written.")
    raise SystemExit(0)

df_expanded['order_date']  = pd.to_datetime(df_expanded['order_date'])
df_expanded['coupon_norm'] = df_expanded['coupon_code'].apply(normalize_coupon)

# =======================
# JOIN AFFILIATE MAPPING (type-aware)
# =======================
map_df = load_affiliate_mapping_from_xlsx(affiliate_xlsx_path, AFFILIATE_SHEET)
df_joined = df_expanded.merge(map_df, how="left", left_on="coupon_norm", right_on="code_norm")

# Debug aide: show a few unmatched coupon norms
missing_aff_mask = df_joined['affiliate_ID'].isna() | (df_joined['affiliate_ID'].astype(str).str.strip() == "")
if missing_aff_mask.any():
    miss = df_joined.loc[missing_aff_mask, ['coupon_norm']].drop_duplicates().sort_values('coupon_norm')
    print("Unmatched coupons (first 30):", miss.head(30).to_dict(orient="list"))

# Normalize mapping fields
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

mask_rev   = df_joined['type_norm'].str.lower().eq('revenue')
mask_sale  = df_joined['type_norm'].str.lower().eq('sale')
mask_fixed = df_joined['type_norm'].str.lower().eq('fixed')

payout.loc[mask_rev]   = df_joined.loc[mask_rev,   'revenue']     * df_joined.loc[mask_rev,   'pct_fraction']
payout.loc[mask_sale]  = df_joined.loc[mask_sale,  'sale_amount'] * df_joined.loc[mask_sale,  'pct_fraction']
payout.loc[mask_fixed] = df_joined.loc[mask_fixed, 'fixed_amount'].fillna(0.0)

# Fallback for unmatched coupons: affiliate_id="1", payout=0
payout.loc[missing_aff_mask] = 0.0
df_joined.loc[missing_aff_mask, 'affiliate_ID'] = FALLBACK_AFFILIATE_ID

df_joined['payout'] = payout.round(2)

# =======================
# BUILD OUTPUT (NEW STRUCTURE)
# =======================
output_df = pd.DataFrame({
    'offer':        OFFER_ID,
    'affiliate_id': df_joined['affiliate_ID'],
    'date':         df_joined['order_date'].dt.strftime('%m-%d-%Y'),
    'status':       STATUS_DEFAULT,
    'payout':       df_joined['payout'],
    'revenue':      df_joined['revenue'].round(2),
    'sale amount':  df_joined['sale_amount'].round(2),
    'coupon':       df_joined['coupon_norm'],
    'geo':          df_joined['country'],
})

# =======================
# SAVE
# =======================
output_df.to_csv(output_file, index=False)

print(f"Saved: {output_file}")
print(
    f"Rows: {len(output_df)} | "
    f"Coupons with no affiliate (set aff={FALLBACK_AFFILIATE_ID}, payout=0): {int(missing_aff_mask.sum())} | "
    f"Type counts -> revenue: {int(mask_rev.sum())}, sale: {int(mask_sale.sum())}, fixed: {int(mask_fixed.sum())}"
)
if not output_df.empty:
    print(f"Date range processed: {output_df['date'].min()} → {output_df['date'].max()}")