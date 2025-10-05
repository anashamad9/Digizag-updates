import pandas as pd
import os
import re

# =======================
# CONFIG (BELUAR)
# =======================
OFFER_ID = 1253  # <<< IMPORTANT: Only 1253
STATUS_DEFAULT = "pending"
DEFAULT_PCT_IF_MISSING = 0.0
FALLBACK_AFFILIATE_ID = "1"

# Input/Output
INPUT_CSV  = "SMART ConverterV2 - Raw_Data (3).csv"
AFFILIATE_XLSX = "Offers Coupons.xlsx"
AFFILIATE_SHEET = "Beluar"   # change if your tab name differs
OUTPUT_CSV = "beluar.csv"

# =======================
# PATHS
# =======================
script_dir = os.path.dirname(os.path.abspath(__file__))
input_dir  = os.path.join(script_dir, '..', 'input data')
output_dir = os.path.join(script_dir, '..', 'output data')
os.makedirs(output_dir, exist_ok=True)

input_file = os.path.join(input_dir, INPUT_CSV)
affiliate_xlsx_path = os.path.join(input_dir, AFFILIATE_XLSX)
output_file = os.path.join(output_dir, OUTPUT_CSV)

# =======================
# HELPERS
# =======================
def normalize_coupon(x: str) -> str:
    """Uppercase, trim, and take first token if multiple codes separated by ; , or whitespace."""
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
        'customer_type','customer type','customer segment','customersegment',
        'new_vs_old','new vs old','new/old','new old','new_vs_existing','new vs existing',
        'user_type','user type','usertype','type_customer','type customer','audience',
    ]
    new_tokens = {
        'new','newuser','newusers','newcustomer','newcustomers','ftu','first',
        'firstorder','firsttime','acquisition','prospect'
    }
    old_tokens = {
        'old','olduser','oldcustomer','existing','existinguser','existingcustomer',
        'return','returning','repeat','rtu','retention','loyal','existingusers'
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
    """Return mapping with code_norm, affiliate_ID, type_norm, pct_new, pct_old, fixed_new, fixed_old."""
    df_sheet = pd.read_excel(xlsx_path, sheet_name=sheet_name, dtype=str)
    cols_lower = {str(c).lower().strip(): c for c in df_sheet.columns}

    def need(name: str) -> str:
        col = cols_lower.get(name)
        if not col:
            raise ValueError(f"[{sheet_name}] must contain a '{name}' column.")
        return col

    code_col  = need('code')
    aff_col   = cols_lower.get('id') or cols_lower.get('affiliate_id')
    type_col  = need('type')
    payout_col = cols_lower.get('payout')
    new_col    = cols_lower.get('new customer payout')
    old_col    = cols_lower.get('old customer payout')

    if not aff_col:
        raise ValueError(f"[{sheet_name}] must contain an 'ID' (or 'affiliate_ID') column.")
    if not (payout_col or new_col or old_col):
        raise ValueError(f"[{sheet_name}] must contain at least one payout column.")

    def extract_numeric(col_name: str) -> pd.Series:
        if not col_name:
            return pd.Series([pd.NA] * len(df_sheet), dtype='Float64')
        raw = df_sheet[col_name].astype(str).str.replace('%', '', regex=False).str.strip()
        return pd.to_numeric(raw, errors='coerce')

    payout_any     = extract_numeric(payout_col)
    payout_new_raw = extract_numeric(new_col).fillna(payout_any)
    payout_old_raw = extract_numeric(old_col).fillna(payout_any)

    type_norm = (
        df_sheet[type_col].astype(str).str.strip().str.lower()
        .replace({'': None})
        .fillna('revenue')
    )

    def pct_from(values: pd.Series) -> pd.Series:
        pct = values.where(type_norm.isin(['revenue', 'sale']))
        return pct.apply(lambda v: (v / 100.0) if pd.notna(v) and v > 1 else (v if pd.notna(v) else pd.NA))

    def fixed_from(values: pd.Series) -> pd.Series:
        return values.where(type_norm.eq('fixed'))

    pct_new  = pct_from(payout_new_raw)
    pct_old  = pct_from(payout_old_raw)
    pct_new  = pct_new.fillna(pct_old)
    pct_old  = pct_old.fillna(pct_new)

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
# READ RAW CSV
# =======================
def _canonical_csv_column(name: str) -> str:
    """Map messy Smart Converter headers to canonical names used downstream."""
    clean = str(name).strip().lower()
    compact = re.sub(r'[^a-z0-9]+', '', clean)

    if 'offerid' in compact:
        return 'offer_id'
    if any(token in compact for token in ['datetime', 'orderdate', 'processdate', 'transactiondate', 'createdat', 'date']):
        return 'order_date'
    if any(token in compact for token in ['couponcode', 'coupon', 'promo', 'voucher', 'affiliateinfo', 'affiliatecode', 'code']):
        return 'coupon_code'
    if any(token in compact for token in ['revenue', 'commission', 'earned', 'netrevenue']):
        return 'revenue'
    if any(token in compact for token in ['saleamount', 'ordervalue', 'orderamount', 'grossamount', 'amount', 'payoutamount']):
        return 'sale_amount'
    if 'geo' in compact or 'country' in compact or 'market' in compact:
        return 'geo'
    return None


df_raw = pd.read_csv(input_file)
if df_raw.empty:
    df = pd.DataFrame(columns=['offer_id', 'order_date', 'coupon_code', 'revenue', 'sale_amount', 'geo'])
else:
    df_raw.columns = [str(c).strip() for c in df_raw.columns]
    canonical_columns = [_canonical_csv_column(col) or col for col in df_raw.columns]
    df = df_raw.copy()
    df.columns = canonical_columns

    if df.columns.duplicated().any():
        df = df.T.groupby(level=0).first().T

    df = df.loc[:, [c for c in df.columns if not str(c).startswith('Unnamed')]]

    for optional in ['revenue', 'sale_amount', 'offer_id', 'geo']:
        if optional not in df.columns:
            df[optional] = pd.NA

required_basic = ['order_date', 'coupon_code']
missing_basic = [c for c in required_basic if c not in df.columns]
if missing_basic:
    raise ValueError(f"Missing required fields from CSV: {missing_basic}")

if not (('revenue' in df.columns) or ('sale_amount' in df.columns)):
    raise ValueError("CSV must provide at least one monetary field: 'revenue' (USD) or 'sale_amount'.")

if 'offer_id' in df.columns:
    df = df[df['offer_id'].astype(str).str.strip() == str(OFFER_ID)]

df = df.reset_index(drop=True)

# =======================
# DERIVED FIELDS
# =======================
df['revenue']     = pd.to_numeric(df.get('revenue'), errors='coerce')
df['sale_amount'] = pd.to_numeric(df.get('sale_amount'), errors='coerce')
df['coupon_norm'] = df['coupon_code'].apply(normalize_coupon)

# =======================
# JOIN AFFILIATE MAPPING
# =======================
map_df = load_affiliate_mapping_from_xlsx(affiliate_xlsx_path, AFFILIATE_SHEET)
df_joined = df.merge(map_df, how="left", left_on="coupon_norm", right_on="code_norm")

missing_aff_mask = df_joined['affiliate_ID'].isna() | (df_joined['affiliate_ID'].astype(str).str.strip() == "")

df_joined['affiliate_ID'] = df_joined['affiliate_ID'].fillna('').astype(str).str.strip()
df_joined['type_norm']    = df_joined['type_norm'].fillna('revenue')

for col in ['pct_new', 'pct_old']:
    df_joined[col] = pd.to_numeric(df_joined.get(col), errors='coerce').fillna(DEFAULT_PCT_IF_MISSING)
for col in ['fixed_new', 'fixed_old']:
    df_joined[col] = pd.to_numeric(df_joined.get(col), errors='coerce')

is_new_customer = infer_is_new_customer(df_joined)
pct_effective   = df_joined['pct_new'].where(is_new_customer, df_joined['pct_old'])
df_joined['pct_fraction'] = pd.to_numeric(pct_effective, errors='coerce').fillna(DEFAULT_PCT_IF_MISSING)
fixed_effective = df_joined['fixed_new'].where(is_new_customer, df_joined['fixed_old'])
df_joined['fixed_amount'] = pd.to_numeric(fixed_effective, errors='coerce')

payout = pd.Series(0.0, index=df_joined.index)
mask_rev   = df_joined['type_norm'].str.lower().eq('revenue')
mask_sale  = df_joined['type_norm'].str.lower().eq('sale')
mask_fixed = df_joined['type_norm'].str.lower().eq('fixed')

payout.loc[mask_rev]   = df_joined.loc[mask_rev,   'revenue'].fillna(0.0)     * df_joined.loc[mask_rev,   'pct_fraction']
payout.loc[mask_sale]  = df_joined.loc[mask_sale,  'sale_amount'].fillna(0.0) * df_joined.loc[mask_sale,  'pct_fraction']
payout.loc[mask_fixed] = df_joined.loc[mask_fixed, 'fixed_amount'].fillna(0.0)

payout.loc[missing_aff_mask] = 0.0
df_joined.loc[missing_aff_mask, 'affiliate_ID'] = FALLBACK_AFFILIATE_ID
df_joined['payout'] = payout.round(2)

# =======================
# BUILD OUTPUT
# =======================
output_df = pd.DataFrame({
    'offer': OFFER_ID,
    'affiliate_id': df_joined['affiliate_ID'],
    'date': pd.to_datetime(df_joined['order_date'], errors='coerce').dt.strftime('%m-%d-%Y'),
    'status': STATUS_DEFAULT,
    'payout': df_joined['payout'],
    'revenue': df_joined['revenue'].round(2) if 'revenue' in df_joined.columns else 0.0,
    'sale amount': df_joined['sale_amount'].round(2) if 'sale_amount' in df_joined.columns else pd.NA,
    'coupon': df_joined['coupon_norm'],
    'geo': 'ksa',
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
if not output_df.empty:
    print(f"Date range processed: {output_df['date'].min()} to {output_df['date'].max()}")
else:
    print("No rows after processing.")
