import json
import os
import re
from urllib import error as url_error
from urllib import request as url_request

import pandas as pd

# =======================
# CONFIG (AL DAKHEEL)
# =======================
OFFER_ID = 1348  # <<< IMPORTANT: Only 1348
STATUS_DEFAULT = "pending"
DEFAULT_PCT_IF_MISSING = 0.0
FALLBACK_AFFILIATE_ID = "1"
SAR_TO_USD = 3.75

# Data sources
SOURCE_RESOURCE_DEFAULT ="تقرير DigiZag تاريخ 13-01-2026.xlsx"
SOURCE_RESOURCE = os.getenv("AL_DAKHEEL_SOURCE", SOURCE_RESOURCE_DEFAULT)
AFFILIATE_XLSX_PRIMARY = "Offers Coupons.xlsx"
AFFILIATE_SHEET = "Al Dakheel Oud"  # change if your tab name differs
OUTPUT_CSV = "al_dakheel.csv"

# =======================
# PATHS
# =======================
script_dir = os.path.dirname(os.path.abspath(__file__))
input_dir = os.path.join(script_dir, '..', 'input data')
output_dir = os.path.join(script_dir, '..', 'output data')
os.makedirs(output_dir, exist_ok=True)

source_path_default = os.path.join(input_dir, SOURCE_RESOURCE_DEFAULT)
affiliate_xlsx_path = os.path.join(input_dir, AFFILIATE_XLSX_PRIMARY)
output_file = os.path.join(output_dir, OUTPUT_CSV)

if not os.path.exists(affiliate_xlsx_path):
    raise FileNotFoundError(f"Coupons mapping not found: {affiliate_xlsx_path}")


# =======================
# HELPERS
# =======================
def normalize_coupon(code: str) -> str:
    if pd.isna(code):
        return ""
    s = str(code).strip().upper()
    parts = re.split(r"[;,\s]+", s)
    return parts[0] if parts else s


def infer_is_new_customer(df: pd.DataFrame) -> pd.Series:
    if df.empty:
        return pd.Series(False, index=df.index, dtype=bool)

    candidates = [
        'customer_type', 'customer type', 'customer segment', 'customersegment',
        'new_vs_old', 'new vs old', 'new/old', 'new old', 'new_vs_existing', 'new vs existing',
        'user_type', 'user type', 'usertype', 'type_customer', 'type customer', 'audience',
    ]
    new_tokens = {
        'new', 'newuser', 'newusers', 'newcustomer', 'newcustomers', 'ftu', 'first',
        'firstorder', 'firsttime', 'acquisition', 'prospect'
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
        raise ValueError(f"[{sheet_name}] must contain at least one payout column.")

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
        .astype(str).str.strip().str.lower()
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


def _canonical_csv_column(name: str) -> str:
    clean = str(name).strip().lower()
    compact = re.sub(r'[^a-z0-9]+', '', clean)

    if compact in {'saleamount', 'netamount', 'finalamount'} or clean in {'sale_amount', 'sale amount'}:
        return 'sale_amount'
    if compact in {'grossamount', 'ordertotal', 'grossordertotal'} or clean in {'gross_amount', 'gross amount'}:
        return 'gross_amount'
    if compact in {'revenue', 'netrevenue'} or clean == 'revenue':
        return 'revenue'
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


ARABIC_COLUMN_MAP = {
    'تاريخ الطلب': 'order_date',
    'رمز الكوبون': 'coupon_code',
    'الإجمالي الصافي': 'sale_amount',
    'اجمالي الطلب': 'gross_amount',
    'إجمالي الطلب': 'gross_amount',
    'الدولة': 'geo',
}


def normalize_tabular(df_raw: pd.DataFrame) -> pd.DataFrame:
    if df_raw.empty:
        return pd.DataFrame(columns=['offer_id', 'order_date', 'coupon_code', 'revenue', 'sale_amount', 'geo'])

    df_raw.columns = [str(c).strip() for c in df_raw.columns]
    rename_map = {c: ARABIC_COLUMN_MAP.get(str(c).strip(), c) for c in df_raw.columns}
    df_raw = df_raw.rename(columns=rename_map)

    canonical_columns = [_canonical_csv_column(col) or col for col in df_raw.columns]
    df = df_raw.copy()
    df.columns = canonical_columns

    if df.columns.duplicated().any():
        df = df.T.groupby(level=0).first().T

    df = df.loc[:, [c for c in df.columns if not str(c).startswith('Unnamed')]]

    for optional in ['revenue', 'sale_amount', 'offer_id', 'geo']:
        if optional not in df.columns:
            df[optional] = pd.NA

    required_basic = {'order_date', 'coupon_code'}
    missing_basic = [c for c in required_basic if c not in df.columns]
    if missing_basic:
        raise ValueError(f"Source missing required columns after normalization: {missing_basic}")

    return df.reset_index(drop=True)


def fetch_json_resource(resource: str, timeout: int = 60):
    if os.path.exists(resource):
        with open(resource, 'r', encoding='utf-8') as fp:
            return json.load(fp)

    if not resource.lower().startswith(('http://', 'https://')):
        raise ValueError(
            "AL_DAKHEEL_SOURCE must be an HTTP(S) endpoint or an existing file path. "
            f"Got: {resource}"
        )

    req = url_request.Request(resource, headers={'User-Agent': 'python-urllib'})
    try:
        with url_request.urlopen(req, timeout=timeout) as resp:
            status = getattr(resp, 'status', None)
            if status and status != 200:
                raise RuntimeError(
                    f"Failed to fetch data from {resource}, status code: {status}"
                )
            data = resp.read()
    except url_error.HTTPError as exc:
        raise RuntimeError(
            f"Failed to fetch data from {resource}, status code: {exc.code}"
        ) from exc
    except url_error.URLError as exc:
        raise RuntimeError(f"Failed to fetch data from {resource}: {exc}") from exc

    return json.loads(data.decode('utf-8'))


def normalize_json_to_df(payload_json) -> pd.DataFrame:
    root = pd.json_normalize(payload_json)
    if 'data' in root.columns:
        series = root['data']
        exploded = series.explode().dropna()
        df_rows = pd.json_normalize(exploded)
    else:
        df_rows = pd.json_normalize(payload_json) if isinstance(payload_json, list) else root

    rename_map = {}
    for c in list(df_rows.columns):
        low = str(c).lower().strip()
        if low in {'order_date', 'date', 'transaction_date', 'process_date', 'orderdate'}:
            rename_map[c] = 'order_date'
        elif low in {'coupon_code', 'coupon', 'code', 'promo', 'voucher', 'promo_code'}:
            rename_map[c] = 'coupon_code'
        elif low in {'revenue', 'revenue_usd', 'net_revenue', 'commission', 'earned', 'network_revenue'}:
            rename_map[c] = 'revenue'
        elif low in {
            'sale_amount', 'order_value', 'final_amount', 'amount', 'total_amount',
            'gross_amount', 'cart_value', 'order_amount', 'order_total'
        }:
            rename_map[c] = 'sale_amount'

    if rename_map:
        df_rows = df_rows.rename(columns=rename_map)

    if df_rows.columns.duplicated().any():
        df_rows = df_rows.loc[:, ~df_rows.columns.duplicated()]

    required_basic = ['order_date', 'coupon_code']
    missing_basic = [c for c in required_basic if c not in df_rows.columns]
    if missing_basic:
        raise ValueError(f"Missing required fields from JSON: {missing_basic}")

    if not (('revenue' in df_rows.columns) or ('sale_amount' in df_rows.columns)):
        raise ValueError("JSON source must include 'revenue' or 'sale_amount'.")

    for col in ['revenue', 'sale_amount']:
        if col in df_rows.columns:
            df_rows[col] = pd.to_numeric(df_rows[col], errors='coerce')

    return df_rows


def normalize_csv(csv_path: str) -> pd.DataFrame:
    df_raw = pd.read_csv(csv_path)
    return normalize_tabular(df_raw)


def normalize_excel(xlsx_path: str) -> pd.DataFrame:
    df_raw = pd.read_excel(xlsx_path)
    return normalize_tabular(df_raw)


def load_source(resource: str) -> pd.DataFrame:
    if resource.lower().startswith(('http://', 'https://')):
        payload = fetch_json_resource(resource, timeout=60)
        return normalize_json_to_df(payload)

    if resource.lower().endswith('.json'):
        payload = fetch_json_resource(resource, timeout=60)
        return normalize_json_to_df(payload)

    if not os.path.isabs(resource):
        candidate = os.path.join(input_dir, resource)
        if os.path.exists(candidate):
            resource = candidate
        elif os.path.exists(os.path.join(script_dir, resource)):
            resource = os.path.join(script_dir, resource)
        elif os.path.exists(resource):
            resource = os.path.abspath(resource)
        else:
            resource = candidate

    if not os.path.exists(resource):
        raise FileNotFoundError(f"Al Dakheel data source not found: {resource}")

    if resource.lower().endswith(('.xlsx', '.xls')):
        return normalize_excel(resource)
    return normalize_csv(resource)


# =======================
# LOAD DATA SOURCE
# =======================
source_resource = SOURCE_RESOURCE or SOURCE_RESOURCE_DEFAULT
df = load_source(source_resource if SOURCE_RESOURCE else source_path_default)

if 'offer_id' in df.columns:
    offer_series = df['offer_id'].astype('string').fillna('').str.strip()
    if offer_series.ne('').any():
        df = df[offer_series == str(OFFER_ID)]

df = df.copy().reset_index(drop=True)

for col in ['revenue', 'sale_amount']:
    if col not in df.columns:
        df[col] = pd.NA


# =======================
# DERIVED FIELDS
# =======================
df['revenue'] = pd.to_numeric(df.get('revenue'), errors='coerce')
df['sale_amount'] = pd.to_numeric(df.get('sale_amount'), errors='coerce')
df['coupon_norm'] = df['coupon_code'].apply(normalize_coupon)

if 'O' in df.columns:
    sale_from_o = pd.to_numeric(df['O'], errors='coerce') / SAR_TO_USD
    df['sale_amount'] = sale_from_o
else:
    df['sale_amount'] = df['sale_amount'] / SAR_TO_USD
df['revenue'] = df['sale_amount'] * 0.07


# =======================
# JOIN AFFILIATE MAPPING
# =======================
map_df = load_affiliate_mapping_from_xlsx(affiliate_xlsx_path, AFFILIATE_SHEET)
df_joined = df.merge(map_df, how='left', left_on='coupon_norm', right_on='code_norm')

missing_aff_mask = df_joined['affiliate_ID'].isna() | (df_joined['affiliate_ID'].astype(str).str.strip() == "")

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
mask_rev = df_joined['type_norm'].str.lower().eq('revenue')
mask_sale = df_joined['type_norm'].str.lower().eq('sale')
mask_fixed = df_joined['type_norm'].str.lower().eq('fixed')

payout.loc[mask_rev] = df_joined.loc[mask_rev, 'revenue'].fillna(0.0) * df_joined.loc[mask_rev, 'pct_fraction']
payout.loc[mask_sale] = df_joined.loc[mask_sale, 'sale_amount'].fillna(0.0) * df_joined.loc[mask_sale, 'pct_fraction']
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
    'geo': df_joined.get('geo', 'ksa'),
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
