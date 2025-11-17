
import os
import re
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd

# =======================
# CONFIG (Rasees)
# =======================
OFFER_ID = 1163
GEO = "ksa"
STATUS_DEFAULT = "pending"
DEFAULT_PCT_IF_MISSING = 0.0
FALLBACK_AFFILIATE_ID = "1"
CURRENCY_DIVISOR = 3.75
NET_SALE_MULTIPLIER = 0.87
REVENUE_RATE = 0.13
DAYS_BACK = 10

REPORT_PREFIX = "Degi Zag - Daily Sales Report"
REPORT_SHEET = "Rasees"
AFFILIATE_XLSX = "Offers Coupons.xlsx"
AFFILIATE_SHEET = "Rasees"
OUTPUT_CSV = "rasees.csv"

# =======================
# PATHS
# =======================
script_dir = os.path.dirname(os.path.abspath(__file__))
input_dir = os.path.join(script_dir, "..", "input data")
output_dir = os.path.join(script_dir, "..", "output data")
os.makedirs(output_dir, exist_ok=True)

affiliate_xlsx_path = os.path.join(input_dir, AFFILIATE_XLSX)
output_file = os.path.join(output_dir, OUTPUT_CSV)

if not os.path.exists(affiliate_xlsx_path):
    raise FileNotFoundError(f"Affiliate sheet not found: {affiliate_xlsx_path}")

# =======================
# HELPERS
# =======================
def _norm(value: str) -> str:
    return re.sub(r"\s+", " ", str(value).strip()).lower()


def find_matching_xlsx(directory: str, prefix: str) -> str:
    prefix_n = _norm(prefix)
    candidates = []
    for fname in os.listdir(directory):
        if fname.startswith("~$"):
            continue
        if not fname.lower().endswith(".xlsx"):
            continue
        base = os.path.splitext(fname)[0]
        if _norm(base).startswith(prefix_n):
            full = os.path.join(directory, fname)
            if os.path.isfile(full):
                candidates.append(full)
    if not candidates:
        available = [f for f in os.listdir(directory) if f.lower().endswith('.xlsx')]
        raise FileNotFoundError(
            f"No .xlsx starting with '{prefix}' in: {directory}\nAvailable: {available}"
        )
    candidates.sort(key=os.path.getmtime, reverse=True)
    return candidates[0]


def normalize_coupon(x: str) -> str:
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

    def extract_numeric(col_name: Optional[str]) -> pd.Series:
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


def resolve_secondary_column(df: pd.DataFrame, base_name: str) -> str:
    preferred = f"{base_name}.1"
    if preferred in df.columns:
        return preferred
    base_low = base_name.strip().lower()
    matches = [col for col in df.columns if str(col).strip().lower().startswith(base_low)]
    if not matches:
        raise KeyError(f"Could not find column for '{base_name}' in sheet: {df.columns.tolist()}")
    return matches[-1]


def parse_created_at(series: pd.Series) -> pd.Series:
    def _parse(value):
        if pd.isna(value):
            return pd.NaT
        text = str(value).strip()
        if not text:
            return pd.NaT
        for fmt in ("%y-%m-%d %H:%M", "%y-%m-%d %H:%M:%S"):
            try:
                return datetime.strptime(text, fmt)
            except ValueError:
                continue
        return pd.to_datetime(text, errors='coerce', dayfirst=False)

    return series.apply(_parse)


def prepare_orders(df: pd.DataFrame) -> pd.DataFrame:
    coupon_col = resolve_secondary_column(df, 'Coupon')
    date_col = resolve_secondary_column(df, 'Created_at')
    amount_col = resolve_secondary_column(df, 'Amount')

    subset = df[[coupon_col, date_col, amount_col]].copy()
    subset.columns = ['coupon_raw', 'created_at', 'amount']

    subset['coupon_raw'] = subset['coupon_raw'].astype(str).str.strip()
    subset['amount'] = pd.to_numeric(subset['amount'], errors='coerce').fillna(0.0)
    subset = subset[(subset['coupon_raw'] != '') & (subset['amount'] > 0)]

    subset['created_at'] = parse_created_at(subset['created_at'])
    subset = subset.dropna(subset=['created_at'])

    subset['coupon_norm'] = subset['coupon_raw'].apply(normalize_coupon)
    subset = subset[subset['coupon_norm'] != '']

    subset['sale_amount'] = (subset['amount'] * NET_SALE_MULTIPLIER) / CURRENCY_DIVISOR
    subset['revenue'] = subset['sale_amount'] * REVENUE_RATE

    subset['order_date'] = subset['created_at'].dt.date
    return subset


# =======================
# LOAD REPORT
# =======================
today = datetime.now().date()
start_date = today - timedelta(days=DAYS_BACK)
print(f"Running Rasees (Offer {OFFER_ID}) at {today} | window starts: {start_date}")

report_path = find_matching_xlsx(input_dir, REPORT_PREFIX)
print(f"Using report file: {os.path.basename(report_path)} | sheet='{REPORT_SHEET}'")

sheet_df = pd.read_excel(report_path, sheet_name=REPORT_SHEET)
orders_df = prepare_orders(sheet_df)

orders_df = orders_df[orders_df['order_date'] >= start_date]
print(f"Orders within window: {len(orders_df)}")

if orders_df.empty:
    output_df = pd.DataFrame(columns=['offer','affiliate_id','date','status','payout','revenue','sale amount','coupon','geo'])
else:
    map_df = load_affiliate_mapping_from_xlsx(affiliate_xlsx_path, AFFILIATE_SHEET)
    dfj = orders_df.merge(map_df, how='left', left_on='coupon_norm', right_on='code_norm')

    dfj['affiliate_ID'] = dfj['affiliate_ID'].fillna('').astype(str).str.strip()
    dfj['type_norm'] = dfj['type_norm'].fillna('revenue')

    for col in ['pct_new', 'pct_old']:
        dfj[col] = pd.to_numeric(dfj.get(col), errors='coerce').fillna(DEFAULT_PCT_IF_MISSING)
    for col in ['fixed_new', 'fixed_old']:
        dfj[col] = pd.to_numeric(dfj.get(col), errors='coerce')

    is_new_customer = infer_is_new_customer(dfj)
    pct_effective = dfj['pct_new'].where(is_new_customer, dfj['pct_old'])
    dfj['pct_fraction'] = pd.to_numeric(pct_effective, errors='coerce').fillna(DEFAULT_PCT_IF_MISSING)
    fixed_effective = dfj['fixed_new'].where(is_new_customer, dfj['fixed_old'])
    dfj['fixed_amount'] = pd.to_numeric(fixed_effective, errors='coerce')

    payout = pd.Series(0.0, index=dfj.index)
    mask_rev = dfj['type_norm'].str.lower().eq('revenue')
    mask_sale = dfj['type_norm'].str.lower().eq('sale')
    mask_fixed = dfj['type_norm'].str.lower().eq('fixed')

    payout.loc[mask_rev] = dfj.loc[mask_rev, 'revenue'] * dfj.loc[mask_rev, 'pct_fraction']
    payout.loc[mask_sale] = dfj.loc[mask_sale, 'sale_amount'] * dfj.loc[mask_sale, 'pct_fraction']
    payout.loc[mask_fixed] = dfj.loc[mask_fixed, 'fixed_amount'].fillna(0.0)

    mask_no_aff = dfj['affiliate_ID'].astype(str).str.strip().eq('')
    payout.loc[mask_no_aff] = 0.0
    dfj.loc[mask_no_aff, 'affiliate_ID'] = FALLBACK_AFFILIATE_ID

    dfj['payout'] = payout.round(2)
    dfj['revenue'] = dfj['revenue'].round(2)
    dfj['sale_amount'] = dfj['sale_amount'].round(2)

    output_df = pd.DataFrame({
        'offer': OFFER_ID,
        'affiliate_id': dfj['affiliate_ID'],
        'date': pd.to_datetime(dfj['order_date']).dt.strftime('%m-%d-%Y'),
        'status': STATUS_DEFAULT,
        'payout': dfj['payout'],
        'revenue': dfj['revenue'],
        'sale amount': dfj['sale_amount'],
        'coupon': dfj['coupon_norm'],
        'geo': GEO,
    })

output_df.to_csv(output_file, index=False)

print(f"Saved: {output_file}")
print(f"Rows exported: {len(output_df)}")
