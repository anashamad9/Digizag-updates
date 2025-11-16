
import os
import re
from datetime import date, datetime, timedelta
from typing import Optional

import pandas as pd

# =======================
# CONFIG (Grasse Matrix)
# =======================
OFFER_ID = 1346
GEO = "no-geo"
STATUS_DEFAULT = "pending"
DEFAULT_PCT_IF_MISSING = 0.0
FALLBACK_AFFILIATE_ID = "1"
CURRENCY_DIVISOR = 3.75

# Choose how many days back to include
DAYS_BACK = 30

# Files
REPORT_PREFIX = "Grasse x Digizag"
REPORT_SHEET = "Report"
AFFILIATE_XLSX = "Offers Coupons.xlsx"
AFFILIATE_SHEET = "GRASSE PERFUME"
OUTPUT_CSV = "grasse.csv"

# =======================
# PATHS
# =======================
script_dir = os.path.dirname(os.path.abspath(__file__))
input_dir = os.path.join(script_dir, "..", "input data")
output_dir = os.path.join(script_dir, "..", "output data")
os.makedirs(output_dir, exist_ok=True)

affiliate_xlsx_path = os.path.join(input_dir, AFFILIATE_XLSX)
output_file = os.path.join(output_dir, OUTPUT_CSV)

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
            full_path = os.path.join(directory, fname)
            if os.path.isfile(full_path):
                candidates.append(full_path)
    if not candidates:
        available = [f for f in os.listdir(directory) if f.lower().endswith('.xlsx')]
        raise FileNotFoundError(
            f"No .xlsx starting with '{prefix}' in: {directory}\nAvailable: {available}"
        )
    return max(candidates, key=os.path.getmtime)


def normalize_coupon(value: str) -> str:
    if pd.isna(value):
        return ""
    s = str(value).strip().upper()
    parts = re.split(r"[;,\s]+", s)
    return parts[0] if parts else s


def infer_is_new_customer(df: pd.DataFrame) -> pd.Series:
    """Infer a boolean new-customer flag from common columns; default False when no signal."""
    if df.empty:
        return pd.Series(False, index=df.index, dtype=bool)

    candidates = [
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


ARABIC_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")
MONTH_ALIASES = {
    'يناير': 1,
    'كانون الثاني': 1,
    'فبراير': 2,
    'شباط': 2,
    'مارس': 3,
    'آذار': 3,
    'ابريل': 4,
    'أبريل': 4,
    'نيسان': 4,
    'مايو': 5,
    'أيار': 5,
    'يونيو': 6,
    'حزيران': 6,
    'يوليو': 7,
    'تموز': 7,
    'اغسطس': 8,
    'أغسطس': 8,
    'آب': 8,
    'سبتمبر': 9,
    'اكتوبر': 10,
    'أكتوبر': 10,
    'تشرين الاول': 10,
    'نوفمبر': 11,
    'تشرين الثاني': 11,
    'ديسمبر': 12,
    'كانون الاول': 12,
    'jan': 1,
    'january': 1,
    'feb': 2,
    'february': 2,
    'mar': 3,
    'march': 3,
    'apr': 4,
    'april': 4,
    'may': 5,
    'jun': 6,
    'june': 6,
    'jul': 7,
    'july': 7,
    'aug': 8,
    'august': 8,
    'sep': 9,
    'sept': 9,
    'september': 9,
    'oct': 10,
    'october': 10,
    'nov': 11,
    'november': 11,
    'dec': 12,
    'december': 12,
}


def _normalize_arabic_letters(text: str) -> str:
    replacements = {
        'إ': 'ا',
        'أ': 'ا',
        'آ': 'ا',
        'ى': 'ي',
        'ؤ': 'و',
        'ئ': 'ي',
        'ة': 'ه',
    }
    for src, dst in replacements.items():
        text = text.replace(src, dst)
    return text


def parse_month_token(token: str) -> Optional[int]:
    if not token:
        return None
    cleaned = _normalize_arabic_letters(token.lower())
    cleaned = re.sub(r"[^a-zء-ي]+", '', cleaned)
    return MONTH_ALIASES.get(cleaned)


def parse_date_label(label, reference_date: date) -> Optional[date]:
    if pd.isna(label):
        return None
    text = str(label).strip()
    if not text:
        return None
    normalized = _normalize_arabic_letters(text)
    normalized = normalized.translate(ARABIC_DIGITS)
    tokens = normalized.split()
    if len(tokens) >= 2 and tokens[0].isdigit():
        day = int(tokens[0])
        month = parse_month_token(tokens[1])
        if month:
            year = reference_date.year
            if month - reference_date.month > 6:
                year -= 1
            elif reference_date.month - month > 6:
                year += 1
            try:
                return date(year, month, day)
            except ValueError:
                return None
    dt = pd.to_datetime(normalized, errors='coerce', dayfirst=True)
    return None if pd.isna(dt) else dt.date()


def reshape_matrix(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if 'DATE' not in df.columns or 'CODE' not in df.columns:
        raise KeyError("Expected columns 'DATE' and 'CODE' in report sheet")
    df['DATE'] = df['DATE'].ffill()
    df = df[df['CODE'].notna()]
    df['row_type'] = df['CODE'].astype(str).str.strip().str.lower()
    df = df[df['row_type'].isin({'order', 'order value', 'commission'})]

    coupon_columns = [c for c in df.columns if str(c).strip().lower() not in {'date', 'code', 'row_type'}]
    if not coupon_columns:
        raise ValueError('No coupon columns detected in report sheet')

    melted = df.melt(
        id_vars=['DATE', 'row_type'],
        value_vars=coupon_columns,
        var_name='coupon_raw',
        value_name='raw_value'
    )
    melted['coupon_raw'] = melted['coupon_raw'].astype(str).str.strip()
    melted = melted[melted['coupon_raw'] != '']
    melted['value'] = pd.to_numeric(melted['raw_value'], errors='coerce').fillna(0.0)

    pivot = (
        melted.pivot_table(
            index=['DATE', 'coupon_raw'],
            columns='row_type',
            values='value',
            aggfunc='sum',
            fill_value=0.0
        )
        .reset_index()
    )
    pivot.columns.name = None
    pivot = pivot.rename(columns={
        'order': 'orders',
        'order value': 'sale_total',
        'commission': 'revenue_total',
    })
    for col in ['orders', 'sale_total', 'revenue_total']:
        if col not in pivot:
            pivot[col] = 0.0
    return pivot


# =======================
# LOAD REPORT
# =======================
today = datetime.now().date()
start_date = today - timedelta(days=DAYS_BACK)
yesterday = today - timedelta(days=1)
print(f"Running Grasse Matrix (Offer {OFFER_ID}) at {today} | window: {start_date} to {yesterday}")

report_path = find_matching_xlsx(input_dir, REPORT_PREFIX)
report_mtime = datetime.fromtimestamp(os.path.getmtime(report_path)).date()
print(f"Using report file: {os.path.basename(report_path)} | sheet='{REPORT_SHEET}'")

report_df = pd.read_excel(report_path, sheet_name=REPORT_SHEET)
matrix_df = reshape_matrix(report_df)
matrix_df['date_value'] = matrix_df['DATE'].apply(lambda v: parse_date_label(v, report_mtime))
matrix_df = matrix_df.dropna(subset=['date_value'])

mask_window = (matrix_df['date_value'] >= start_date) & (matrix_df['date_value'] <= yesterday)
matrix_df = matrix_df[mask_window]
print(f"Coupons/date combinations within window: {len(matrix_df)}")

if not matrix_df.empty:
    matrix_df['orders'] = pd.to_numeric(matrix_df['orders'], errors='coerce').fillna(0.0)
    matrix_df['sale_total'] = pd.to_numeric(matrix_df['sale_total'], errors='coerce').fillna(0.0)
    matrix_df['revenue_total'] = pd.to_numeric(matrix_df['revenue_total'], errors='coerce').fillna(0.0)

    matrix_df['orders_int'] = matrix_df['orders'].round().astype(int).clip(lower=0)
    matrix_df = matrix_df[matrix_df['orders_int'] > 0]

    matrix_df['sale_converted'] = matrix_df['sale_total'] / CURRENCY_DIVISOR
    matrix_df = matrix_df[matrix_df['sale_converted'] > 0]

    matrix_df['revenue_converted'] = matrix_df['revenue_total'] / CURRENCY_DIVISOR
    matrix_df['sale_per_order'] = matrix_df['sale_converted'] / matrix_df['orders_int']
    matrix_df['revenue_per_order'] = matrix_df['revenue_converted'] / matrix_df['orders_int']
    matrix_df['coupon_norm'] = matrix_df['coupon_raw'].apply(normalize_coupon)

    expanded = matrix_df.loc[matrix_df.index.repeat(matrix_df['orders_int'])].copy()
    expanded['sale_amount'] = matrix_df['sale_per_order'].reindex(expanded.index).values
    expanded['revenue'] = matrix_df['revenue_per_order'].reindex(expanded.index).values
    expanded['coupon_norm'] = matrix_df['coupon_norm'].reindex(expanded.index).values
    expanded['date_value'] = matrix_df['date_value'].reindex(expanded.index).values
else:
    expanded = pd.DataFrame(columns=['coupon_norm', 'sale_amount', 'revenue', 'date_value'])

if expanded.empty:
    output_df = pd.DataFrame(columns=['offer','affiliate_id','date','status','payout','revenue','sale amount','coupon','geo'])
else:
    map_df = load_affiliate_mapping_from_xlsx(affiliate_xlsx_path, AFFILIATE_SHEET)
    dfj = expanded.merge(map_df, how='left', left_on='coupon_norm', right_on='code_norm')

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
    payout.loc[mask_rev] = dfj.loc[mask_rev, 'revenue'] * dfj.loc[mask_rev, 'pct_fraction']
    mask_sale = dfj['type_norm'].str.lower().eq('sale')
    payout.loc[mask_sale] = dfj.loc[mask_sale, 'sale_amount'] * dfj.loc[mask_sale, 'pct_fraction']
    mask_fixed = dfj['type_norm'].str.lower().eq('fixed')
    payout.loc[mask_fixed] = dfj.loc[mask_fixed, 'fixed_amount'].fillna(0.0)

    mask_no_aff = dfj['affiliate_ID'].astype(str).str.strip().eq('')
    payout.loc[mask_no_aff] = 0.0
    dfj.loc[mask_no_aff, 'affiliate_ID'] = FALLBACK_AFFILIATE_ID

    dfj['payout'] = payout.round(2)
    dfj['revenue'] = dfj['revenue'].round(2)
    dfj['sale_amount'] = dfj['sale_amount'].round(2)

    date_series = pd.to_datetime(dfj['date_value'], errors='coerce')
    dfj['date_str'] = date_series.dt.strftime('%m-%d-%Y')

    output_df = pd.DataFrame({
        'offer': OFFER_ID,
        'affiliate_id': dfj['affiliate_ID'],
        'date': dfj['date_str'],
        'status': STATUS_DEFAULT,
        'payout': dfj['payout'],
        'revenue': dfj['revenue'],
        'sale amount': dfj['sale_amount'],
        'coupon': dfj['coupon_norm'],
        'geo': GEO,
    })

output_df.to_csv(output_file, index=False)

print(f"Saved: {output_file}")
print(f"Expanded orders: {len(expanded)} | Output rows: {len(output_df)}")
