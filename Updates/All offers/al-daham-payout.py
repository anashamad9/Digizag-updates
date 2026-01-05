import pandas as pd
from datetime import datetime, timedelta
import os
import re

# =======================
# CONFIG (AL DAHAM)
# =======================
<<<<<<< HEAD
days_back = 12
=======
days_back = 28
>>>>>>> 0d89299 (D)
OFFER_ID = 1353
STATUS_DEFAULT = "pending"
FALLBACK_AFFILIATE_ID = "1"

REPORT_PREFIX   = "DigiZag Coupon Tracking(Coupon Performance)"
AFFILIATE_XLSX  = "Offers Coupons.xlsx"
AFFILIATE_SHEET = "Al Daham"
OUTPUT_CSV      = "al_daham.csv"

SAR_TO_USD_DIVISOR = 3.75
REVENUE_RATE = 0.06

# =======================
# PATHS
# =======================
script_dir = os.path.dirname(os.path.abspath(__file__))
input_dir  = os.path.join(script_dir, '..', 'input data')
output_dir = os.path.join(script_dir, '..', 'output data')
os.makedirs(output_dir, exist_ok=True)

affiliate_xlsx_path = os.path.join(input_dir, AFFILIATE_XLSX)
output_file = os.path.join(output_dir, OUTPUT_CSV)

# =======================
# DATE WINDOW
# =======================
end_date = datetime.now().date()
start_date = end_date - timedelta(days=days_back)
print(f"Current date: {end_date}, Start date (days_back={days_back}): {start_date}")

# =======================
# HELPERS
# =======================
def find_matching_csv(directory: str, prefix: str) -> str:
    """Return the newest CSV whose base filename starts with `prefix` (case-insensitive)."""
    prefix_lower = prefix.lower()
    candidates = []
    for fname in os.listdir(directory):
        if fname.startswith("~$"):
            continue
        if not fname.lower().endswith('.csv'):
            continue
        base = os.path.splitext(fname)[0].lower()
        if base.startswith(prefix_lower):
            candidates.append(os.path.join(directory, fname))

    if not candidates:
        available = [f for f in os.listdir(directory) if f.lower().endswith('.csv')]
        raise FileNotFoundError(
            f"No .csv file starting with '{prefix}' found in: {directory}\n"
            f"Available .csv files: {available}"
        )

    exact = [p for p in candidates if os.path.basename(p).lower() == (prefix_lower + '.csv')]
    if exact:
        return exact[0]

    return max(candidates, key=os.path.getmtime)


def normalize_coupon(code: str) -> str:
    """Uppercase, trim, and take the first token (handles separators and NBSP)."""
    if pd.isna(code):
        return ""
    s = str(code).replace(' ', ' ').strip().upper()
    parts = re.split(r"[;,\s]+", s)
    return parts[0] if parts else s


def load_affiliate_mapping_from_xlsx(xlsx_path: str, sheet_name: str) -> pd.DataFrame:
    """Load coupon → affiliate id mapping from the Offers Coupons workbook."""
    df_sheet = pd.read_excel(xlsx_path, sheet_name=sheet_name, dtype=str)
    cols_lower = {str(c).lower().strip(): c for c in df_sheet.columns}

    code_col = cols_lower.get('code') or cols_lower.get('coupon code') or cols_lower.get('coupon')
    aff_col = cols_lower.get('id') or cols_lower.get('affiliate_id') or cols_lower.get('affiliate id')

    if not code_col:
        raise ValueError(f"[{sheet_name}] must contain a 'Code' column.")
    if not aff_col:
        raise ValueError(f"[{sheet_name}] must contain an 'ID' (or 'affiliate_ID') column.")

    out = pd.DataFrame({
        'code_norm': df_sheet[code_col].apply(normalize_coupon),
        'affiliate_ID': df_sheet[aff_col].fillna('').astype(str).str.strip(),
    })
    out = out[out['code_norm'].astype(str).str.len() > 0]
    out = out[out['affiliate_ID'].astype(str).str.len() > 0]

    return out.drop_duplicates(subset=['code_norm'], keep='last')


def to_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(
        series.astype(str)
              .str.replace(',', '', regex=False)
              .str.replace('SAR', '', regex=False)
              .str.replace('sar', '', regex=False)
              .str.replace('$', '', regex=False)
              .str.strip(),
        errors='coerce'
    )


# =======================
# LOAD REPORT
# =======================
input_file = find_matching_csv(input_dir, REPORT_PREFIX)
print(f"Using report file: {input_file}")

df = pd.read_csv(input_file)
if df.empty:
    df = pd.DataFrame(columns=['Date', 'Coupon Code', 'sale_amount', 'coupon_norm'])
else:
    df.columns = [str(c).strip() for c in df.columns]

    date_col = 'Date'
    coupon_col = 'Coupon Code'
    amount_col = None
    for col in df.columns:
        if col.lower().startswith('amount'):
            amount_col = col
            break

    required = [date_col, coupon_col, amount_col]
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in input CSV: {missing}")

    df[date_col] = pd.to_datetime(df[date_col], dayfirst=True, errors='coerce')
    before = len(df)
    df = df.dropna(subset=[date_col])
    print(f"Total rows before filtering: {before}")
    print(f"Rows with invalid dates dropped: {before - len(df)}")

    df = df[(df[date_col].dt.date >= start_date) & (df[date_col].dt.date <= end_date)].copy()
    print(f"Rows after filtering date range: {len(df)}")

    df['coupon_norm'] = df[coupon_col].apply(normalize_coupon)
    df['sale_amount'] = to_numeric(df[amount_col]).fillna(0.0) / SAR_TO_USD_DIVISOR

# =======================
# AFFILIATE MAPPING JOIN
# =======================
map_df = load_affiliate_mapping_from_xlsx(affiliate_xlsx_path, AFFILIATE_SHEET)

df_joined = df.merge(map_df, how='left', left_on='coupon_norm', right_on='code_norm') if not df.empty else df.assign(affiliate_ID=pd.NA)
missing_aff_mask = df_joined['affiliate_ID'].isna() | (df_joined['affiliate_ID'].astype(str).str.strip() == '')

if missing_aff_mask.any():
    sample = (
        df_joined.loc[missing_aff_mask, ['coupon_norm']]
        .drop_duplicates()
        .head(10)
        .to_dict(orient='records')
    )
    print("Coupons without affiliate mapping:", sample)

if df_joined.empty:
    df_joined['affiliate_ID'] = FALLBACK_AFFILIATE_ID
    df_joined['sale_amount'] = 0.0
else:
    df_joined.loc[missing_aff_mask, 'affiliate_ID'] = FALLBACK_AFFILIATE_ID
    df_joined.loc[missing_aff_mask, 'sale_amount'] = 0.0

# Ensure columns exist when df was empty
if df_joined.empty:
    df_joined = pd.DataFrame({
        'coupon_norm': [],
        'affiliate_ID': [],
        'payout': [],
        'revenue': [],
        'sale_amount': [],
        'Date': []
    })
else:
    pass  # df_joined already populated

sale_amount_series = df_joined.get('sale_amount', pd.Series(index=df_joined.index, dtype=float))
sale_amount_series = pd.to_numeric(sale_amount_series, errors='coerce').reindex(df_joined.index, fill_value=0.0)
sale_amount_series = sale_amount_series.fillna(0.0).round(2)
df_joined['sale_amount'] = sale_amount_series
df_joined['revenue'] = (sale_amount_series * REVENUE_RATE).round(2)
df_joined['payout'] = df_joined['revenue']

# =======================
# BUILD OUTPUT
# =======================
date_col = 'Date'
output_df = pd.DataFrame({
    'offer': OFFER_ID,
    'affiliate_id': df_joined['affiliate_ID'].astype(str).str.strip(),
    'date': pd.to_datetime(df_joined[date_col], errors='coerce').dt.strftime('%m-%d-%Y'),
    'status': STATUS_DEFAULT,
    'payout': df_joined.get('payout', pd.Series(dtype=float)),
    'revenue': df_joined.get('revenue', pd.Series(dtype=float)),
    'sale amount': df_joined.get('sale_amount', pd.Series(dtype=float)),
    'coupon': df_joined['coupon_norm'] if 'coupon_norm' in df_joined else pd.Series(dtype=str),
    'geo': 'no-geo',
})

# =======================
# SAVE
# =======================
output_df.to_csv(output_file, index=False)

print(f"Saved: {output_file}")
print(f"Rows: {len(output_df)} | No-affiliate coupons (set aff={FALLBACK_AFFILIATE_ID}, payout=0): {int(missing_aff_mask.sum())}")
if not output_df.empty:
    print(f"Date range processed: {output_df['date'].min()} to {output_df['date'].max()}")
else:
    print("No rows after processing.")
