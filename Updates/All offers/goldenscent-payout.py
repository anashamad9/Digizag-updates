import pandas as pd
from datetime import datetime, timedelta
import os
import re
import unicodedata
# =======================
# CONFIG
# =======================

OFFER_ID = 1333
STATUS_DEFAULT = "pending"
DEFAULT_PCT_IF_MISSING = 0.0
FALLBACK_AFFILIATE_ID = "1"
GEO = 'ksa'
USD_TO_SAR = 3.75

# Local files
AFFILIATE_XLSX   = "Offers Coupons.xlsx"
AFFILIATE_SHEET  = "Golden Scent"
# Latest dashboard export lives under this prefix (suffix like " (1).csv" still OK)
REPORT_PREFIX    = "Influencer_Agent"
OUTPUT_CSV       = "goldenscent.csv"

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

    # rev_col = cols_lower.get('total revenue')

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
    # payout_raw = extract_numeric(rev_col).fillna(payout_any)

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
    # pct_old = pct_from(payout_raw)
    pct_new = pct_new.fillna(pct_old)
    pct_old = pct_old.fillna(pct_new)

    fixed_new = fixed_from(payout_new_raw)
    fixed_old = fixed_from(payout_old_raw)
    fixed_new = fixed_new.fillna(fixed_old)
    fixed_old = fixed_old.fillna(fixed_new)

    # fixed = fixed_from(payout_raw)

    out = pd.DataFrame({
        'code_norm': df_sheet[code_col].apply(normalize_coupon),
        'affiliate_ID': df_sheet[aff_col].fillna('').astype(str).str.strip(),
        'type_norm': type_norm,
        'pct_new': pd.to_numeric(pct_new, errors='coerce').fillna(DEFAULT_PCT_IF_MISSING),
        'pct_old': pd.to_numeric(pct_old, errors='coerce').fillna(DEFAULT_PCT_IF_MISSING),
        'fixed_new': pd.to_numeric(fixed_new, errors='coerce'),
        'fixed_old': pd.to_numeric(fixed_old, errors='coerce'),
        # 'fixed': pd.to_numeric(fixed, errors='coerce')
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


input_file = find_latest_csv_by_prefix(input_dir, REPORT_PREFIX)
aff_file = load_affiliate_mapping_from_xlsx(affiliate_xlsx_path, AFFILIATE_SHEET)

df = pd.read_csv(input_file)
unwanted = ['Delivered Orders', 'PayOut - Gross Orders', 'PayOut - Del.Orders', 'Rev For New Cust.', 'Rev For Repeat Cust.', 'Agency Name', 'App URL']

df.drop(unwanted, axis=1, inplace=True)

print("Deleting Columns:\n", df.head(5))

df['Old Customers (Del. Orders)'] = df['Gross Orders'] - df['New Customers (Del. Orders)'] 

df['Coupon Code'] = df['Coupon Code'].apply(normalize_coupon)

df['Revenue (SAR)'] = df['Revenue (SAR)'] / USD_TO_SAR
df['Revenue (SAR)'] = df['Revenue (SAR)'].apply(round, 2)

new_df = pd.DataFrame({
    "Offer": pd.Series(OFFER_ID, dtype=str),
    "Code": pd.Series([], dtype=str),
    "Date": pd.NA,
    "Revenue": pd.Series([], dtype=float),
    "Sale Amount": pd.Series([], dtype=float)
})

# print(new_df)

i = 0

# print(df.info())

for _, row in df.iterrows():

    new_orders = row.get("New Customers (Del. Orders)")
    old_orders = row.get("Old Customers (Del. Orders)")
    sale_amount = row.get("Revenue (SAR)") / (new_orders + old_orders)

    for new_sale in range(new_orders):
        new_df.loc[i, 'Offer'] = OFFER_ID
        new_df.loc[i, 'Code'] = row.get('Coupon Code')
        new_df.loc[i, 'Revenue'] = 10
        new_df.loc[i, 'Sale Amount'] = sale_amount

        i+=1

    for old_sale in range(old_orders):
        new_df.loc[i, 'Offer'] = OFFER_ID
        new_df.loc[i, 'Code'] = row.get('Coupon Code')
        new_df.loc[i, 'Revenue'] = 5
        new_df.loc[i, 'Sale Amount'] = sale_amount

        i+=1

del df

new_df = new_df.merge(aff_file, how="left", left_on="Code", right_on = "code_norm")

new_df['Payout'] = new_df['Revenue'] * new_df['pct_new']

final_df = pd.DataFrame({
    'offer': OFFER_ID,
    'affiliate_id': new_df['affiliate_ID'],
    'date': pd.NA,
    'status': "Pending",
    'payout': new_df['Payout'],
    'revenue': new_df['Revenue'],
    'sale_amount': 0.0,
    'coupon': new_df['Code'],
    'geo': GEO
})

print(final_df.head(100))

final_df.to_csv(output_file, index = False)