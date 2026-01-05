import pandas as pd
from datetime import datetime, timedelta
import os
import re

# =======================
# CONFIG
# =======================
days_back = 50
OFFER_ID = 1283
GEO = "ksa"
STATUS_DEFAULT = "pending"
DEFAULT_PCT_IF_MISSING = 0.0  # fraction fallback when percent missing (0.30 == 30%)

# Local files
AFFILIATE_XLSX = "Offers Coupons.xlsx"   # multi-sheet Excel you uploaded
REPORT_PREFIX  = "Individual-Item-Report"  # any CSV starting with this will match

# Offer -> worksheet name mapping
OFFER_SHEET_BY_ID = {
    1283: "Adidas",
    # add others later if needed
}

# =======================
# PATHS
# =======================

script_dir = os.path.dirname(os.path.abspath(__file__))
input_dir = os.path.join(script_dir, '..', 'input data')
output_dir = os.path.join(script_dir, '..', 'output data')
os.makedirs(output_dir, exist_ok=True)

affiliate_xlsx_path = os.path.join(input_dir, AFFILIATE_XLSX)
output_file = os.path.join(output_dir, 'adidasssss_with_payout.csv')

# =======================
# HELPERS
# =======================

def pick_payout_column(cols_lower_map):
    """Priority for payout column: payout > new customer payout > old customer payout."""
    for candidate in ["payout", "new customer payout", "old customer payout"]: # Prioritizes newer column.
        if candidate in cols_lower_map:
            return cols_lower_map[candidate]
    return None

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
        """Remove non alpha-numeric characters."""
        if pd.isna(value):
            return set()
        # Why replace non-alphanumeric with space when new_tokens and old_tokens strings don't have spaces?
        text = ''.join(ch if ch.isalnum() else ' ' for ch in str(value).lower()) 
        return {tok for tok in text.split() if tok}

    for key in candidates: # Check if candidates match actual dataframe columns.
        actual = columns_map.get(key)
        if not actual:
            continue
        tokens_series = df[actual].apply(tokenize)
        
        # Checking if tokenenized data matches tokens in sets.
        is_new = tokens_series.apply(lambda toks: bool(toks & new_tokens))  
        is_old = tokens_series.apply(lambda toks: bool(toks & old_tokens))

        # Checking to see if data of column matches presumed data of sets 
        recognized = (is_new | is_old) & ~resolved
        if recognized.any():
            result.loc[recognized] = is_new.loc[recognized]
            resolved.loc[recognized] = True
        if resolved.all():
            break
    return result

def load_affiliate_mapping_from_xlsx(xlsx_path: str, offer_id: int) -> pd.DataFrame:
    """Return mapping with columns code_norm, affiliate_ID, type_norm, pct_new, pct_old, fixed_new, fixed_old."""
    sheet_name = OFFER_SHEET_BY_ID.get(offer_id)
    if not sheet_name:
        raise ValueError(f"No sheet mapping defined for offer {offer_id}. Please add it to OFFER_SHEET_BY_ID.")

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
    payout_col = pick_payout_column(cols_lower)
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

    type_norm = ( # Normalize type col data.
        df_sheet[type_col]
        .astype(str)
        .str.strip()
        .str.lower()
        .replace({'': None})
        .fillna('revenue')
    )

    def pct_from(values: pd.Series) -> pd.Series: # Get payout percentage
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

    if not candidates: # Lists every file in the input folder.
        available = [f for f in os.listdir(directory) if f.lower().endswith(".csv")]
        raise FileNotFoundError(
            f"No .csv file starting with '{prefix}' found in: {directory}\n"
            f"Available .csv files: {available}"
        )

    exact = [p for p in candidates if os.path.basename(p).lower() == (prefix_lower + ".csv")]
    # Returns if there is a file with exact name as prefix
    if exact:
        return exact[0]


    # Otherwise, returns most recent file.
    return max(candidates, key=os.path.getmtime)

def normalize_coupon(x: str) -> str:
    """Uppercase, trim, and take the first token if multiple codes separated by ; , or whitespace."""
    if pd.isna(x):
        return ""
    s = str(x).strip().upper()
    parts = re.split(r"[;,\s]+", s)
    return parts[0] if parts else s

# Find the changing-named report file dynamically
input_file = find_matching_csv(input_dir, REPORT_PREFIX)

# =======================
# LOAD MAIN REPORT
# =======================
df = pd.read_csv(input_file, skiprows=range(4))

# Convert 'Transaction Date' to datetime, drop NaT
df['Transaction Date'] = pd.to_datetime(df['Transaction Date'], format='%m/%d/%y', errors='coerce')
df = df.dropna(subset=['Transaction Date'])

end_date = datetime.now().date()
start_date = end_date - timedelta(days=days_back)
today = datetime.now().date()

# Filter for Adidas KSA within range, excluding current day
df_filtered = df[
    (df['Advertiser Name'] == 'Adidas KSA') &
    (df['Transaction Date'].dt.date >= start_date) &
    (df['Transaction Date'].dt.date < today)
]

# Split rows with # of Items > 1 into per-item rows
split_rows = []
for _, row in df_filtered.iterrows():
    items = int(row['# of Items']) if pd.notnull(row['# of Items']) else 1 # If null assign 'items' with 1
    total_sales = float(row['Sales']) if pd.notnull(row['Sales']) else 0.0 # If null assign 'sales' with 0
    sales_per_item = (total_sales / items) if items > 0 else 0.0 # TODO: redundant ternary operation ('items' is inherently > 0)
    for _ in range(items):
        split_rows.append({
            'Order Coupon Code(s)': row.get('Order Coupon Code(s)', ''),
            'Transaction Date': row['Transaction Date'],
            'Sales': sales_per_item,
            '# of Items': 1
        })

df_split = pd.DataFrame(split_rows)

# Compute sale_amount and revenue
df_split['sale_amount'] = df_split['Sales'] * 1.17
df_split['revenue'] = df_split['sale_amount'] * 0.07

# Normalize coupon for joining
df_split['coupon_norm'] = df_split['Order Coupon Code(s)'].apply(normalize_coupon)

# =======================
# JOIN AFFILIATE MAPPING (type-aware)
# =======================
map_df = load_affiliate_mapping_from_xlsx(affiliate_xlsx_path, OFFER_ID)
df_joined = df_split.merge(map_df, how="left", left_on="coupon_norm", right_on="code_norm")

# Ensure required fields exist and derive effective payouts
df_joined['affiliate_ID'] = df_joined['affiliate_ID'].fillna("").astype(str).str.strip()
df_joined['type_norm'] = df_joined['type_norm'].fillna("revenue")
for col in ['pct_new', 'pct_old']:
    df_joined[col] = pd.to_numeric(df_joined.get(col), errors='coerce').fillna(DEFAULT_PCT_IF_MISSING)
for col in ['fixed_new', 'fixed_old']:
    df_joined[col] = pd.to_numeric(df_joined.get(col), errors='coerce')
is_new_customer = infer_is_new_customer(df_joined)
pct_effective = df_joined['pct_new'].where(is_new_customer, df_joined['pct_old'])
df_joined['pct_fraction'] = pd.to_numeric(pct_effective, errors='coerce').fillna(DEFAULT_PCT_IF_MISSING)
fixed_effective = df_joined['fixed_new'].where(is_new_customer, df_joined['fixed_old'])
df_joined['fixed_amount'] = pd.to_numeric(fixed_effective, errors='coerce')

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

# Force payout = 0 when affiliate_id is missing/empty
mask_no_aff = (df_joined['affiliate_ID'] == "")
payout.loc[mask_no_aff] = 0.0

df_joined['payout'] = payout.round(2)

# =======================
# BUILD FINAL OUTPUT (NEW STRUCTURE)
# =======================
output_df = pd.DataFrame({
    'offer': OFFER_ID,
    'affiliate_id': df_joined['affiliate_ID'],
    'date': df_joined['Transaction Date'].dt.strftime('%m-%d-%Y'),
    'status': STATUS_DEFAULT,
    'payout': df_joined['payout'],
    'revenue': df_joined['revenue'].round(2),
    'sale amount': df_joined['sale_amount'].round(2),
    'coupon': df_joined['coupon_norm'],
    'geo': GEO,
})

# Save
output_df.to_csv(output_file, index=False)

print(f"Using report file: {input_file}")
print(f"Saved: {output_file}")
print(
    f"Rows: {len(output_df)} | "
    f"Coupons without affiliate_id (payout forced to 0): {int(mask_no_aff.sum())} | "
    f"Type counts -> revenue: {int(mask_rev.sum())}, sale: {int(mask_sale.sum())}, fixed: {int(mask_fixed.sum())}"
)