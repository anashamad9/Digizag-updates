import os
import pandas as pd
from datetime import datetime, timedelta

# =======================
# CONFIG (Noon Links)
# =======================
days_back = 4
OFFER_ID = 1355
STATUS_DEFAULT = "pending"
FALLBACK_AFFILIATE_ID = "1"

REPORT_PREFIX   = "Digizag UTM DASHBOARD_ORDERS_Table"
AFFILIATE_XLSX  = "Offers Coupons.xlsx"
AFFILIATE_SHEETS = ["Noon Gcc Links", "Noon GCC"]
OUTPUT_CSV      = "noon_links.csv"
AED_TO_USD_DIVISOR = 3.75
HISTORY_FILE = "noon_links_history.csv"

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
    prefix_lower = prefix.lower()
    candidates = []
    for fname in os.listdir(directory):
        if fname.startswith('~$') or not fname.lower().endswith('.csv'):
            continue
        if os.path.splitext(fname)[0].lower().startswith(prefix_lower):
            candidates.append(os.path.join(directory, fname))
    if not candidates:
        available = [f for f in os.listdir(directory) if f.lower().endswith('.csv')]
        raise FileNotFoundError(
            f"No .csv file starting with '{prefix}' found in: {directory}\nAvailable .csv files: {available}"
        )
    exact = [p for p in candidates if os.path.basename(p).lower() == (prefix_lower + '.csv')]
    if exact:
        return exact[0]
    return max(candidates, key=os.path.getmtime)


def normalize_unique_id(value) -> str:
    if pd.isna(value):
        return ''
    try:
        return str(int(float(value)))
    except (TypeError, ValueError):
        return str(value).strip()


def load_affiliate_mapping(xlsx_path: str, sheets: list[str]) -> pd.DataFrame:
    frames = []
    for sheet in sheets:
        try:
            df_sheet = pd.read_excel(xlsx_path, sheet_name=sheet, dtype=str)
        except Exception as exc:
            print(f"Warning: skipped sheet '{sheet}': {exc}")
            continue
        cols_lower = {str(c).lower().strip(): c for c in df_sheet.columns}
        id_col = cols_lower.get('id')
        type_col = cols_lower.get('type')
        payout_col = cols_lower.get('new customer payout')
        if not id_col or not payout_col:
            continue
        sheet_df = df_sheet.copy()
        if type_col in sheet_df.columns:
            sheet_df = sheet_df[sheet_df[type_col].astype(str).str.strip().str.lower().eq('revenue')]
        
        temp = pd.DataFrame({
            'affiliate_ID': sheet_df[id_col].apply(normalize_unique_id),
            'pct_fraction': pd.to_numeric(
                sheet_df[payout_col].astype(str).str.replace('%','',regex=False).str.strip(),
                errors='coerce'
            )
        })
        frames.append(temp)
    if not frames:
        return pd.DataFrame(columns=['affiliate_ID','pct_fraction'])
    out = pd.concat(frames, ignore_index=True)
    out = out[out['affiliate_ID'].astype(str).str.len() > 0]
    # Normalize percent numbers: 80 -> 0.80, 0.8 -> 0.8
    out['pct_fraction'] = out['pct_fraction'].apply(lambda v: (v/100.0) if pd.notna(v) and v > 1 else (v if pd.notna(v) else 0.0))
    return out.drop_duplicates(subset=['affiliate_ID'], keep='first')


def to_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors='coerce')


def tier_revenue_for_order_value(order_value_aed: float) -> float:
    """Order value in AED -> fixed revenue dollars.

    Tiers:
    - <= 200 AED  -> $2.50
    - <= 400 AED  -> $6.25
    - >  400 AED  -> $9.00
    """
    if order_value_aed <= 200:
        return 2.50
    if order_value_aed <= 400:
        return 6.25
    return 9.00


# =======================
# LOAD REPORT
# =======================
input_file = find_matching_csv(input_dir, REPORT_PREFIX)
print(f"Using report file: {input_file}")

df = pd.read_csv(input_file)
if df.empty:
    print('Input CSV is empty; output will be empty.')

df.columns = [str(c).strip() for c in df.columns]

required_cols = ['EVENT DATE', 'UNIQUE ID', 'ORDER NUMBER', 'ORDER VALUE']
missing_cols = [col for col in required_cols if col not in df.columns]
if missing_cols:
    raise ValueError(f"CSV is missing required columns: {missing_cols}")

df['EVENT DATE'] = pd.to_datetime(df['EVENT DATE'], format='%b %d, %Y', errors='coerce')

before = len(df)
df = df.dropna(subset=['EVENT DATE'])
print(f"Rows with invalid dates dropped: {before - len(df)}")

df = df[(df['EVENT DATE'].dt.date >= start_date) & (df['EVENT DATE'].dt.date <= end_date)].copy()
print(f"Rows after filtering date range: {len(df)}")

valid_mask = df['ORDER NUMBER'].notna() & df['ORDER VALUE'].notna()
df = df[valid_mask].copy()
print(f"Rows after dropping null order info: {len(df)}")

if df.empty:
    output_df = pd.DataFrame(columns=['offer','affiliate_id','date','status','payout','revenue','sale amount','coupon','geo'])
    output_df.to_csv(output_file, index=False)
    print(f"Saved empty output: {output_file}")
    raise SystemExit(0)

# =======================
# TRANSFORMATIONS
# =======================
df['affiliate_ID'] = df['UNIQUE ID'].apply(normalize_unique_id)
df['order_value_aed'] = to_numeric(df['ORDER VALUE']).fillna(0.0)
df['sale_amount'] = df['order_value_aed'] / AED_TO_USD_DIVISOR

# Load or init history of processed orders to compute lifetime counts
history_path = os.path.join(output_dir, HISTORY_FILE)
if os.path.exists(history_path):
    hist = pd.read_csv(history_path, dtype=str)
    hist['affiliate_ID'] = hist['affiliate_ID'].astype(str)
    hist['order_number'] = hist['order_number'].astype(str)
else:
    hist = pd.DataFrame(columns=['affiliate_ID','order_number'])

# Normalize current order numbers
df['order_number'] = df['ORDER NUMBER'].astype(str).str.strip()

# Compute previous counts per affiliate
prev_counts = hist.groupby('affiliate_ID')['order_number'].nunique()
prev_counts = prev_counts.reindex(df['affiliate_ID'].unique(), fill_value=0)
prev_counts = prev_counts.to_dict()

# Assign sequential index within current batch per affiliate, ordered by date then order number
df_sorted = df.sort_values(['affiliate_ID','EVENT DATE','order_number']).copy()
df_sorted['batch_index'] = df_sorted.groupby('affiliate_ID').cumcount() + 1
df_sorted['lifetime_index'] = df_sorted.apply(lambda r: int(prev_counts.get(r['affiliate_ID'],0)) + int(r['batch_index']), axis=1)

# Revenue per order based on lifetime index
df_sorted['revenue_fixed'] = df_sorted['order_value_aed'].apply(tier_revenue_for_order_value)

# Bring back in same shape
df = df_sorted

# Map payout percent by affiliate ID from coupon sheets (type=revenue)
affiliate_map = load_affiliate_mapping(affiliate_xlsx_path, AFFILIATE_SHEETS)
df_joined = df.merge(affiliate_map, how='left', on='affiliate_ID')
df_joined['pct_fraction'] = pd.to_numeric(df_joined['pct_fraction'], errors='coerce').fillna(0.0)
missing_aff_mask = df_joined['pct_fraction'].eq(0.0)
if missing_aff_mask.any():
    sample = df_joined.loc[missing_aff_mask, ['affiliate_ID']].drop_duplicates().head(10).to_dict(orient='records')
    print('Affiliate IDs without payout mapping (pct=0):', sample)

# Compute payout = revenue_fixed * pct
df_joined['payout'] = (df_joined['revenue_fixed'] * df_joined['pct_fraction']).round(2)

# =======================
# BUILD OUTPUT
# =======================
output_df = pd.DataFrame({
    'offer': OFFER_ID,
    'affiliate_id': df_joined['affiliate_ID'].where(~missing_aff_mask, FALLBACK_AFFILIATE_ID),
    'date': df_joined['EVENT DATE'].dt.strftime('%m-%d-%Y'),
    'status': STATUS_DEFAULT,
    'payout': df_joined['payout'].round(2),
    'revenue': df_joined['revenue_fixed'].round(2),
    'sale amount': df_joined['sale_amount'].round(2),
    'coupon': 'link',
    'geo': 'no-geo',
})

# =======================
# SAVE
# =======================
output_df.to_csv(output_file, index=False)

print(f"Saved: {output_file}")
print(f"Rows: {len(output_df)} | Missing affiliate mappings set to default: {int(missing_aff_mask.sum())}")
if not output_df.empty:
    print(f"Date range processed: {output_df['date'].min()} to {output_df['date'].max()}")
else:
    print('No rows after processing.')

# Update history with newly seen orders
new_hist = df[['affiliate_ID','order_number']].drop_duplicates()
hist = pd.concat([hist, new_hist], ignore_index=True).drop_duplicates()
