import pandas as pd
from datetime import datetime, timedelta
import os
import re

# =======================
# CONFIG
# =======================
days_back = 20
STATUS_DEFAULT = "pending"
DEFAULT_PCT_IF_MISSING = 0.0
FALLBACK_AFFILIATE_ID = "1"

REPORT_PREFIX   = "Social Affiliate - digizag_Untitled Page_Pivot table"
AFFILIATE_XLSX  = "Offers Coupons.xlsx"
OUTPUT_CSV      = "shaye3_v2.csv"

# Brand -> Offer
brand_to_offer = {
    'vs': 1208,   # Victoria Secret
    'pk': 1250,   # Pottery Barn Kids
    'nb': 1161,   # New Balance
    'mc': 1146,   # Mothercare
    'hm': 1132,   # H&M
    'fl': 1160,   # Footlocker
    'bbw': 1130,  # Bath & Body Works
    'aeo': 1133,  # American Eagle
    'pb': 1176,   # Pottery Barn
    'wes': 1131   # WestELM
}

# Offer -> Sheet name in Offers Coupons.xlsx
offer_to_sheet = {
    1208: "Victoria Secret",
    1250: "Pottery Barn Kids",
    1161: "New Balance",
    1146: "Mothercare",
    1132: "H&M",
    1160: "Footlocker",
    1130: "Bath & Body Works",
    1133: "American Eagle",
    1176: "Pottery Barn",
    1131: "WestELM",
}

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
    if pd.isna(x):
        return ""
    s = str(x).replace("\u00A0", " ").strip().upper()
    parts = re.split(r"[;,\s]+", s)
    return parts[0] if parts else s

def normalize_brand(x) -> str:
    if pd.isna(x):
        return ""
    return str(x).replace("\u00A0", " ").strip().lower()

def load_affiliate_mapping_for_offer(xlsx_path: str, sheet_name: str, offer_id: int) -> pd.DataFrame:
    df_sheet = pd.read_excel(xlsx_path, sheet_name=sheet_name, dtype=str)
    cols_lower = {str(c).lower().strip(): c for c in df_sheet.columns}
    code_col = cols_lower.get("code") or cols_lower.get("coupon code") or cols_lower.get("coupon")
    aff_col  = cols_lower.get("id") or cols_lower.get("affiliate_id")
    type_col = cols_lower.get("type")
    payout_col = cols_lower.get("new customer payout")
    if not code_col or not aff_col or not payout_col:
        raise ValueError(f"[{sheet_name}] missing required columns (Code/ID/new customer payout)")
    df_eff = df_sheet
    if type_col:
        df_eff = df_sheet[df_sheet[type_col].astype(str).str.strip().str.lower().eq('revenue')]
    payout_num = pd.to_numeric(
        df_eff[payout_col].astype(str).str.replace('%','',regex=False).str.strip(),
        errors='coerce'
    )
    pct_fraction = payout_num.apply(lambda v: (v/100.0) if pd.notna(v) and v>1 else (v if pd.notna(v) else DEFAULT_PCT_IF_MISSING))
    out = pd.DataFrame({
        'offer': offer_id,
        'code_norm': df_eff[code_col].apply(normalize_coupon),
        'affiliate_ID': df_eff[aff_col].fillna('').astype(str).str.strip(),
        'pct_fraction': pct_fraction.fillna(DEFAULT_PCT_IF_MISSING)
    })
    out = out[out['code_norm'].astype(str).str.len() > 0]
    out['has_aff'] = out['affiliate_ID'].astype(str).str.len() > 0
    out = (
        out.sort_values(by=['offer','code_norm','has_aff'], ascending=[True,True,False])
           .drop_duplicates(subset=['offer','code_norm'], keep='first')
           .drop(columns=['has_aff'])
    )
    return out

def build_master_affiliate_map(xlsx_path: str, offer_to_sheet_map: dict) -> pd.DataFrame:
    frames = []
    for offer_id, sheet in offer_to_sheet_map.items():
        try:
            frames.append(load_affiliate_mapping_for_offer(xlsx_path, sheet, offer_id))
        except Exception as e:
            print(f"Warning: skipped sheet '{sheet}' for offer {offer_id}: {e}")
    if not frames:
        return pd.DataFrame(columns=['offer','code_norm','affiliate_ID','pct_fraction'])
    return pd.concat(frames, ignore_index=True)

# =======================
# LOAD REPORT
# =======================
input_file = find_matching_csv(input_dir, REPORT_PREFIX)
print(f"Using report file: {input_file}")
df = pd.read_csv(input_file)
df['Date'] = pd.to_datetime(df['Date'], format='%b %d, %Y', errors='coerce')
before_all = len(df)
df = df.dropna(subset=['Date'])
df = df[(df['Date'].dt.date >= start_date) & (df['Date'].dt.date <= end_date)].copy()
print(f"Total rows before filtering: {before_all}")
print(f"Rows after filtering date range: {len(df)}")

# CIR filter: keep <=39% or missing
cir_raw = df['CIR'].astype(str).str.replace('%','',regex=False).str.strip()
cir_num = pd.to_numeric(cir_raw, errors='coerce')
max_cir = cir_num.dropna().max()
threshold = 0.39 if pd.notna(max_cir) and max_cir <= 1.0 else 39.0
before_cir = len(df)
mask_ok = cir_num.isna() | (cir_num <= threshold)
df = df[mask_ok].copy()
print(f"Rows removed due to CIR > {int(threshold*100) if threshold<1 else int(threshold)}%: {before_cir - len(df)}")

# Normalize columns
df['brand_norm'] = df['Brand'].apply(normalize_brand)
df['offer'] = df['brand_norm'].map(brand_to_offer)
df = df.dropna(subset=['offer']).copy()
df['offer'] = df['offer'].astype(int)

df['affiliate_revenue'] = pd.to_numeric(df['Affiliate Revenue'], errors='coerce').fillna(0.0)
df['new_cust_revenue'] = pd.to_numeric(df['New Cust. Revenue'], errors='coerce').fillna(0.0)
df['old_cust_revenue'] = pd.to_numeric(df['Old Cust. Revenue'], errors='coerce').fillna(0.0)
df['new_customers'] = pd.to_numeric(df['New Customers'], errors='coerce').fillna(0.0)
df['old_customers'] = pd.to_numeric(df['Old Customers'], errors='coerce').fillna(0.0)
df['affiliate_orders'] = pd.to_numeric(df['Affiliate Orders'], errors='coerce').fillna(0.0)
df['market_norm'] = df['Market'].astype(str).str.strip().str.lower()
df['coupon_norm'] = df['Coupon Code'].apply(normalize_coupon)

# Load coupons mapping and merge
master_map = build_master_affiliate_map(affiliate_xlsx_path, offer_to_sheet)
df_joined = df.merge(master_map, how='left', left_on=['offer','coupon_norm'], right_on=['offer','code_norm'])
missing_aff_mask = df_joined['affiliate_ID'].isna() | (df_joined['affiliate_ID'].astype(str).str.strip() == '')

def _safe_order_count(row: pd.Series) -> float:
    orders = row.get('affiliate_orders', 0.0) or 0.0
    if orders == 0:
        orders = (row.get('new_customers', 0.0) or 0.0) + (row.get('old_customers', 0.0) or 0.0)
    return orders

oman_markets = {"omn", "om", "oman"}
gcc_markets = {"ksa", "uae", "kwt", "qat", "qatar", "bhr", "bah", "bahrain", "omn", "om", "oman"}
egypt_markets = {"egy", "eg", "egypt"}

def calculate_brand_revenue(row: pd.Series) -> float:
    brand = row.get('brand_norm', '')
    market = row.get('market_norm', '')
    total_rev = row.get('affiliate_revenue', 0.0) or 0.0
    new_rev = row.get('new_cust_revenue', 0.0) or 0.0
    new_cnt = row.get('new_customers', 0.0) or 0.0
    old_cnt = row.get('old_customers', 0.0) or 0.0
    val = 0.0
    if brand == 'wes':
        val = 0.10 * new_rev
    elif brand == 'vs':
        val = 5.0 * (new_cnt + old_cnt)
    elif brand == 'pk':
        val = 0.10 * new_rev
    elif brand == 'pb':
        val = 0.10 * new_rev
    elif brand == 'nb':
        val = 0.03 * total_rev
    elif brand == 'fl':
        orders = _safe_order_count(row)
        if market in egypt_markets:
            val = 5.0 * orders
        elif market in gcc_markets:
            val = 6.0 * orders
    elif brand == 'mc':
        val = 4.0 * new_cnt
    elif brand == 'hm':
        val = 8.0 * new_cnt + 2.0 * old_cnt
    elif brand == 'bbw':
        if market in {'ksa', 'kwt'}:
            val = 5.0 * new_cnt + 3.0 * old_cnt
        elif market == 'uae':
            val = 6.0 * new_cnt
    elif brand == 'aeo':
        if market in oman_markets:
            val = 4.0 * _safe_order_count(row)
        elif market in gcc_markets and market != 'qat':
            val = 4.0 * new_cnt + 2.0 * old_cnt
    return float(val)

df_joined['revenue_calc'] = df_joined.apply(calculate_brand_revenue, axis=1)

# Drop zero-revenue rows
df_joined = df_joined[df_joined['revenue_calc'] > 0].copy()

# Payout = revenue * coupon percent (new customer payout)
df_joined['pct_fraction'] = pd.to_numeric(df_joined['pct_fraction'], errors='coerce').fillna(DEFAULT_PCT_IF_MISSING)
df_joined.loc[missing_aff_mask, 'pct_fraction'] = DEFAULT_PCT_IF_MISSING
df_joined.loc[missing_aff_mask, 'affiliate_ID'] = FALLBACK_AFFILIATE_ID
df_joined['payout'] = (df_joined['revenue_calc'] * df_joined['pct_fraction']).round(2)

output_df = pd.DataFrame({
    'offer': df_joined['offer'].astype(int),
    'affiliate_id': df_joined['affiliate_ID'],
    'date': df_joined['Date'].dt.strftime('%m-%d-%Y'),
    'status': STATUS_DEFAULT,
    'payout': df_joined['payout'],
    'revenue': df_joined['revenue_calc'].round(2),
    'sale amount': df_joined['affiliate_revenue'].round(2),
    'coupon': df_joined['coupon_norm'],
    'geo': df_joined['Market'],
})

output_df.to_csv(output_file, index=False)
print(f"Saved: {output_file}")
print(f"Rows: {len(output_df)} | Zero-revenue rows removed")
if not output_df.empty:
    print(f"Date range processed: {output_df['date'].min()} to {output_df['date'].max()}")
else:
    print("No rows after processing.")

