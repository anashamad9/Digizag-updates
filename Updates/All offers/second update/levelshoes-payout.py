import pandas as pd
from datetime import datetime, timedelta
import os
import re

# =======================
# CONFIG
# =======================
OFFER_ID = 1159
STATUS_DEFAULT = "pending"
FALLBACK_AFFILIATE_ID = "1"
DEFAULT_PCT_IF_MISSING = 0.0

# How many days back to include (EXCLUDES today)
DAYS_BACK = 60

# Currency: set divisor to 1.0 if sale amounts are already USD
AED_TO_USD_DIVISOR = 3.67

# Files
AFFILIATE_XLSX   = "Offers Coupons.xlsx"
AFFILIATE_SHEET  = "Levelshoes"
OUTPUT_CSV       = "levelshoes.csv"

# =======================
# PATHS
# =======================
script_dir = os.path.dirname(os.path.abspath(__file__))
updates_dir = os.path.dirname(os.path.dirname(script_dir))
input_dir  = os.path.join(updates_dir, 'Input data')
output_dir = os.path.join(updates_dir, 'output data')
os.makedirs(output_dir, exist_ok=True)

aff_map_path  = os.path.join(input_dir, AFFILIATE_XLSX)
output_path   = os.path.join(output_dir, OUTPUT_CSV)

# =======================
# DATE WINDOW
# =======================
today = datetime.now().date()
start_date = today - timedelta(days=DAYS_BACK)
print(f"Today: {today} | Start date (days_back={DAYS_BACK}): {start_date}")

# =======================
# FIND LATEST SOURCE FILE (conversion_item_report_YYYY-MM-DD_HH_MM_SS*.csv)
# =======================
# Accepts optional suffix before .csv, e.g., "(1)"
NAME_RE = re.compile(
    r"^conversion_item_report_(\d{4}-\d{2}-\d{2})_(\d{2})_(\d{2})_(\d{2}).*\.csv$",
    re.IGNORECASE
)

def extract_stamp(fname: str):
    """
    Return a datetime from the filename if it matches the expected pattern,
    else None.
    """
    m = NAME_RE.match(fname)
    if not m:
        return None
    date_s, hh, mm, ss = m.groups()
    try:
        return datetime.strptime(f"{date_s} {hh}:{mm}:{ss}", "%Y-%m-%d %H:%M:%S")
    except Exception:
        return None

candidates = []
for f in os.listdir(input_dir):
    if not f.lower().endswith(".csv"):
        continue
    if NAME_RE.match(f):
        candidates.append(f)

if not candidates:
    avail = [f for f in os.listdir(input_dir) if f.lower().endswith(".csv")]
    raise FileNotFoundError(
        "No 'conversion_item_report_YYYY-MM-DD_HH_MM_SS*.csv' file found in input data folder.\n"
        f"Available CSVs: {avail}"
    )

# Prefer by embedded timestamp; fallback to mtime if none parse
stamped = [(extract_stamp(f), f) for f in candidates]
stamped = [t for t in stamped if t[0] is not None]
if stamped:
    stamped.sort(key=lambda t: t[0])
    latest_file = stamped[-1][1]
else:
    latest_file = max(candidates, key=lambda f: os.path.getmtime(os.path.join(input_dir, f)))

source_path = os.path.join(input_dir, latest_file)
print(f"Using input file: {latest_file}")

# =======================
# HELPERS
# =======================
def xl_col_to_index(col_letters: str) -> int:
    col_letters = col_letters.strip().upper()
    n = 0
    for ch in col_letters:
        n = n * 26 + (ord(ch) - ord('A') + 1)
    return n - 1

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
        'customer_type',
        'customer type',
        'cust_type',
        'cust type',
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


# =======================
# LOAD SOURCE (columns by Excel letters: E=date, W=sale, X=user type, AJ=coupon)
# =======================
df_raw = pd.read_csv(source_path, header=0)

col_date_idx      = xl_col_to_index("E")
col_sale_idx      = xl_col_to_index("W")
col_type_idx      = xl_col_to_index("X")
col_coupon_idx    = xl_col_to_index("AJ")
col_publisher_idx = xl_col_to_index("B")

max_needed = max(col_date_idx, col_sale_idx, col_type_idx, col_coupon_idx, col_publisher_idx)
if df_raw.shape[1] <= max_needed:
    raise IndexError(
        f"CSV has {df_raw.shape[1]} columns, need ≥ {max_needed+1} to access B/E/W/X/AJ."
    )

df = pd.DataFrame({
    "date_raw":   df_raw.iloc[:, col_date_idx],
    "sale_raw":   df_raw.iloc[:, col_sale_idx],
    "cust_type":  df_raw.iloc[:, col_type_idx],
    "coupon_raw": df_raw.iloc[:, col_coupon_idx],
    "publisher_id": df_raw.iloc[:, col_publisher_idx],
})

# =======================
# DERIVED FIELDS & DATE FILTER
# =======================
df["date"] = pd.to_datetime(df["date_raw"], errors="coerce")
df = df.dropna(subset=["date"]).copy()

# Filter to last DAYS_BACK days, excluding today
df = df[(df["date"].dt.date >= start_date) & (df["date"].dt.date < today)].copy()

# Sale amount (AED -> USD if needed)
df["sale_amount"] = pd.to_numeric(df["sale_raw"], errors="coerce").fillna(0.0) / AED_TO_USD_DIVISOR

def is_new(val) -> bool:
    return "new" in str(val).strip().lower()

df["publisher_id"] = df["publisher_id"].fillna("").astype(str).str.strip()

# Revenue rule: 10% new, 5% old
df["revenue"] = df.apply(lambda r: r["sale_amount"] * (0.10 if is_new(r["cust_type"]) else 0.05), axis=1)

df["coupon_norm"] = df["coupon_raw"].apply(normalize_coupon)

# =======================
# JOIN AFFILIATE MAPPING (type-aware)
# =======================
map_df = load_affiliate_mapping_from_xlsx(aff_map_path, AFFILIATE_SHEET)
df_joined = df.merge(map_df, how="left", left_on="coupon_norm", right_on="code_norm")

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

df_joined['publisher_id'] = df_joined['publisher_id'].fillna("").astype(str).str.strip()
blank_publishers = int(df_joined['publisher_id'].eq("").sum())
if blank_publishers:
    print(f"Rows missing publisher_id (relying on voucher mapping): {blank_publishers}")
missing_aff = df_joined["affiliate_ID"].isna() | (df_joined["affiliate_ID"].astype(str).str.strip() == "")

# Compute payout by type
payout = pd.Series(0.0, index=df_joined.index)
mask_rev   = df_joined["type_norm"].str.lower().eq("revenue")
mask_sale  = df_joined["type_norm"].str.lower().eq("sale")
mask_fixed = df_joined["type_norm"].str.lower().eq("fixed")

payout.loc[mask_rev]   = df_joined.loc[mask_rev,   "revenue"]     * df_joined.loc[mask_rev,   "pct_fraction"].fillna(DEFAULT_PCT_IF_MISSING)
payout.loc[mask_sale]  = df_joined.loc[mask_sale,  "sale_amount"] * df_joined.loc[mask_sale,  "pct_fraction"].fillna(DEFAULT_PCT_IF_MISSING)
payout.loc[mask_fixed] = df_joined.loc[mask_fixed, "fixed_amount"].fillna(0.0)

# Fallback when no affiliate match
payout.loc[missing_aff] = 0.0
df_joined.loc[missing_aff, "affiliate_ID"] = FALLBACK_AFFILIATE_ID
df_joined["payout"] = payout.round(2)

# =======================
# OUTPUT (unified structure)
# =======================
output_df = pd.DataFrame({
    "offer": OFFER_ID,
    "affiliate_id": df_joined["affiliate_ID"],
    "date": df_joined["date"].dt.strftime("%m-%d-%Y"),
    "status": STATUS_DEFAULT,
    "payout": df_joined["payout"],
    "revenue": df_joined["revenue"].round(2),
    "sale amount": df_joined["sale_amount"].round(2),
    "coupon": df_joined["coupon_norm"],
    "geo": "no-geo",
})

output_df.to_csv(output_path, index=False)

print(f"Saved: {output_path}")
print(f"Rows: {len(output_df)} | Fallback affiliate rows: {int(missing_aff.sum())}")
print(f"Window: {start_date} ≤ date < {today}")
