#!/usr/bin/env python3
import pandas as pd
from datetime import datetime, timedelta
import os
import re
from typing import List, Optional

# =======================
# CONFIG
# =======================
days_back = 120
OFFER_ID = 1334
STATUS_DEFAULT = "pending"
DEFAULT_PCT_IF_MISSING = 0.0
FALLBACK_AFFILIATE_ID = "1"

# File names
AFFILIATE_XLSX  = "Offers Coupons.xlsx"
AFFILIATE_SHEET = "Dr Nutrition"    # coupons sheet for this offer

# =======================
# PATHS & SEARCH
# =======================
def existing_dirs(paths: List[str]) -> List[str]:
    out = []
    for p in paths:
        try:
            if os.path.isdir(p):
                out.append(p)
        except Exception:
            pass
    return out

def get_script_dir() -> str:
    try:
        return os.path.dirname(os.path.abspath(__file__))
    except NameError:
        # Fallback for interactive runs
        return os.getcwd()

script_dir = get_script_dir()

# Where to look for inputs (includes your upload area)
SEARCH_DIRS = existing_dirs([
    os.path.join(script_dir, '..', 'input data'),
    script_dir,
    os.path.abspath(os.path.join(script_dir, '..')),
    '/mnt/data',
])

# ALWAYS write outputs to a sibling "../output data" next to the script
output_dir = os.path.join(script_dir, '..', 'output data')
os.makedirs(output_dir, exist_ok=True)

def find_first_existing_file(filename: str, extra_dirs: Optional[List[str]] = None) -> Optional[str]:
    dirs = SEARCH_DIRS + (extra_dirs or [])
    for d in dirs:
        cand = os.path.join(d, filename)
        if os.path.isfile(cand):
            return cand
    return None

def list_matching_files(prefix: str, suffix: str) -> List[str]:
    files = []
    for d in SEARCH_DIRS:
        try:
            for f in os.listdir(d):
                if f.startswith(prefix) and f.endswith(suffix):
                    files.append(os.path.join(d, f))
        except Exception:
            pass
    return files

# =======================
# HELPERS
# =======================
def normalize_coupon(x: str) -> str:
    """Uppercase, trim, first token if multiple codes separated by ; , or whitespace."""
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


def extract_timestamp(fullpath: str) -> datetime:
    filename = os.path.basename(fullpath)
    m = re.search(r'Dr\.Nutrition_DigiZag_Report_(\d{4}_\d{2}_\d{2}_\d{2}_\d{2}_\d{2})', filename)
    if m:
        ts = m.group(1).replace('_', '-')
        return datetime.strptime(ts, '%Y-%m-%d-%H-%M-%S')
    return datetime.min

def choose_sheet(xlsx_path: str, preferred: str = "Worksheet") -> str:
    xf = pd.ExcelFile(xlsx_path)
    return preferred if preferred in xf.sheet_names else xf.sheet_names[0]

def norm_key(s: str) -> str:
    return re.sub(r"\s+", "", s).strip().lower()

def get_col(df: pd.DataFrame, candidates: List[str]) -> str:
    """
    Case/space-insensitive column resolver.
    Returns the real column name from df.columns that matches any candidate.
    """
    norm = {norm_key(c): c for c in df.columns}
    for cand in candidates:
        key = norm_key(cand)
        if key in norm:
            return norm[key]
    raise KeyError(f"None of the columns {candidates} found. Available: {list(df.columns)}")


def load_affiliate_mapping_from_xlsx(xlsx_path: str, sheet_name: str) -> pd.DataFrame:
    """Return mapping with columns code_norm, affiliate_ID, type_norm, pct_new, pct_old, fixed_new, fixed_old."""
    df_sheet = pd.read_excel(xlsx_path, sheet_name=sheet_name, dtype=str)
    cols_norm = {norm_key(c): c for c in df_sheet.columns}

    def find_col(candidates):
        for cand in candidates:
            col = cols_norm.get(norm_key(cand))
            if col:
                return col
        return None

    def require_col(label: str, candidates) -> str:
        col = find_col(candidates)
        if not col:
            cand_list = ", ".join(f"'{c}'" for c in candidates)
            raise ValueError(f"[{sheet_name}] must contain a {label} column (tried {cand_list}).")
        return col

    code_col = require_col("'code'", [
        'code',
        'coupon',
        'coupon code',
        'coupon_code',
        'couponcode',
        'promo code',
        'voucher code',
        'unnamed: 0',
        'unnamed:0',
    ])
    aff_col = find_col(['id', 'affiliate id', 'affiliate_id'])
    type_col = require_col("'type'", ['type', 'offer type'])
    payout_col = find_col(['payout', 'default payout'])
    new_col = find_col(['new customer payout', 'new payout'])
    old_col = find_col(['old customer payout', 'old payout', 'existing customer payout'])

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


def map_geo(geo):
    geo = str(geo).strip() if pd.notnull(geo) else ''
    if geo == 'Saudi Arabia':
        return 'ksa'
    elif geo == 'Kuwait':
        return 'kwt'
    elif geo == 'Qatar':
        return 'qtr'
    elif geo == 'Jordan':
        return None  # exclude Jordan
    elif geo == 'UAE':
        return 'uae'
    return 'no-geo'

# =======================
# LOAD LATEST REPORT
# =======================
end_date = datetime.now().date()
start_date = end_date - timedelta(days=days_back)
print(f"[INFO] Current date: {end_date}, Start date (days_back={days_back}): {start_date}")
print(f"[INFO] Input search dirs: {SEARCH_DIRS}")
print(f"[INFO] Output dir: {output_dir}")

matches = list_matching_files('Dr.Nutrition_DigiZag_Report_', '.xlsx')
if not matches:
    raise FileNotFoundError(
        "No files starting with 'Dr.Nutrition_DigiZag_Report_' found in: "
        + ", ".join(SEARCH_DIRS)
    )

latest_path = max(matches, key=extract_timestamp)
print(f"[INFO] Using input file: {latest_path}")
report_sheet = choose_sheet(latest_path, preferred="Worksheet")
print(f"[INFO] Using report sheet: {report_sheet}")

df = pd.read_excel(latest_path, sheet_name=report_sheet)

# Normalize column names (case/space insensitive lookup)
created_col   = get_col(df, ["Created Date", "Date", "Created"])
campaign_col  = get_col(df, ["Campaign"])
status_col    = get_col(df, ["Status"])
code_col      = get_col(df, ["Code"])
sell_col      = get_col(df, ["Selling Price"])
comm_col      = get_col(df, ["commission"])
country_col   = get_col(df, ["country"])
try:
    type_detail_col = get_col(df, ["Type"])
except KeyError:
    type_detail_col = None

# Convert date/time and drop invalids
df[created_col] = pd.to_datetime(df[created_col], errors='coerce')
before = len(df)
df = df.dropna(subset=[created_col])
print(f"[INFO] Total rows before filtering: {before}")
print(f"[INFO] Rows with invalid dates dropped: {before - len(df)}")

# Campaign filter (DigiZag) and not canceled
df_offer = df[df[campaign_col].astype(str) == 'DigiZag'].copy()
df_offer = df_offer[df_offer[status_col].astype(str).str.lower() != 'canceled'].copy()

# Date window (exclude 'today')
df_filtered = df_offer[
    (df_offer[created_col].dt.date >= start_date) &
    (df_offer[created_col].dt.date < end_date)
].copy()

# Track raw Type values for special payout handling (e.g., coupon M33)
if type_detail_col is not None:
    df_filtered["order_type_value"] = df_filtered[type_detail_col].fillna("")
else:
    df_filtered["order_type_value"] = ""
df_filtered["order_type_value"] = df_filtered["order_type_value"].astype(str)
df_filtered["order_type_norm"] = df_filtered["order_type_value"].str.strip().str.lower()

# =======================
# DERIVED FIELDS
# =======================
# Currency conversion: AED->USD
df_filtered["sale_amount"] = pd.to_numeric(df_filtered[sell_col], errors="coerce") / 3.67
df_filtered["revenue"]     = pd.to_numeric(df_filtered[comm_col], errors="coerce") / 3.67

# Geo mapping; drop Jordan
df_filtered["geo"] = df_filtered[country_col].apply(map_geo)
df_filtered = df_filtered.dropna(subset=["geo"])

# Normalize coupon for joining
df_filtered["coupon_norm"] = df_filtered[code_col].apply(normalize_coupon)

# =======================
# JOIN AFFILIATE MAPPING
# =======================
affiliate_xlsx_path = find_first_existing_file(AFFILIATE_XLSX)
if affiliate_xlsx_path is None:
    raise FileNotFoundError(f"Could not find '{AFFILIATE_XLSX}' in: {SEARCH_DIRS}")
print(f"[INFO] Using affiliate file: {affiliate_xlsx_path} | sheet: {AFFILIATE_SHEET}")

map_df = load_affiliate_mapping_from_xlsx(affiliate_xlsx_path, AFFILIATE_SHEET)
df_joined = df_filtered.merge(map_df, how="left", left_on="coupon_norm", right_on="code_norm")

# Missing affiliate?
missing_aff_mask = df_joined["affiliate_ID"].isna() | (df_joined["affiliate_ID"].astype(str).str.strip() == "")

# Normalize fields
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

mask_rev   = df_joined["type_norm"].str.lower().eq("revenue")
mask_sale  = df_joined["type_norm"].str.lower().eq("sale")
mask_fixed = df_joined["type_norm"].str.lower().eq("fixed")

payout.loc[mask_rev]   = df_joined.loc[mask_rev,   "revenue"]     * df_joined.loc[mask_rev,   "pct_fraction"]
payout.loc[mask_sale]  = df_joined.loc[mask_sale,  "sale_amount"] * df_joined.loc[mask_sale,  "pct_fraction"]
payout.loc[mask_fixed] = df_joined.loc[mask_fixed, "fixed_amount"].fillna(0.0)

# Special handling for coupon M33 based on report "Type"
coupon_norm_upper = df_joined["coupon_norm"].fillna("").astype(str).str.upper()
type_norm_series = df_joined.get("order_type_norm", pd.Series("", index=df_joined.index))
type_norm_series = type_norm_series.fillna("").astype(str).str.strip().str.lower()

mask_m33 = coupon_norm_upper.eq("M33")
mask_m33_sale = mask_m33 & type_norm_series.eq("sale")
mask_m33_empty = mask_m33 & type_norm_series.eq("")

payout.loc[mask_m33_sale] = df_joined.loc[mask_m33_sale, "revenue"] * 0.90
payout.loc[mask_m33_empty] = df_joined.loc[mask_m33_empty, "revenue"] * 0.85

# Enforce: if no affiliate match, set affiliate_id="1", payout=0
payout.loc[missing_aff_mask] = 0.0
df_joined.loc[missing_aff_mask, "affiliate_ID"] = FALLBACK_AFFILIATE_ID

df_joined["payout"] = payout.round(2)

# =======================
# BUILD FINAL OUTPUT
# =======================
output_df = pd.DataFrame({
    "offer":        OFFER_ID,
    "affiliate_id": df_joined["affiliate_ID"],
    "date":         df_joined[created_col].dt.strftime("%m-%d-%Y"),
    "status":       STATUS_DEFAULT,
    "payout":       df_joined["payout"],
    "revenue":      df_joined["revenue"].round(2),
    "sale amount":  df_joined["sale_amount"].round(2),
    "coupon":       df_joined["coupon_norm"],
    "geo":          df_joined["geo"],
})

# =======================
# SAVE — ALWAYS INTO output_dir
# =======================
output_file = os.path.join(output_dir, "Dr_Nu.csv")
output_df.to_csv(output_file, index=False)

print(f"[OK] Saved: {output_file}")
print(
    f"[STATS] Rows: {len(output_df)} | "
    f"Fallback affiliates: {int(missing_aff_mask.sum())} | "
    f"Type counts -> revenue: {int(mask_rev.sum())}, sale: {int(mask_sale.sum())}, fixed: {int(mask_fixed.sum())}"
)
print(f"[INFO] Date range processed: "
      f"{output_df['date'].min() if not output_df.empty else 'N/A'} to "
      f"{output_df['date'].max() if not output_df.empty else 'N/A'}")
