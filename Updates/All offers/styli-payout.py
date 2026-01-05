import pandas as pd
from datetime import datetime, timedelta
import os
import re
from typing import Optional  # <-- for Optional[str] annotations

# =======================
# CONFIG
# =======================
days_back = 3
OFFER_ID = 1215
STATUS_DEFAULT = "pending"
DEFAULT_PCT_IF_MISSING = 0.0
FALLBACK_AFFILIATE_ID = "1"

REPORT_PREFIX   = "Styli - Affiliate Payout_Raw Data - Order Level_Table"  # dynamic CSV start
AFFILIATE_XLSX  = "Offers Coupons.xlsx"
AFFILIATE_SHEET = "Styli"
OUTPUT_CSV      = "styli.csv"

AED_TO_USD = 3.67  # divisor

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
today = datetime.now().date()
end_date = today            # exclude "today"
start_date = end_date - timedelta(days=days_back)
print(f"Window: {start_date} ≤ date < {end_date}")

# =======================
# HELPERS
# =======================
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
    """Uppercase, trim, NBSP→space, keep first token if multiple codes separated by ; , or whitespace."""
    if pd.isna(x):
        return ""
    s = str(x).replace("\u00A0", " ").strip().upper()
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


def to_number(series: pd.Series) -> pd.Series:
    """Robust numeric coercion (strips commas/currency tokens)."""
    return pd.to_numeric(
        series.astype(str)
              .str.replace(",", "", regex=False)
              .str.replace("AED", "", case=False, regex=False)
              .str.strip(),
        errors="coerce"
    )

def pick_col(df: pd.DataFrame, *cands) -> Optional[str]:
    """Case/space-insensitive header resolver."""
    norm = {str(c).strip().lower(): c for c in df.columns}
    # exact match
    for cand in cands:
        key = str(cand).strip().lower()
        if key in norm:
            return norm[key]
    # startswith fallback
    for cand in cands:
        key = str(cand).strip().lower()
        for low, actual in norm.items():
            if low.startswith(key):
                return actual
    return None


def load_affiliate_mapping_from_xlsx(xlsx_path: str, sheet_name: str) -> pd.DataFrame:
    """Return mapping with columns code_norm, affiliate_ID, type_norm, pct_new, pct_old, fixed_new, fixed_old."""
    df_sheet = pd.read_excel(xlsx_path, sheet_name=sheet_name, dtype=str)

    code_col = pick_col(df_sheet, 'code', 'coupon code', 'coupon')
    aff_col = pick_col(df_sheet, 'id', 'affiliate_id', 'affiliate id')
    type_col = pick_col(df_sheet, 'type', 'payout type', 'commission type')
    payout_col = pick_col(df_sheet, 'payout', 'commission', 'rate')
    new_col = pick_col(df_sheet, 'new customer payout', 'new payout', 'ftu payout', 'acquisition payout')
    old_col = pick_col(df_sheet, 'old customer payout', 'existing customer payout', 'returning customer payout', 'rtu payout')

    if not code_col:
        raise ValueError(f"[{sheet_name}] must contain a 'Code' column.")
    if not aff_col:
        raise ValueError(f"[{sheet_name}] must contain an 'ID' (or 'affiliate_ID') column.")
    if not type_col:
        raise ValueError(f"[{sheet_name}] must contain a 'type' column (revenue/sale/fixed).")
    if not (payout_col or new_col or old_col):
        raise ValueError(f"[{sheet_name}] must contain at least one payout-like column.")

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
# LOAD REPORT
# =======================
# Dynamically pick the changing CSV
input_file = find_matching_csv(input_dir, REPORT_PREFIX)
print(f"Using report file: {input_file}")

df_raw = pd.read_csv(input_file)

# Resolve important columns flexibly
date_col    = pick_col(df_raw, "order date")
sale_col    = pick_col(df_raw, "order value (aed)", "order value")
country_col = pick_col(df_raw, "country")
coupon_col  = pick_col(df_raw, "coupon", "coupon code", "aff_coupon")

missing = [n for n, c in {
    "Order Date": date_col,
    "Order Value (AED)": sale_col,
    "Country": country_col,
    "Coupon": coupon_col
}.items() if c is None]
if missing:
    raise KeyError(f"Missing expected column(s): {missing}. Columns found: {list(df_raw.columns)}")

df = df_raw.rename(columns={
    date_col: "Order Date",
    sale_col: "Order Value (AED)",
    country_col: "country",
    coupon_col: "Coupon",
})

# Parse dates & filter window (exclude today)
df["Order Date"] = pd.to_datetime(df["Order Date"], errors="coerce")
df = df.dropna(subset=["Order Date"])
df_filtered = df[(df["Order Date"].dt.date >= start_date) & (df["Order Date"].dt.date < end_date)].copy()
print(f"Rows after date filter: {len(df_filtered)}")

# =======================
# DERIVED FIELDS
# =======================
# Sale amount (AED -> USD), robust parsing
df_filtered["sale_amount"] = to_number(df_filtered["Order Value (AED)"]).fillna(0.0) / AED_TO_USD

# Revenue (10% of sale)
df_filtered["revenue"] = df_filtered["sale_amount"] * 0.10

# Geo mapping
geo_mapping = {"KSA": "ksa", "UAE": "uae", "KWT": "kwt"}
df_filtered["geo"] = df_filtered["country"].map(geo_mapping).fillna("no-geo")

# Coupon normalization for join
df_filtered["coupon_norm"] = df_filtered["Coupon"].apply(normalize_coupon)

# =======================
# JOIN AFFILIATE MAPPING (type-aware)
# =======================
map_df = load_affiliate_mapping_from_xlsx(affiliate_xlsx_path, AFFILIATE_SHEET)
df_joined = df_filtered.merge(map_df, how="left", left_on="coupon_norm", right_on="code_norm")

# Missing affiliate?
missing_aff_mask = df_joined["affiliate_ID"].isna() | (df_joined["affiliate_ID"].astype(str).str.strip() == "")
if missing_aff_mask.any():
    print("Unmatched coupons (sample):",
          df_joined.loc[missing_aff_mask, "coupon_norm"].drop_duplicates().head(20).to_list())

# Normalize mapping fields
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

payout.loc[mask_rev]   = df_joined.loc[mask_rev, "revenue"]      * df_joined.loc[mask_rev, "pct_fraction"]
payout.loc[mask_sale]  = df_joined.loc[mask_sale, "sale_amount"] * df_joined.loc[mask_sale, "pct_fraction"]
payout.loc[mask_fixed] = df_joined.loc[mask_fixed, "fixed_amount"].fillna(0.0)

# Enforce fallback for no affiliate
payout.loc[missing_aff_mask] = 0.0
df_joined.loc[missing_aff_mask, "affiliate_ID"] = FALLBACK_AFFILIATE_ID

df_joined["payout"] = payout.round(2)

# =======================
# BUILD OUTPUT (NEW STRUCTURE)
# =======================
output_df = pd.DataFrame({
    "offer": OFFER_ID,
    "affiliate_id": df_joined["affiliate_ID"],
    "date": df_joined["Order Date"].dt.strftime("%m-%d-%Y"),
    "status": STATUS_DEFAULT,
    "payout": df_joined["payout"],
    "revenue": df_joined["revenue"].round(2),
    "sale amount": df_joined["sale_amount"].round(2),
    "coupon": df_joined["coupon_norm"],
    "geo": df_joined["geo"],
})

# =======================
# SAVE
# =======================
output_df.to_csv(output_file, index=False)

print(f"Saved: {output_file}")
print(f"Rows: {len(output_df)} | No-affiliate coupons: {int(missing_aff_mask.sum())}")
if not output_df.empty:
    print(f"Date range processed: {output_df['date'].min()} → {output_df['date'].max()}")