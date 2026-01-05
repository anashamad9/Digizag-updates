import pandas as pd
from datetime import datetime, timedelta
import os
import re

# =======================
# CONFIG (Al Matar)
# =======================
<<<<<<< HEAD
days_back = 33
=======
days_back = 89
>>>>>>> 0d89299 (D)
OFFER_ID = 1349
GEO = "KSA"
STATUS_DEFAULT = "pending"
DEFAULT_PCT_IF_MISSING = 0.0  # 0.30 == 30%
FALLBACK_AFFILIATE_ID = "1"

# Local files (match your tree)
AFFILIATE_XLSX  = "Offers Coupons.xlsx"     # latest workbook you shared
AFFILIATE_SHEET = "Al Matar"                # sheet name for this offer
REPORT_PREFIX   = "Al Matar - Digizag Data - Backend"  # dynamic filename start

# =======================
# PATHS (match your tree)
# =======================
script_dir = os.path.dirname(os.path.abspath(__file__))
input_dir = os.path.join(script_dir, '..', 'input data')
output_dir = os.path.join(script_dir, '..', 'output data')
os.makedirs(output_dir, exist_ok=True)

affiliate_xlsx_path = os.path.join(input_dir, AFFILIATE_XLSX)
output_file = os.path.join(output_dir, 'al_matar.csv')

# =======================
# HELPERS
# =======================
def find_matching_xlsx(directory: str, prefix: str) -> str:
    """
    Find an .xlsx in `directory` whose base filename starts with `prefix` (case-insensitive).
    - Ignores temporary files like '~$...'
    - Prefers exact '<prefix>.xlsx' if present
    - Otherwise returns the newest by modified time
    """
    prefix_lower = prefix.lower()
    candidates = []
    for fname in os.listdir(directory):
        if fname.startswith("~$"):
            continue
        if not fname.lower().endswith(".xlsx"):
            continue
        base = os.path.splitext(fname)[0].lower()
        if base.startswith(prefix_lower):
            candidates.append(os.path.join(directory, fname))

    if not candidates:
        available = [f for f in os.listdir(directory) if f.lower().endswith(".xlsx")]
        raise FileNotFoundError(
            f"No .xlsx file starting with '{prefix}' found in: {directory}\n"
            f"Available .xlsx files: {available}"
        )

    exact = [p for p in candidates if os.path.basename(p).lower() == (prefix_lower + ".xlsx")]
    if exact:
        return exact[0]

    return max(candidates, key=os.path.getmtime)

def normalize_coupon(x: str) -> str:
    """Uppercase, trim, and take the first token if multiple codes separated by ; , or whitespace."""
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


def col_by_letter(df: pd.DataFrame, letter: str) -> str:
    """Return actual column name by Excel letter (A=0, B=1, ...)."""
    idx = ord(letter.upper()) - ord('A')
    if idx < 0 or idx >= len(df.columns):
        raise IndexError(f"Column letter {letter} out of range for columns: {list(df.columns)}")
    return df.columns[idx]

def find_coupon_column(df: pd.DataFrame) -> str:
    """Try common coupon column names; return '' if none found."""
    candidates = ["Coupon Code", "Promo Code", "Coupon", "Code", "Voucher", "Voucher Code"]
    low = {c.lower().strip(): c for c in df.columns}
    for name in candidates:
        col = low.get(name.lower().strip())
        if col:
            return col
    return ""  # handle as missing


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


def parse_route(route_val: str):
    """
    Parse a route like 'KSA-KSA', 'KSA > KSA', 'SAUDI ARABIA to UAE'.
    Return (origin, dest) uppercase or (None, None).
    """
    if pd.isna(route_val):
        return None, None
    s = str(route_val).strip()
    if not s:
        return None, None
    s_norm = re.sub(r"\s*(?:-|–|—|>|to|\/|→)\s*", "-", s, flags=re.IGNORECASE)
    parts = [p.strip().upper() for p in s_norm.split("-") if p.strip()]
    if len(parts) >= 2:
        return parts[0], parts[1]
    return None, None

# =======================
# LOAD MAIN REPORT
# =======================
print(f"Current date: {datetime.now().date()}, Start date (days_back={days_back}): {(datetime.now().date() - timedelta(days=days_back))}")

# Dynamically find the report file
input_file = find_matching_xlsx(input_dir, REPORT_PREFIX)

df = pd.read_excel(input_file)

# Resolve columns by LETTER as requested:
# A: Date, C: Status (must be "success"), G: Product Type (hotel/flight), L: Flight Route - Country
date_col    = col_by_letter(df, 'A')
status_col  = col_by_letter(df, 'C')
product_col = col_by_letter(df, 'G')
route_col   = col_by_letter(df, 'L')

# Parse dates & filter window (exclude today)
df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
df = df.dropna(subset=[date_col])

end_date = datetime.now().date()
start_date = end_date - timedelta(days=days_back)
today = end_date

df_filtered = df[(df[date_col].dt.date >= start_date) & (df[date_col].dt.date < today)].copy()

# Keep only success rows in status (column C)
df_filtered = df_filtered[df_filtered[status_col].astype(str).str.lower().str.strip().eq("success")].copy()

# Sale amount from "Revenue - BE (SAR)" divided by 3.67
low = {c.lower().strip(): c for c in df_filtered.columns}
sale_src = low.get("revenue - be (sar)")
if not sale_src:
    raise KeyError("Could not find 'Revenue - BE (SAR)' column in the report.")
df_filtered['sale_amount'] = pd.to_numeric(df_filtered[sale_src], errors='coerce').fillna(0.0) / 3.67

# Compute revenue by product type & route
def compute_revenue(row):
    pt = str(row.get(product_col, "")).strip().lower()
    sale_amt = float(row.get('sale_amount', 0.0))
    if "hotel" in pt:
        return sale_amt * 0.04  # 4%
    if "flight" in pt or "air" in pt:
        origin, dest = parse_route(row.get(route_col, ""))
        if origin and dest:
            if origin == dest:
                return sale_amt * 0.01   # domestic 1%
            else:
                return sale_amt * 0.015  # international 1.5%
        # can't parse -> treat as international
        return sale_amt * 0.015
    # default conservative -> international flight rate
    return sale_amt * 0.015

df_filtered['revenue'] = df_filtered.apply(compute_revenue, axis=1)

SPECIAL_COUPON_WALA = "WALA2025"

def compute_wala_payout(row):
    """Custom payout for coupon WALA2025 using sale amount tiers."""
    pt = str(row.get(product_col, "")).strip().lower()
    sale_amt = float(row.get('sale_amount', 0.0))
    if "hotel" in pt:
        return sale_amt * 0.036  # 3.6%
    if "flight" in pt or "air" in pt:
        origin, dest = parse_route(row.get(route_col, ""))
        if origin and dest and origin == dest:
            return sale_amt * 0.009  # 0.9% domestic
        return sale_amt * 0.015  # 1.5% international
    # fallback -> treat as international flight
    return sale_amt * 0.015

# Normalize coupon for joining
coupon_col = find_coupon_column(df_filtered)
if coupon_col:
    df_filtered['coupon_norm'] = df_filtered[coupon_col].apply(normalize_coupon)
else:
    df_filtered['coupon_norm'] = ""

# =======================
# JOIN AFFILIATE MAPPING (type-aware)
# =======================
map_df = load_affiliate_mapping_from_xlsx(affiliate_xlsx_path, AFFILIATE_SHEET)
df_joined = df_filtered.merge(map_df, how="left", left_on="coupon_norm", right_on="code_norm")

# Ensure mapping fields exist
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

mask_rev = df_joined['type_norm'].str.lower().eq('revenue')
payout.loc[mask_rev] = (df_joined.loc[mask_rev, 'revenue'] * df_joined.loc[mask_rev, 'pct_fraction'])

mask_sale = df_joined['type_norm'].str.lower().eq('sale')
payout.loc[mask_sale] = (df_joined.loc[mask_sale, 'sale_amount'] * df_joined.loc[mask_sale, 'pct_fraction'])

mask_fixed = df_joined['type_norm'].str.lower().eq('fixed')
payout.loc[mask_fixed] = df_joined.loc[mask_fixed, 'fixed_amount'].fillna(0.0)

mask_wala = df_joined['coupon_norm'].str.upper().eq(SPECIAL_COUPON_WALA)
if mask_wala.any():
    special_values = df_joined.loc[mask_wala].apply(compute_wala_payout, axis=1)
    payout.loc[mask_wala] = special_values

# Fallback: when affiliate_ID is missing -> payout=0 and affiliate_id="1"
mask_no_aff = df_joined['affiliate_ID'].astype(str).str.strip().eq("")
payout.loc[mask_no_aff] = 0.0
df_joined.loc[mask_no_aff, 'affiliate_ID'] = FALLBACK_AFFILIATE_ID

df_joined['payout'] = payout.round(2)

# =======================
# BUILD FINAL OUTPUT (standard schema)
# =======================
output_df = pd.DataFrame({
    'offer': OFFER_ID,
    'affiliate_id': df_joined['affiliate_ID'],
    'date': df_joined[date_col].dt.strftime('%m-%d-%Y'),
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
