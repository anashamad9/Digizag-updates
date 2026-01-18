import pandas as pd
from datetime import datetime, timedelta
import os
import re

# =======================
# CONFIG (Al Matar)
# =======================
days_back = 33
OFFER_ID = 1349
GEO = "KSA"
STATUS_DEFAULT = "pending"
DEFAULT_PCT_IF_MISSING = 0.0  # 0.30 == 30%
FALLBACK_AFFILIATE_ID = "1"

# Local files (match your tree)
AFFILIATE_XLSX_PREFIX  = "Offers Coupons"   # latest workbook prefix
AFFILIATE_SHEET = "Al Matar"                # sheet name for this offer
REPORT_PREFIX   = "Al Matar - Digizag Data - Backend"  # dynamic filename start

# =======================
# PATHS (match your tree)
# =======================
script_dir = os.path.dirname(os.path.abspath(__file__))
input_dir = os.path.join(script_dir, '..', 'Input data')
output_dir = os.path.join(script_dir, '..', 'output data')
os.makedirs(output_dir, exist_ok=True)

affiliate_xlsx_path = None
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

affiliate_xlsx_path = find_matching_xlsx(input_dir, AFFILIATE_XLSX_PREFIX)

def normalize_coupon(x: str) -> str:
    """Uppercase, trim, and take the first token if multiple codes separated by ; , or whitespace."""
    if pd.isna(x):
        return ""
    s = str(x).strip().upper()
    parts = re.split(r"[;,\s]+", s)
    return parts[0] if parts else s

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
    """Return mapping with columns code_norm, affiliate_ID, pct_domestic, pct_international, pct_hotel."""
    df_sheet = pd.read_excel(xlsx_path, sheet_name=sheet_name, dtype=str)
    cols_lower = {str(c).lower().strip(): c for c in df_sheet.columns}

    def need(name: str) -> str:
        col = cols_lower.get(name)
        if not col:
            raise ValueError(f"[{sheet_name}] must contain a '{name}' column.")
        return col

    code_col = need('code')
    aff_col = cols_lower.get('id') or cols_lower.get('affiliate_id')
    domestic_col = cols_lower.get('domestic')
    international_col = cols_lower.get('international')
    hotel_col = cols_lower.get('hotels') or cols_lower.get('hotel')

    if not aff_col:
        raise ValueError(f"[{sheet_name}] must contain an 'ID' (or 'affiliate_ID') column.")
    if not (domestic_col or international_col or hotel_col):
        raise ValueError(f"[{sheet_name}] must contain payout columns for domestic/international/hotels.")

    def extract_numeric(col_name: str) -> pd.Series:
        if not col_name:
            return pd.Series([pd.NA] * len(df_sheet), dtype='Float64')
        raw = df_sheet[col_name].astype(str).str.replace('%', '', regex=False).str.strip()
        return pd.to_numeric(raw, errors='coerce')

    def pct_from(values: pd.Series) -> pd.Series:
        return values.apply(lambda v: (v / 100.0) if pd.notna(v) and v > 1 else (v if pd.notna(v) else pd.NA))

    pct_domestic = pct_from(extract_numeric(domestic_col))
    pct_international = pct_from(extract_numeric(international_col))
    pct_hotel = pct_from(extract_numeric(hotel_col))

    out = pd.DataFrame({
        'code_norm': df_sheet[code_col].apply(normalize_coupon),
        'affiliate_ID': df_sheet[aff_col].fillna('').astype(str).str.strip(),
        'pct_domestic': pd.to_numeric(pct_domestic, errors='coerce').fillna(DEFAULT_PCT_IF_MISSING),
        'pct_international': pd.to_numeric(pct_international, errors='coerce').fillna(DEFAULT_PCT_IF_MISSING),
        'pct_hotel': pd.to_numeric(pct_hotel, errors='coerce').fillna(DEFAULT_PCT_IF_MISSING),
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
# A: Date, C: Status, G: Product Type (hotel/flight), L: Flight Route - Country
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

## Keep all statuses (no filtering)

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
        return sale_amt * 0.05  # 5%
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

def compute_trip_type(row) -> str:
    pt = str(row.get(product_col, "")).strip().lower()
    if "hotel" in pt:
        return "hotel"
    if "flight" in pt or "air" in pt:
        origin, dest = parse_route(row.get(route_col, ""))
        if origin and dest and origin == dest:
            return "domestic"
        return "international"
    return "international"

df_filtered['trip_type'] = df_filtered.apply(compute_trip_type, axis=1)

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
# JOIN AFFILIATE MAPPING (trip-type aware)
# =======================
map_df = load_affiliate_mapping_from_xlsx(affiliate_xlsx_path, AFFILIATE_SHEET)
df_joined = df_filtered.merge(map_df, how="left", left_on="coupon_norm", right_on="code_norm")

# Ensure mapping fields exist
df_joined['affiliate_ID'] = df_joined['affiliate_ID'].fillna("").astype(str).str.strip()
for col in ['pct_domestic', 'pct_international', 'pct_hotel']:
    df_joined[col] = pd.to_numeric(df_joined.get(col), errors='coerce').fillna(DEFAULT_PCT_IF_MISSING)
df_joined['pct_fraction'] = df_joined['pct_international']
df_joined.loc[df_joined['trip_type'].eq('hotel'), 'pct_fraction'] = df_joined['pct_hotel']
df_joined.loc[df_joined['trip_type'].eq('domestic'), 'pct_fraction'] = df_joined['pct_domestic']
df_joined['pct_fraction'] = pd.to_numeric(df_joined['pct_fraction'], errors='coerce').fillna(DEFAULT_PCT_IF_MISSING)

# =======================
# COMPUTE PAYOUT BASED ON TYPE
# =======================
payout = (df_joined['revenue'] * df_joined['pct_fraction']).fillna(0.0)

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
    f"Trip counts -> hotel: {int(df_joined['trip_type'].eq('hotel').sum())}, "
    f"domestic: {int(df_joined['trip_type'].eq('domestic').sum())}, "
    f"international: {int(df_joined['trip_type'].eq('international').sum())}"
)
