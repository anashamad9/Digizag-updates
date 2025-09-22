import pandas as pd
from datetime import datetime, timedelta
import os
import re

# =======================
# CONFIG
# =======================
days_back = 14
OFFER_ID = 1291
STATUS_DEFAULT = "pending"          # always pending
DEFAULT_PCT_IF_MISSING = 0.0        # fallback fraction if percent missing
FALLBACK_AFFILIATE_ID = "1"         # when no affiliate match: set "1" and payout = 0
GEO_FALLBACK = "no-geo"             # used if you later want to remap; for now we keep country as-is

# Dynamic report file: pick any .xlsx that starts with this prefix
REPORT_PREFIX   = "DigiZag_MAGRABi_Report"
REPORT_SHEET    = "Sheet1"

# Affiliate map
AFFILIATE_XLSX  = "Offers Coupons.xlsx"
AFFILIATE_SHEET = "Magrabi"

# Output
OUTPUT_CSV      = "magrabi.csv"

# =======================
# PATHS
# =======================
script_dir = os.path.dirname(os.path.abspath(__file__))
input_dir = os.path.join(script_dir, '..', 'input data')
output_dir = os.path.join(script_dir, '..', 'output data')
os.makedirs(output_dir, exist_ok=True)

affiliate_xlsx_path = os.path.join(input_dir, AFFILIATE_XLSX)
output_file = os.path.join(output_dir, OUTPUT_CSV)

# =======================
# HELPERS
# =======================
def _norm_name(s: str) -> str:
    return re.sub(r"\s+", " ", str(s).strip()).lower()

def find_matching_xlsx(directory: str, prefix: str) -> str:
    """
    Find an .xlsx in `directory` whose base filename starts with `prefix` (space/case-insensitive).
    Prefer exact '<prefix>.xlsx' if present; else pick newest by mtime.
    """
    prefix_n = _norm_name(prefix)
    candidates = []
    for fname in os.listdir(directory):
        if fname.startswith("~$"):
            continue
        if not fname.lower().endswith(".xlsx"):
            continue
        base = os.path.splitext(fname)[0]
        if _norm_name(base).startswith(prefix_n):
            candidates.append(os.path.join(directory, fname))
    if not candidates:
        available = [f for f in os.listdir(directory) if f.lower().endswith(".xlsx")]
        raise FileNotFoundError(
            f"No .xlsx starting with '{prefix}' in: {directory}\nAvailable: {available}"
        )
    exact = [p for p in candidates if _norm_name(os.path.splitext(os.path.basename(p))[0]) == prefix_n]
    if exact:
        return exact[0]
    return max(candidates, key=os.path.getmtime)

def normalize_coupon(x: str) -> str:
    """Uppercase, trim, take first token if multiple codes separated by ; , or whitespace."""
    if pd.isna(x):
        return ""
    s = str(x).strip().upper()
    parts = re.split(r"[;,\s]+", s)
    return parts[0] if parts else s

def convert_date(val):
    """Handle Excel serials (days since 1899-12-30) and normal strings."""
    if pd.isna(val):
        return pd.NaT
    # try Excel serial first
    try:
        return pd.to_datetime(val, origin='1899-12-30', unit='D')
    except Exception:
        return pd.to_datetime(val, errors='coerce')

def resolve_col(df: pd.DataFrame, candidates) -> str:
    """Resolve a column name case-insensitively with startswith fallback."""
    low = {str(c).strip().lower(): c for c in df.columns}
    for cand in candidates:
        k = cand.strip().lower()
        if k in low:
            return low[k]
    # startswith fallback (helps with weird suffixes)
    for actual_lower, actual in low.items():
        for cand in candidates:
            if actual_lower.startswith(cand.strip().lower()):
                return actual
    return None

def load_affiliate_mapping_from_xlsx(xlsx_path: str, sheet_name: str) -> pd.DataFrame:
    """
    Return mapping with columns:
      code_norm, affiliate_ID (from 'ID' or 'affiliate_ID'), type_norm,
      pct_fraction (for 'revenue'/'sale'), fixed_amount (for 'fixed')
    """
    df_sheet = pd.read_excel(xlsx_path, sheet_name=sheet_name, dtype=str)
    cols_lower = {c.lower().strip(): c for c in df_sheet.columns}

    code_col = cols_lower.get("code")
    aff_col  = cols_lower.get("id") or cols_lower.get("affiliate_id")
    type_col = cols_lower.get("type")
    payout_col = (cols_lower.get("payout")
                  or cols_lower.get("new customer payout")
                  or cols_lower.get("old customer payout"))

    if not code_col:
        raise ValueError(f"[{sheet_name}] must contain a 'Code' column.")
    if not aff_col:
        raise ValueError(f"[{sheet_name}] must contain an 'ID' (or 'affiliate_ID') column.")
    if not type_col:
        raise ValueError(f"[{sheet_name}] must contain a 'type' column with values 'revenue'/'sale'/'fixed'.")
    if not payout_col:
        raise ValueError(f"[{sheet_name}] must contain a payout column (e.g., 'payout').")

    payout_raw = df_sheet[payout_col].astype(str).str.replace("%", "", regex=False).str.strip()
    payout_num = pd.to_numeric(payout_raw, errors="coerce")
    type_norm  = df_sheet[type_col].astype(str).str.strip().str.lower().replace({"": None})

    # % for revenue/sale
    pct_fraction = payout_num.where(type_norm.isin(["revenue", "sale"])).apply(
        lambda v: (v / 100.0) if pd.notna(v) and v > 1 else (v if pd.notna(v) else DEFAULT_PCT_IF_MISSING)
    )
    # fixed amount
    fixed_amount = payout_num.where(type_norm.eq("fixed"))

    out = pd.DataFrame({
        "code_norm": df_sheet[code_col].apply(normalize_coupon),
        "affiliate_ID": df_sheet[aff_col].fillna("").astype(str).str.strip(),
        "type_norm": type_norm,
        "pct_fraction": pct_fraction,
        "fixed_amount": fixed_amount
    }).dropna(subset=["code_norm"])

    # last occurrence wins for duplicate codes
    return out.drop_duplicates(subset=["code_norm"], keep="last")

# =======================
# DATE WINDOW
# =======================
end_date = datetime.now().date()
start_date = end_date - timedelta(days=days_back)
today = end_date
print(f"Current date: {end_date}, Start date (days_back={days_back}): {start_date}")

# =======================
# PICK REPORT (prefix-based)
# =======================
report_path = find_matching_xlsx(input_dir, REPORT_PREFIX)
print(f"Using report file: {os.path.basename(report_path)}")

# =======================
# LOAD & CLEAN REPORT
# =======================
df_raw = pd.read_excel(report_path, sheet_name=REPORT_SHEET)

# Normalize headers once
df_raw.columns = [str(c).strip() for c in df_raw.columns]

# Resolve columns (common EN/AR variants)
date_col    = resolve_col(df_raw, ["date", "order date", "action date", "created", "created at", "تاريخ", "التاريخ", "تاريخ الطلب"])
status_col  = resolve_col(df_raw, ["status", "order status", "الحالة"])
price_col   = resolve_col(df_raw, ["price (sar)", "price sar", "price", "amount (sar)", "amount", "total", "subtotal", "قيمة", "السعر"])
coupon_col  = resolve_col(df_raw, ["coupon code", "coupon", "promo code", "voucher", "voucher code", "رمز", "كود", "كوبون"])
country_col = resolve_col(df_raw, ["country", "geo", "الدولة", "بلد"])

# Hard requirements
missing = [nm for nm, col in {
    "date": date_col, "status": status_col, "price": price_col, "coupon": coupon_col
}.items() if not col]
if missing:
    raise KeyError(f"Missing required columns: {missing}. Found columns: {list(df_raw.columns)}")

# Convert 'date' flexibly
df_raw['__date_parsed'] = df_raw[date_col].apply(convert_date)
before = len(df_raw)
df = df_raw.dropna(subset=['__date_parsed']).copy()
print(f"Total rows before filtering: {before}")
print(f"Rows with invalid dates dropped: {before - len(df)}")

# Filter out cancelled + date range (exclude today to match common pattern)
df = df[df[status_col].astype(str).str.strip().str.lower() != 'cancelled']
df_filtered = df[
    (df['__date_parsed'].dt.date >= start_date) &
    (df['__date_parsed'].dt.date < today)
].copy()
print(f"Rows after filtering cancelled and date range: {len(df_filtered)}")

# =======================
# DERIVED FIELDS
# =======================
# sale_amount (SAR -> USD)
df_filtered['sale_amount'] = pd.to_numeric(df_filtered[price_col], errors='coerce').fillna(0.0) / 3.75

# revenue fixed value
df_filtered['revenue'] = 26.66

# coupon normalization
df_filtered['coupon_norm'] = df_filtered[coupon_col].apply(normalize_coupon)

# geo: keep the original 'country' value (you can map later if needed)
if country_col:
    df_filtered['geo'] = df_filtered[country_col].fillna(GEO_FALLBACK)
else:
    df_filtered['geo'] = GEO_FALLBACK

# =======================
# JOIN AFFILIATE MAPPING (type-aware)
# =======================
map_df = load_affiliate_mapping_from_xlsx(affiliate_xlsx_path, AFFILIATE_SHEET)
df_joined = df_filtered.merge(map_df, how="left", left_on="coupon_norm", right_on="code_norm")

# Missing affiliate?
missing_aff_mask = df_joined['affiliate_ID'].isna() | (df_joined['affiliate_ID'].astype(str).str.strip() == "")

# Normalize fields
df_joined['affiliate_ID'] = df_joined['affiliate_ID'].fillna("").astype(str).str.strip()
df_joined['type_norm'] = df_joined['type_norm'].fillna("revenue")
df_joined['pct_fraction'] = df_joined['pct_fraction'].fillna(DEFAULT_PCT_IF_MISSING)

# =======================
# COMPUTE PAYOUT (by type)
# =======================
payout = pd.Series(0.0, index=df_joined.index)

# revenue-based %
mask_rev = df_joined['type_norm'].str.lower().eq('revenue')
payout.loc[mask_rev] = df_joined.loc[mask_rev, 'revenue'] * df_joined.loc[mask_rev, 'pct_fraction']

# sale-based %
mask_sale = df_joined['type_norm'].str.lower().eq('sale')
payout.loc[mask_sale] = df_joined.loc[mask_sale, 'sale_amount'] * df_joined.loc[mask_sale, 'pct_fraction']

# fixed amount
mask_fixed = df_joined['type_norm'].str.lower().eq('fixed')
payout.loc[mask_fixed] = df_joined.loc[mask_fixed, 'fixed_amount'].fillna(0.0)

# Enforce: if no affiliate match, set affiliate_id="1", payout=0
payout.loc[missing_aff_mask] = 0.0
df_joined.loc[missing_aff_mask, 'affiliate_ID'] = FALLBACK_AFFILIATE_ID

df_joined['payout'] = payout.round(2)

# =======================
# BUILD FINAL OUTPUT
# =======================
output_df = pd.DataFrame({
    'offer': OFFER_ID,
    'affiliate_id': df_joined['affiliate_ID'],
    'date': df_joined['__date_parsed'].dt.strftime('%m-%d-%Y'),
    'status': STATUS_DEFAULT,
    'payout': df_joined['payout'],
    'revenue': df_joined['revenue'].round(2),
    'sale amount': df_joined['sale_amount'].round(2),
    'coupon': df_joined['coupon_norm'],
    'geo': df_joined['geo'],
})

# =======================
# SAVE
# =======================
output_df.to_csv(output_file, index=False)

print(f"Saved: {output_file}")
print(
    f"Rows: {len(output_df)} | "
    f"Coupons with no affiliate (set aff={FALLBACK_AFFILIATE_ID}, payout=0): {int(missing_aff_mask.sum())} | "
    f"Type counts -> revenue: {int(mask_rev.sum())}, sale: {int(mask_sale.sum())}, fixed: {int(mask_fixed.sum())}"
)
print(f"Date range processed: {output_df['date'].min() if not output_df.empty else 'N/A'} to {output_df['date'].max() if not output_df.empty else 'N/A'}")
if len(output_df) < len(df_joined):
    print("Warning: Some rows were excluded during output creation.")
