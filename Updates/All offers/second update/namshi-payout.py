import pandas as pd
from datetime import datetime, timedelta
import os
import re

# =======================
# CONFIG
# =======================
<<<<<<< HEAD
days_back = 9
=======
days_back = 50
>>>>>>> 0d89299 (D)
OFFER_ID = 1189
STATUS_DEFAULT = "pending"
DEFAULT_PCT_IF_MISSING = 0.0
FALLBACK_AFFILIATE_ID = "1"

# Files
AFFILIATE_XLSX  = "Offers Coupons.xlsx"
AFFILIATE_SHEET = "Namshi"

# =======================
# PATHS
# =======================
script_dir = os.path.dirname(os.path.abspath(__file__))
updates_dir = os.path.dirname(os.path.dirname(script_dir))
input_dir = os.path.join(updates_dir, 'Input data')
output_dir = os.path.join(updates_dir, 'output data')
os.makedirs(output_dir, exist_ok=True)

affiliate_xlsx_path = os.path.join(input_dir, AFFILIATE_XLSX)
output_file = os.path.join(output_dir, 'namshi.csv')

# =======================
# HELPERS
# =======================
def find_latest_sales_xlsx(directory: str) -> str:
    """
    Pick the most recently modified Excel file whose name matches:
      - sales.xlsx
      - sales (N).xlsx
      - or generally starts with 'sales' (case-insensitive)
    """
    # strict pattern first: sales or sales (number).xlsx
    strict = re.compile(r"^sales(?:\s*\(\d+\))?\.xlsx$", re.IGNORECASE)
    strict_matches = [
        f for f in os.listdir(directory)
        if strict.match(f) and f.lower().endswith(".xlsx")
    ]
    if strict_matches:
        return os.path.join(directory, max(strict_matches, key=lambda f: os.path.getmtime(os.path.join(directory, f))))

    # fallback: any .xlsx that starts with "sales"
    fallback = [
        f for f in os.listdir(directory)
        if f.lower().endswith(".xlsx") and os.path.splitext(f)[0].lower().startswith("sales")
    ]
    if fallback:
        return os.path.join(directory, max(fallback, key=lambda f: os.path.getmtime(os.path.join(directory, f))))

    raise FileNotFoundError("No 'sales*.xlsx' file found in the input data folder.")

def normalize_coupon(x: str) -> str:
    if pd.isna(x):
        return ""
    s = str(x).strip().upper()
    parts = re.split(r"[;,\s]+", s)
    return parts[0] if parts else s

def _coalesce_columns(df: pd.DataFrame, candidates: list[str]) -> pd.Series:
    """Return the first non-empty value across the candidate columns for each row."""
    result = pd.Series(pd.NA, index=df.index, dtype=object)
    for col in candidates:
        if col is None or col not in df.columns:
            continue
        series = df[col]
        clean = series.where(series.notna(), pd.NA)
        clean = clean.where(series.astype(str).str.strip().ne(''), pd.NA)
        result = result.fillna(clean)
    return result


def load_affiliate_mapping_from_xlsx(xlsx_path: str, sheet_name: str) -> pd.DataFrame:
    """
    Returns mapping with new/old payout values, yielding columns:
      code_norm, affiliate_ID, type_norm, pct_new, pct_old, fixed_new, fixed_old.
    Accepts payout as % (for revenue/sale) or fixed amounts (for fixed); if only a
    single payout column exists it is used for both new/old.
    """
    df_sheet = pd.read_excel(xlsx_path, sheet_name=sheet_name, dtype=str)
    cols_lower = {c.lower().strip(): c for c in df_sheet.columns}

    code_candidates = [
        cols_lower.get("code"),
        cols_lower.get("coupon code"),
        cols_lower.get("coupon"),
        cols_lower.get("f"),
    ]
    code_candidates.extend([
        c for c in df_sheet.columns
        if str(c).strip().lower().startswith("code") and c not in code_candidates
    ])
    code_series = _coalesce_columns(df_sheet, [c for c in code_candidates if c])

    aff_candidates = [
        cols_lower.get("id"),
        cols_lower.get("affiliate_id"),
    ]
    aff_candidates.extend([
        c for c in df_sheet.columns
        if str(c).strip().lower().startswith("id") and c not in aff_candidates
    ])
    aff_series = _coalesce_columns(df_sheet, [c for c in aff_candidates if c])

    type_col = cols_lower.get("type")
    payout_col = cols_lower.get("payout")
    new_col = cols_lower.get("new customer payout")
    old_col = cols_lower.get("old customer payout")

    if code_series.isna().all():
        raise ValueError(f"[{sheet_name}] must contain a 'Code' column.")
    if aff_series.isna().all():
        raise ValueError(f"[{sheet_name}] must contain an 'ID' (or 'affiliate_ID') column.")
    if not type_col:
        raise ValueError(f"[{sheet_name}] must contain a 'type' column (revenue/sale/fixed).")
    if not (payout_col or new_col or old_col):
        raise ValueError(
            f"[{sheet_name}] must contain at least one payout column (e.g., 'payout', 'new customer payout')."
        )

    def extract_numeric(col_name: str) -> pd.Series:
        if not col_name:
            return pd.Series([pd.NA] * len(df_sheet), dtype="Float64")
        raw = df_sheet[col_name].astype(str).str.replace("%", "", regex=False).str.strip()
        return pd.to_numeric(raw, errors="coerce")

    payout_any = extract_numeric(payout_col)
    payout_new_raw = extract_numeric(new_col).fillna(payout_any)
    payout_old_raw = extract_numeric(old_col).fillna(payout_any)

    type_norm = df_sheet[type_col].astype(str).str.strip().str.lower().replace({"": None}).fillna("revenue")

    def pct_from(values: pd.Series, type_series: pd.Series) -> pd.Series:
        pct = values.where(type_series.isin(["revenue", "sale"]))
        return pct.apply(
            lambda v: (v / 100.0) if pd.notna(v) and v > 1 else (v if pd.notna(v) else pd.NA)
        )

    def fixed_from(values: pd.Series, type_series: pd.Series) -> pd.Series:
        return values.where(type_series.eq("fixed"))

    pct_new = pct_from(payout_new_raw, type_norm)
    pct_old = pct_from(payout_old_raw, type_norm)
    pct_new = pct_new.fillna(pct_old)
    pct_old = pct_old.fillna(pct_new)

    pct_new = pd.to_numeric(pct_new, errors='coerce')
    pct_old = pd.to_numeric(pct_old, errors='coerce')

    fixed_new = fixed_from(payout_new_raw, type_norm)
    fixed_old = fixed_from(payout_old_raw, type_norm)
    fixed_new = fixed_new.fillna(fixed_old)
    fixed_old = fixed_old.fillna(fixed_new)

    fixed_new = pd.to_numeric(fixed_new, errors='coerce')
    fixed_old = pd.to_numeric(fixed_old, errors='coerce')

    mapping_frames: list[pd.DataFrame] = []

    def append_mapping(code_col: str, id_candidates: list[str]) -> None:
        if code_col not in df_sheet.columns:
            return
        codes = df_sheet[code_col]
        ids = _coalesce_columns(df_sheet, id_candidates)
        frame = pd.DataFrame({
            "code_norm": codes.apply(normalize_coupon),
            "affiliate_ID": ids.fillna("").astype(str).str.strip(),
            "type_norm": type_norm,
            "pct_new": pct_new.fillna(DEFAULT_PCT_IF_MISSING),
            "pct_old": pct_old.fillna(DEFAULT_PCT_IF_MISSING),
            "fixed_new": fixed_new,
            "fixed_old": fixed_old,
        })
        mapping_frames.append(frame)

    append_mapping('F', ['ID'])
    append_mapping(cols_lower.get('code'), [cols_lower.get('id.1')])
    append_mapping('Code.1', ['ID.2', 'Unnamed: 19'])

    if not mapping_frames:
        raise ValueError(f"[{sheet_name}] did not yield any coupon mapping columns.")

    out = pd.concat(mapping_frames, ignore_index=True)
    out = out.dropna(subset=['code_norm'])
    out['code_norm'] = out['code_norm'].astype(str).str.strip()
    out = out[out['code_norm'].str.len() > 0]

    # Strip placeholder affiliate IDs so blanks can't overwrite real mappings.
    def _clean_affiliate(value: object) -> str:
        if pd.isna(value):
            return ""
        s = str(value).strip()
        if not s or s == "-" or s.lower() in {"nan", "none"}:
            return ""
        return s

    out['affiliate_ID'] = out['affiliate_ID'].map(_clean_affiliate)

    # Drop later rows that only carry blanks when a code already has a valid affiliate.
    has_affiliate = out['affiliate_ID'].ne("")
    coded_with_aff = set(out.loc[has_affiliate, 'code_norm'])
    out = out[~(~has_affiliate & out['code_norm'].isin(coded_with_aff))]

    return out.drop_duplicates(subset=['code_norm'], keep='first')

# =======================
# LOAD REPORT
# =======================
today = datetime.now().date()
# window includes "today" by making end_date = today + 1 and filtering < end_date
end_date = today + timedelta(days=1)
start_date = end_date - timedelta(days=days_back + 1)

print(f"Current date: {today}, Start date (days_back={days_back}): {start_date}")

input_file = find_latest_sales_xlsx(input_dir)
print(f"Using input file: {os.path.basename(input_file)}")

df = pd.read_excel(input_file)

# Filter for Namshi only (robust to stray spaces/case)
df = df[df['Advertiser'].astype(str).str.strip().str.casefold() == 'namshi'].copy()

# Date filter: include dates >= start_date and < end_date
df['Order Date'] = pd.to_datetime(df['Order Date'], errors='coerce')
df = df.dropna(subset=['Order Date'])
df = df[(df['Order Date'].dt.date >= start_date) & (df['Order Date'].dt.date < end_date)].copy()

# =======================
# EXPAND FTU / RTU
# =======================
# FTU
ftu_repeat_idx = pd.to_numeric(df['FTU Orders'], errors='coerce').fillna(0).astype(int)
ftu = df.loc[df.index.repeat(ftu_repeat_idx)].copy()
ftu = ftu[ftu['FTU Orders'] > 0]
ftu['sale_amount'] = (
    pd.to_numeric(ftu['FTU Order Values'], errors='coerce').fillna(0.0) /
    pd.to_numeric(ftu['FTU Orders'], errors='coerce').replace(0, pd.NA).fillna(1).astype(float)
) / 3.67
ftu['revenue'] = ftu['sale_amount'] * 0.08
ftu['order_date'] = ftu['Order Date']
ftu['coupon_code'] = ftu['Coupon Code']
ftu['Country'] = ftu['Country']
ftu['customer_type'] = 'new'

# RTU
rtu_repeat_idx = pd.to_numeric(df['RTU Orders'], errors='coerce').fillna(0).astype(int)
rtu = df.loc[df.index.repeat(rtu_repeat_idx)].copy()
rtu = rtu[rtu['RTU Orders'] > 0]
rtu['sale_amount'] = (
    pd.to_numeric(rtu['RTU Order Value'], errors='coerce').fillna(0.0) /
    pd.to_numeric(rtu['RTU Orders'], errors='coerce').replace(0, pd.NA).fillna(1).astype(float)
) / 3.67
rtu['revenue'] = rtu['sale_amount'] * 0.025
rtu['order_date'] = rtu['Order Date']
rtu['coupon_code'] = rtu['Coupon Code']
rtu['Country'] = rtu['Country']
rtu['customer_type'] = 'old'

# Combine
df_expanded = pd.concat([ftu, rtu], ignore_index=True)
df_expanded['coupon_norm'] = df_expanded['coupon_code'].apply(normalize_coupon)

# =======================
# GEO MAP
# =======================
geo_mapping = {'SA': 'ksa', 'AE': 'uae', 'BH': 'bhr', 'KW': 'kwt'}
df_expanded['geo'] = df_expanded['Country'].map(geo_mapping).fillna(df_expanded['Country'])

# =======================
# JOIN AFFILIATE MAPPING (type-aware)
# =======================
map_df = load_affiliate_mapping_from_xlsx(affiliate_xlsx_path, AFFILIATE_SHEET)
df_joined = df_expanded.merge(map_df, how="left", left_on="coupon_norm", right_on="code_norm")

# Missing affiliate?
missing_aff_mask = df_joined['affiliate_ID'].isna() | (df_joined['affiliate_ID'].astype(str).str.strip() == "")

# Normalize mapping fields
df_joined['affiliate_ID']  = df_joined['affiliate_ID'].fillna("").astype(str).str.strip()
df_joined['type_norm']     = df_joined['type_norm'].fillna("revenue")
for col in ['pct_new', 'pct_old']:
    df_joined[col] = pd.to_numeric(df_joined[col], errors='coerce').fillna(DEFAULT_PCT_IF_MISSING)
for col in ['fixed_new', 'fixed_old']:
    df_joined[col] = pd.to_numeric(df_joined[col], errors='coerce')

is_new_customer = df_joined['customer_type'].astype(str).str.lower().eq('new')
pct_effective = df_joined['pct_new'].where(is_new_customer, df_joined['pct_old'])
pct_effective = pd.to_numeric(pct_effective, errors='coerce').fillna(DEFAULT_PCT_IF_MISSING)
fixed_effective = df_joined['fixed_new'].where(is_new_customer, df_joined['fixed_old'])
fixed_effective = pd.to_numeric(fixed_effective, errors='coerce').fillna(0.0)

# =======================
# PAYOUT CALC
# =======================
# Explicit overrides: enforce exact 88% for specific coupons
# (applies only to percentage-based types: revenue/sale)
override_codes = {"GBT", "GBK", "GKP"}
override_mask_pct = (
    df_joined['coupon_norm'].isin(override_codes)
    & df_joined['type_norm'].str.lower().isin(['revenue', 'sale'])
)
if override_mask_pct.any():
    pct_effective.loc[override_mask_pct] = 0.88

payout = pd.Series(0.0, index=df_joined.index)

mask_rev   = df_joined['type_norm'].str.lower().eq('revenue')
mask_sale  = df_joined['type_norm'].str.lower().eq('sale')
mask_fixed = df_joined['type_norm'].str.lower().eq('fixed')

payout.loc[mask_rev]   = df_joined.loc[mask_rev, 'revenue']     * pct_effective.loc[mask_rev]
payout.loc[mask_sale]  = df_joined.loc[mask_sale, 'sale_amount'] * pct_effective.loc[mask_sale]
payout.loc[mask_fixed] = fixed_effective.loc[mask_fixed]

# Enforce no-match rule
payout.loc[missing_aff_mask] = 0.0
df_joined.loc[missing_aff_mask, 'affiliate_ID'] = FALLBACK_AFFILIATE_ID

df_joined['payout'] = payout.round(2)

# =======================
# BUILD OUTPUT (NEW STRUCTURE)
# =======================
output_df = pd.DataFrame({
    'offer': OFFER_ID,
    'affiliate_id': df_joined['affiliate_ID'],
    'date': pd.to_datetime(df_joined['order_date']).dt.strftime('%m-%d-%Y'),
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
    f"No-affiliate coupons (set aff={FALLBACK_AFFILIATE_ID}, payout=0): {int(missing_aff_mask.sum())}"
)
print(f"Date range processed: {start_date} to {end_date - timedelta(days=1)}")
