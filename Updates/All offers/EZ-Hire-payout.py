import os
import re
from datetime import datetime, timedelta
from typing import Optional, Tuple

import pandas as pd




OFFER_ID = 1352
STATUS_DEFAULT = "pending"
FALLBACK_AFFILIATE_ID = "1"
DEFAULT_PCT_IF_MISSING = 0.0

REPORT_PREFIX = "Bookings With Publisher Promo"   
COUPONS_PREFIX = "Offers Coupons"                  
AFFILIATE_SHEET = "EZ Hire"                        

OUTPUT_CSV = "ez-hire.csv"

FX_DIVISOR = 3.67
BOOKING_TYPE_RATES = {
    "monthly": 0.02,
    "weekly": 0.0325,
    "daily": 0.03,
}




script_dir = os.path.dirname(os.path.abspath(__file__))
input_dir = os.path.join(script_dir, '..', 'input data')
output_dir = os.path.join(script_dir, '..', 'output data')
os.makedirs(output_dir, exist_ok=True)




days_back = 3
today = datetime.now().date()
end_date = today
start_date = end_date - timedelta(days=days_back)
print(f"Window: {start_date} ≤ date < {end_date}  (days_back={days_back}, excl. today)")




def find_matching_file(directory: str, prefix: str, exts=(".csv", ".xlsx", ".xls")) -> str:
    """Return exact match '<prefix>.<ext>' if present, else newest file whose base starts with prefix."""
    prefix_lower = prefix.lower()
    candidates = []
    for fname in os.listdir(directory):
        if fname.startswith("~$"):
            continue
        lower = fname.lower()
        if not lower.endswith(exts):
            continue
        base = os.path.splitext(lower)[0]
        if base.startswith(prefix_lower):
            candidates.append(os.path.join(directory, fname))
    if not candidates:
        avail = [f for f in os.listdir(directory) if f.lower().endswith(exts)]
        raise FileNotFoundError(
            f"No file starting with '{prefix}' found in: {directory}\n"
            f"Available: {avail}"
        )
    for ext in exts:
        exact = os.path.join(directory, prefix + ext)
        if os.path.exists(exact):
            return exact
    return max(candidates, key=os.path.getmtime)


def read_any(path: str) -> pd.DataFrame:
    lower = path.lower()
    if lower.endswith(".csv"):
        return pd.read_csv(path)
    if lower.endswith((".xlsx", ".xls")):
        return pd.read_excel(path)
    raise ValueError(f"Unsupported file type: {path}")


def normalize_coupon(x: str) -> str:
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
    return pd.to_numeric(
        series.astype(str)
              .str.replace(",", "", regex=False)
              .str.replace("SAR", "", case=False, regex=False)
              .str.replace("AED", "", case=False, regex=False)
              .str.strip(),
        errors="coerce"
    )


def pick_col(df: pd.DataFrame, *cands) -> Optional[str]:
    norm = {str(c).strip().lower(): c for c in df.columns}
    # exact
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


def revenue_pct_from_booking_type(value: str) -> float:
    return BOOKING_TYPE_RATES.get(str(value).strip().lower(), BOOKING_TYPE_RATES['daily'])


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


def find_any_coupons_workbook(directory: str, prefix: str) -> Tuple[str, str]:
    """Return (path, sheet_name_hint) — sheet_name_hint is just what you configured."""
    path = find_matching_file(directory, prefix, exts=(".xlsx", ".xls"))
    return path, AFFILIATE_SHEET


# =======================
# LOAD FILES
# =======================
report_path = find_matching_file(input_dir, REPORT_PREFIX, exts=(".csv", ".xlsx", ".xls"))
print(f"Using report file: {report_path}")
df_raw = read_any(report_path)

coupons_path, coupons_sheet = find_any_coupons_workbook(input_dir, COUPONS_PREFIX)
print(f"Using coupons workbook: {coupons_path} | sheet: {coupons_sheet}")

# =======================
# SCHEMA DETECTION
# =======================
cols_lower = {c.lower() for c in df_raw.columns}
is_processed = ("sale_amount" in cols_lower) or ({"datetime", "revenue"} <= cols_lower)

# Column candidates seen across your exports
date_cands    = ["datetime", "order date", "transaction date", "process date", "date", "booking date"]
amount_cands  = ["sale_amount", "order value (aed)", "order value", "amount", "total", "booking amount", "total amount"]
coupon_cands  = ["publisher promo", "promo code", "promo", "coupon", "coupon code", "affiliate_info1", "voucher", "code"]
status_cands  = ["booking_status", "status"]
offer_cands   = ["offer_id"]
geo_cands     = ["geo", "country", "market"]
booking_type_cands = ["booking type", "type", "booking_type"]

# =======================
# NORMALIZE & FILTER
# =======================
df = df_raw.copy()

status_col = pick_col(df, *status_cands)
if status_col:
    cancelled_mask = df[status_col].astype(str).str.contains('cancel', case=False, na=False)
    cancelled_count = int(cancelled_mask.sum())
    if cancelled_count:
        print(f"Dropped {cancelled_count} cancelled rows based on '{status_col}'.")
    df = df.loc[~cancelled_mask].copy()
else:
    print("WARNING: No booking status column found; cancellation filter skipped.")

date_col = pick_col(df, *date_cands)
if not date_col:
    raise KeyError(f"Missing date column. Tried: {date_cands}. Found: {list(df.columns)}")

df["Order Date"] = pd.to_datetime(df[date_col], errors="coerce")
df = df.dropna(subset=["Order Date"])
df = df[(df["Order Date"].dt.date >= start_date) & (df["Order Date"].dt.date < end_date)].copy()
print(f"Rows after date filter: {len(df)}")

if df.empty:
    print("No rows to process after date/cancellation filters.")

# =======================
# SALE AMOUNT (USD) & REVENUE
# =======================
sale_col = pick_col(df, *amount_cands)
if not sale_col:
    raise KeyError(f"Missing amount column. Tried: {amount_cands}. Found: {list(df.columns)}")

df["sale_amount"] = to_number(df[sale_col]).fillna(0.0) / FX_DIVISOR

booking_type_col = pick_col(df, *booking_type_cands)
if booking_type_col:
    df["booking_type_norm"] = df[booking_type_col].astype(str).str.strip().str.lower()
else:
    df["booking_type_norm"] = "daily"

df["revenue_pct"] = df["booking_type_norm"].apply(revenue_pct_from_booking_type)
df["revenue"] = (df["sale_amount"] * df["revenue_pct"]).round(6)

# =======================
# COUPON / CODE → AFFILIATE MAPPING
# =======================
coupon_col = pick_col(df, *coupon_cands)
if coupon_col:
    df["coupon_norm"] = df[coupon_col].apply(normalize_coupon)
else:
    df["coupon_norm"] = ""

map_df = load_affiliate_mapping_from_xlsx(coupons_path, coupons_sheet) if df["coupon_norm"].str.len().gt(0).any() else pd.DataFrame()
if not map_df.empty:
    df = df.merge(map_df, how="left", left_on="coupon_norm", right_on="code_norm")
    df["affiliate_ID"] = df["affiliate_ID"].fillna("").astype(str).str.strip()
    df["type_norm"] = df.get("type_norm", "revenue").fillna("revenue")
    for col in ["pct_new", "pct_old"]:
        df[col] = pd.to_numeric(df.get(col), errors='coerce').fillna(DEFAULT_PCT_IF_MISSING)
    for col in ["fixed_new", "fixed_old"]:
        df[col] = pd.to_numeric(df.get(col), errors='coerce')
    is_new_customer = infer_is_new_customer(df)
    pct_effective = df['pct_new'].where(is_new_customer, df['pct_old'])
    df['pct_fraction'] = pd.to_numeric(pct_effective, errors='coerce').fillna(DEFAULT_PCT_IF_MISSING)
    fixed_effective = df['fixed_new'].where(is_new_customer, df['fixed_old'])
    df['fixed_amount'] = pd.to_numeric(fixed_effective, errors='coerce')
else:
    if "affiliate_ID" not in df.columns:
        df["affiliate_ID"] = ""

missing_aff = df["affiliate_ID"].isna() | (df["affiliate_ID"].astype(str).str.strip() == "")
if missing_aff.any():
    print("Note: some rows have no mapped affiliate; applying fallback and zero payout.")
    df.loc[missing_aff, "affiliate_ID"] = FALLBACK_AFFILIATE_ID

# =======================
# PAYOUT (type-aware, if mapping provided)
# =======================
payout = pd.Series(0.0, index=df.index)
if {"type_norm", "pct_fraction"}.issubset(df.columns) or "fixed_amount" in df.columns:
    mask_rev   = df.get("type_norm", "").astype(str).str.lower().eq("revenue")
    mask_sale  = df.get("type_norm", "").astype(str).str.lower().eq("sale")
    mask_fixed = df.get("type_norm", "").astype(str).str.lower().eq("fixed")

    payout.loc[mask_rev]   = df.loc[mask_rev, "revenue"].fillna(0.0)      * df.loc[mask_rev, "pct_fraction"].fillna(DEFAULT_PCT_IF_MISSING)
    payout.loc[mask_sale]  = df.loc[mask_sale, "sale_amount"].fillna(0.0) * df.loc[mask_sale, "pct_fraction"].fillna(DEFAULT_PCT_IF_MISSING)
    payout.loc[mask_fixed] = df.loc[mask_fixed, "fixed_amount"].fillna(0.0)

    payout.loc[missing_aff] = 0.0

df["payout"] = payout.round(2)

# =======================
# GEO & STATUS & OFFER
# =======================
df["geo_out"] = "no-geo"

# Always force payout export status to the configured default.
df["status_out"] = STATUS_DEFAULT

offer_vals = OFFER_ID

# =======================
# BUILD OUTPUT
# =======================
output_df = pd.DataFrame({
    "offer": offer_vals,
    "affiliate_id": df["affiliate_ID"].astype(str),
    "date": df["Order Date"].dt.strftime("%m-%d-%Y"),
    "status": df["status_out"],
    "payout": df["payout"],
    "revenue": df["revenue"].round(2),
    "sale amount": df["sale_amount"].round(2),
    "coupon": df.get("coupon_norm", ""),
    "geo": df["geo_out"],
})

# =======================
# SAVE
# =======================
out_path = os.path.join(output_dir, OUTPUT_CSV)
output_df.to_csv(out_path, index=False)

print(f"Saved: {out_path}")
print(f"Rows: {len(output_df)} | Fallback affiliates: {int((output_df['affiliate_id'] == FALLBACK_AFFILIATE_ID).sum())}")
print(f"FX divisor: {FX_DIVISOR}")
if not output_df.empty:
    print(f"Revenue % by booking type (fraction): {sorted(set(df['revenue_pct']))}")
    print(f"Date range processed: {output_df['date'].min()} → {output_df['date'].max()}")
