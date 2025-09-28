import pandas as pd
from datetime import datetime, timedelta
import os
import re
from typing import Optional, Tuple

# =======================
# CONFIG — EDIT HERE
# =======================
OFFER_ID = 1352
STATUS_DEFAULT = "pending"
FALLBACK_AFFILIATE_ID = "1"
DEFAULT_PCT_IF_MISSING = 0.0

# Choose aggregation level to drive revenue% rule: "daily", "weekly", or "monthly"
AGG_LEVEL = "daily"   # <-- change to "weekly" or "monthly" when needed

# Input autodetect:
#   - Report: will match newest file starting with this prefix (CSV/XLS/XLSX)
#   - Coupons workbook: will match first Excel file starting with this prefix
REPORT_PREFIX = "Bookings With Publisher Promo"   # matches your uploaded xlsx
COUPONS_PREFIX = "Offers Coupons"                 # matches "Offers Coupons (7).xlsx" etc.
AFFILIATE_SHEET = "EZ Hire"                       # change to the exact tab name

OUTPUT_CSV = "ezhire.csv"

# FX (AED -> USD)
FX_DIVISOR = 3.67

# =======================
# PATHS
# =======================
script_dir = os.path.dirname(os.path.abspath(__file__))
input_dir  = os.path.join(script_dir, '..', 'input data')
output_dir = os.path.join(script_dir, '..', 'output data')
os.makedirs(output_dir, exist_ok=True)

# =======================
# DATE WINDOW (last 3 days, excl. today)
# =======================
days_back = 3
today = datetime.now().date()
end_date = today
start_date = end_date - timedelta(days=days_back)
print(f"Window: {start_date} ≤ date < {end_date}  (days_back={days_back}, excl. today)")

# =======================
# HELPERS
# =======================
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
    # prefer exact
    for ext in exts:
        exact = os.path.join(directory, prefix + ext)
        if os.path.exists(exact):
            return exact
    return max(candidates, key=os.path.getmtime)

def read_any(path: str) -> pd.DataFrame:
    lower = path.lower()
    if lower.endswith(".csv"):
        return pd.read_csv(path)
    if lower.endswith(".xlsx") or lower.endswith(".xls"):
        return pd.read_excel(path)
    raise ValueError(f"Unsupported file type: {path}")

def normalize_coupon(x: str) -> str:
    if pd.isna(x):
        return ""
    s = str(x).replace("\u00A0", " ").strip().upper()
    parts = re.split(r"[;,\s]+", s)
    return parts[0] if parts else s

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

def revenue_pct_for(level: str) -> float:
    level = (level or "").strip().lower()
    if level == "monthly":
        return 0.02
    if level == "weekly":
        return 0.0325
    # default daily
    return 0.03

def load_affiliate_mapping_from_xlsx(xlsx_path: str, sheet_name: str) -> pd.DataFrame:
    df_sheet = pd.read_excel(xlsx_path, sheet_name=sheet_name, dtype=str)

    code_col   = pick_col(df_sheet, "code", "coupon code", "coupon", "promo", "promo code")
    aff_col    = pick_col(df_sheet, "id", "affiliate_id", "affiliate id")
    type_col   = pick_col(df_sheet, "type", "payout type", "commission type")
    payout_col = pick_col(df_sheet, "payout", "new customer payout", "old customer payout", "commission", "rate")

    if not code_col:
        raise ValueError(f"[{sheet_name}] must contain a 'Code' (or Promo/Coupon) column.")
    if not aff_col:
        raise ValueError(f"[{sheet_name}] must contain an 'ID' (or 'affiliate_ID') column.")
    if not type_col:
        raise ValueError(f"[{sheet_name}] must contain a 'type' column (revenue/sale/fixed).")
    if not payout_col:
        raise ValueError(f"[{sheet_name}] must contain a 'payout' column.")

    payout_raw = df_sheet[payout_col].astype(str).str.replace("%", "", regex=False).str.strip()
    payout_num = pd.to_numeric(payout_raw, errors="coerce")
    type_norm  = df_sheet[type_col].astype(str).str.strip().str.lower().replace({"": None})

    pct_fraction = payout_num.where(type_norm.isin(["revenue", "sale"])).apply(
        lambda v: (v/100.0) if pd.notna(v) and v > 1 else (v if pd.notna(v) else DEFAULT_PCT_IF_MISSING)
    )
    fixed_amount = payout_num.where(type_norm.eq("fixed"))

    out = (
        pd.DataFrame({
            "code_norm": df_sheet[code_col].apply(normalize_coupon),
            "affiliate_ID": df_sheet[aff_col].fillna("").astype(str).str.strip(),
            "type_norm": type_norm.fillna("revenue"),
            "pct_fraction": pct_fraction.fillna(DEFAULT_PCT_IF_MISSING),
            "fixed_amount": fixed_amount
        })
        .dropna(subset=["code_norm"])
    )

    # prefer rows with affiliate_ID
    out["has_aff"] = out["affiliate_ID"].astype(str).str.len() > 0
    out = (
        out.sort_values(by=["code_norm", "has_aff"], ascending=[True, False])
           .drop_duplicates(subset=["code_norm"], keep="first")
           .drop(columns=["has_aff"])
    )
    return out

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
status_cands  = ["status"]
offer_cands   = ["offer_id"]
geo_cands     = ["geo", "country", "market"]

# =======================
# NORMALIZE & FILTER DATES
# =======================
date_col = pick_col(df_raw, *date_cands)
if not date_col:
    raise KeyError(f"Missing date column. Tried: {date_cands}. Found: {list(df_raw.columns)}")

df = df_raw.copy()
df["Order Date"] = pd.to_datetime(df[date_col], errors="coerce")
df = df.dropna(subset=["Order Date"])
df = df[(df["Order Date"].dt.date >= start_date) & (df["Order Date"].dt.date < end_date)].copy()
print(f"Rows after date filter: {len(df)}")

# =======================
# SALE AMOUNT (USD) & REVENUE
# =======================
# Amount source (processed or raw)
sale_col = pick_col(df, *amount_cands)
if not sale_col:
    raise KeyError(f"Missing amount column. Tried: {amount_cands}. Found: {list(df.columns)}")

# Always convert AED -> USD per your rule
df["sale_amount"] = to_number(df[sale_col]).fillna(0.0) / FX_DIVISOR

# Revenue percent based on aggregation level
rev_pct = revenue_pct_for(AGG_LEVEL)
df["revenue"] = (df["sale_amount"] * rev_pct).round(6)  # keep precision before final rounding

# =======================
# COUPON / CODE → AFFILIATE MAPPING
# =======================
coupon_col = pick_col(df_raw, *coupon_cands)
if coupon_col:
    df["coupon_norm"] = df_raw[coupon_col].apply(normalize_coupon)
else:
    df["coupon_norm"] = ""

map_df = load_affiliate_mapping_from_xlsx(coupons_path, coupons_sheet) if df["coupon_norm"].str.len().gt(0).any() else pd.DataFrame()
if not map_df.empty:
    df = df.merge(map_df, how="left", left_on="coupon_norm", right_on="code_norm")

# Affiliate fallback
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

    # zero payout for any row using fallback affiliate
    payout.loc[missing_aff] = 0.0

df["payout"] = payout.round(2)

# =======================
# GEO & STATUS & OFFER
# =======================
# Your rule: geo always no-geo
df["geo_out"] = "no-geo"

# status (use file if present, else default)
status_col = pick_col(df_raw, *status_cands)
if status_col:
    status_series = df[status_col].fillna(STATUS_DEFAULT).astype(str).str.strip()
    status_series = status_series.replace("", STATUS_DEFAULT)
    df["status_out"] = status_series
else:
    df["status_out"] = STATUS_DEFAULT

# offer: force to 1352 (your rule)
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
print(f"Aggregation level: {AGG_LEVEL} | revenue%: {rev_pct*100:.2f}% | FX divisor: {FX_DIVISOR}")
if not output_df.empty:
    print(f"Date range processed: {output_df['date'].min()} → {output_df['date'].max()}")
