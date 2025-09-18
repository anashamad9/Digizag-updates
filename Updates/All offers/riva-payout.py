import pandas as pd
from datetime import datetime, timedelta
import os
import re
from typing import Optional

# =======================
# CONFIG (Riva Fashion)
# =======================
days_back = 2                               # previous N days INCLUDING today
OFFER_ID = 1183
STATUS_DEFAULT = "pending"
DEFAULT_PCT_IF_MISSING = 0.0
FALLBACK_AFFILIATE_ID = "1"
GEO_DEFAULT = "no-geo"

# Files (dynamic report name)
REPORT_PREFIX   = "sales-DigiZag-"         # e.g., sales-DigiZag-2025-09-16__2025-09-17.csv
AFFILIATE_XLSX  = "Offers Coupons.xlsx"
AFFILIATE_SHEET = "Riva Fashion"           # adjust if your tab is named differently
OUTPUT_CSV      = "RivaFashion.csv"

# Currency / business rules
FINAL_TOTAL_TO_USD_MULT = 3.26             # sale_amount = FINAL_TOTAL * 3.26
NEW_RATE = 0.10
OLD_RATE = 0.07

# =======================
# PATHS
# =======================
script_dir = os.path.dirname(os.path.abspath(__file__))
input_dir  = os.path.join(script_dir, "..", "input data")
output_dir = os.path.join(script_dir, "..", "output data")
os.makedirs(output_dir, exist_ok=True)

affiliate_xlsx_path = os.path.join(input_dir, AFFILIATE_XLSX)
output_file         = os.path.join(output_dir, OUTPUT_CSV)

# =======================
# DATE WINDOW (incl. today)
# =======================
today = datetime.now().date()
end_exclusive = today + timedelta(days=1)                # exclude tomorrow
start_date = end_exclusive - timedelta(days=days_back)   # inclusive
print(f"Window: {start_date} ≤ date < {end_exclusive}  (days_back={days_back}, incl. today)")

# =======================
# HELPERS
# =======================
def find_matching_csv(directory: str, prefix: str) -> str:
    """
    Pick a CSV whose base filename starts with `prefix` (case-insensitive).
    Prefer files that match the pattern:
        sales-DigiZag-YYYY-MM-DD__YYYY-MM-DD.csv
    Choose the one with the latest (end_date, start_date).
    If none match the pattern, fall back to newest by mtime.
    """
    prefix_lower = prefix.lower()
    csvs = []
    for fname in os.listdir(directory):
        if fname.startswith("~$") or not fname.lower().endswith(".csv"):
            continue
        if os.path.splitext(fname)[0].lower().startswith(prefix_lower):
            csvs.append(os.path.join(directory, fname))
    if not csvs:
        raise FileNotFoundError(f"No .csv starting with '{prefix}' found in: {directory}")

    pat = re.compile(r"^sales-DigiZag-(\d{4}-\d{2}-\d{2})__(\d{4}-\d{2}-\d{2})\.csv$", re.IGNORECASE)
    dated = []
    fallback = []
    for p in csvs:
        m = pat.match(os.path.basename(p))
        if m:
            try:
                s = datetime.strptime(m.group(1), "%Y-%m-%d").date()
                e = datetime.strptime(m.group(2), "%Y-%m-%d").date()
                dated.append((e, s, p))
            except Exception:
                fallback.append(p)
        else:
            fallback.append(p)

    if dated:
        dated.sort(key=lambda t: (t[0], t[1]), reverse=True)  # newest end_date then start_date
        return dated[0][2]
    # fallback to newest by mtime
    return max(fallback, key=os.path.getmtime)

def normalize_coupon(x: str) -> str:
    """Uppercase, trim, NBSP→space, take first token if multiple separated by ; , or whitespace."""
    if pd.isna(x):
        return ""
    s = str(x).replace("\u00A0", " ").strip().upper()
    parts = re.split(r"[;,\s]+", s)
    return parts[0] if parts else s

def to_number(series: pd.Series) -> pd.Series:
    """Robust numeric coercion."""
    return pd.to_numeric(
        series.astype(str)
              .str.replace(",", "", regex=False)
              .str.replace("$", "", regex=False)
              .str.strip(),
        errors="coerce"
    )

def pick_col(df: pd.DataFrame, *cands) -> Optional[str]:
    """Case/space-insensitive header resolver with startswith fallback."""
    norm = {str(c).strip().lower(): c for c in df.columns}
    # exact
    for cand in cands:
        key = str(cand).strip().lower()
        if key in norm:
            return norm[key]
    # startswith
    for cand in cands:
        key = str(cand).strip().lower()
        for low, actual in norm.items():
            if low.startswith(key):
                return actual
    return None

def load_affiliate_mapping_from_xlsx(xlsx_path: str, sheet_name: str) -> pd.DataFrame:
    """
    Return mapping with: code_norm, affiliate_ID, type_norm, pct_fraction, fixed_amount.
    Accepts 'ID' or 'affiliate_ID'. Payout can be % (for revenue/sale) or fixed (for fixed).
    """
    df_sheet = pd.read_excel(xlsx_path, sheet_name=sheet_name, dtype=str)
    code_col   = pick_col(df_sheet, "code", "coupon code", "coupon")
    aff_col    = pick_col(df_sheet, "id", "affiliate_id", "affiliate id")
    type_col   = pick_col(df_sheet, "type", "payout type", "commission type")
    payout_col = pick_col(df_sheet, "payout", "new customer payout", "old customer payout", "commission", "rate")

    if not code_col:
        raise ValueError(f"[{sheet_name}] must contain a 'Code' column.")
    if not aff_col:
        raise ValueError(f"[{sheet_name}] must contain an 'ID' (or 'affiliate_ID') column.")
    if not type_col:
        raise ValueError(f"[{sheet_name}] must contain a 'type' column (revenue/sale/fixed).")
    if not payout_col:
        raise ValueError(f"[{sheet_name}] must contain a payout-like column (e.g., 'payout').")

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

    # Prefer rows with an affiliate_ID if duplicates exist
    out["has_aff"] = out["affiliate_ID"].astype(str).str.len() > 0
    out = (
        out.sort_values(by=["code_norm", "has_aff"], ascending=[True, False])
           .drop_duplicates(subset=["code_norm"], keep="first")
           .drop(columns=["has_aff"])
    )
    return out

# =======================
# LOAD REPORT (dynamic)
# =======================
input_file = find_matching_csv(input_dir, REPORT_PREFIX)
print(f"Using report file: {os.path.basename(input_file)}")

df_raw = pd.read_csv(input_file)
df_raw.columns = [c.strip() for c in df_raw.columns]
if df_raw.columns.duplicated().any():
    dup_cols = df_raw.columns[df_raw.columns.duplicated()].unique().tolist()
    print(f"Warning: duplicate columns detected {dup_cols}; keeping first occurrence.")
    df_raw = df_raw.loc[:, ~df_raw.columns.duplicated()]

# Flexible headers (handles common typos like "Puchase Date")
date_col    = pick_col(df_raw, "puchase date", "purchase date", "order date", "date")
total_col   = pick_col(df_raw, "final_total", "final total", "total")
ctype_col   = pick_col(df_raw, "customer_type", "customer type", "user type")
coupon_col  = pick_col(df_raw, "coupon code", "coupon", "code")
country_col = pick_col(df_raw, "country", "market", "geo")
status_col  = pick_col(df_raw, "status", "order status")  # optional

missing = [n for n, c in {
    "Date": date_col,
    "FINAL_TOTAL": total_col,
    "Customer_Type": ctype_col,
    "Coupon Code": coupon_col,
    "Country": country_col
}.items() if c is None]
if missing:
    raise KeyError(f"Missing expected column(s): {missing}. Columns present: {list(df_raw.columns)}")

df = df_raw.rename(columns={
    date_col:    "Date",
    total_col:   "FINAL_TOTAL",
    ctype_col:   "Customer_Type",
    coupon_col:  "Coupon Code",
    country_col: "Country",
    **({status_col: "Status"} if status_col else {})
})
if df.columns.duplicated().any():
    dup_cols = df.columns[df.columns.duplicated()].unique().tolist()
    print(f"Warning: duplicate standardized columns detected {dup_cols}; keeping first occurrence.")
    df = df.loc[:, ~df.columns.duplicated()]

# Parse dates & filter window (include today)
df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
df = df.dropna(subset=["Date"])
df = df[(df["Date"].dt.date >= start_date) & (df["Date"].dt.date < end_exclusive)].copy()

# Status column is intentionally ignored so every row in the date window is kept

# =======================
# DERIVED FIELDS
# =======================
# sale_amount
df["sale_amount"] = to_number(df["FINAL_TOTAL"]).fillna(0.0) * FINAL_TOTAL_TO_USD_MULT

# revenue: New -> 10%, else 7%
df["Customer_Type"] = df["Customer_Type"].astype(str).str.strip().str.lower()
df["revenue"] = df.apply(
    lambda r: r["sale_amount"] * NEW_RATE if r["Customer_Type"] == "new" else r["sale_amount"] * OLD_RATE,
    axis=1
)

# geo mapping
geo_mapping = {
    "Bahrain": "bhr",
    "Saudi Arabia": "ksa",
    "Kuwait": "kwt",
    "United Arab Emirates": "uae",
    "Oman": "omn",
    "Qatar": "qat",
    "Jordan": "jor",
}
df["geo"] = df["Country"].map(geo_mapping).fillna(GEO_DEFAULT)

# coupon normalized
df["coupon_norm"] = df["Coupon Code"].apply(normalize_coupon)

# =======================
# JOIN AFFILIATE MAP & PAYOUT
# =======================
map_df = load_affiliate_mapping_from_xlsx(affiliate_xlsx_path, AFFILIATE_SHEET)
dfj = df.merge(map_df, how="left", left_on="coupon_norm", right_on="code_norm")

# Normalize mapping fields
missing_aff_mask = dfj["affiliate_ID"].isna() | (dfj["affiliate_ID"].astype(str).str.strip() == "")
dfj["affiliate_ID"] = dfj["affiliate_ID"].fillna("").astype(str).str.strip()
dfj["type_norm"]    = dfj["type_norm"].fillna("revenue")
dfj["pct_fraction"] = dfj["pct_fraction"].fillna(DEFAULT_PCT_IF_MISSING)

# Payout by type
payout = pd.Series(0.0, index=dfj.index)
mask_rev   = dfj["type_norm"].str.lower().eq("revenue")
mask_sale  = dfj["type_norm"].str.lower().eq("sale")
mask_fixed = dfj["type_norm"].str.lower().eq("fixed")

payout.loc[mask_rev]   = dfj.loc[mask_rev, "revenue"]      * dfj.loc[mask_rev, "pct_fraction"]
payout.loc[mask_sale]  = dfj.loc[mask_sale, "sale_amount"] * dfj.loc[mask_sale, "pct_fraction"]
payout.loc[mask_fixed] = dfj.loc[mask_fixed, "fixed_amount"].fillna(0.0)

# Fallback when no affiliate match
payout.loc[missing_aff_mask] = 0.0
dfj.loc[missing_aff_mask, "affiliate_ID"] = FALLBACK_AFFILIATE_ID

dfj["payout"] = payout.round(2)

# =======================
# BUILD OUTPUT (standard schema)
# =======================
output_df = pd.DataFrame({
    "offer":        OFFER_ID,
    "affiliate_id": dfj["affiliate_ID"],
    "date":         dfj["Date"].dt.strftime("%m-%d-%Y"),
    "status":       STATUS_DEFAULT,
    "payout":       dfj["payout"],
    "revenue":      dfj["revenue"].round(2),
    "sale amount":  dfj["sale_amount"].round(2),
    "coupon":       dfj["coupon_norm"],
    "geo":          dfj["geo"],
})

# =======================
# SAVE
# =======================
output_df.to_csv(output_file, index=False)

print(f"Saved: {output_file}")
print(
    f"Rows: {len(output_df)} | "
    f"No-affiliate coupons (set aff={FALLBACK_AFFILIATE_ID}, payout=0): {int(missing_aff_mask.sum())} | "
    f"Type counts -> revenue: {int(mask_rev.sum())}, sale: {int(mask_sale.sum())}, fixed: {int(mask_fixed.sum())}"
)
if not output_df.empty:
    print(f"Date range processed: {output_df['date'].min()} → {output_df['date'].max()}")
else:
    print("No rows after processing.")
