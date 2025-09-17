import pandas as pd
from datetime import datetime, timedelta
import os
import re
from typing import Optional

# =======================
# CONFIG
# =======================
DAYS_BACK = 100                        # rolling window length (excludes today)
OFFER_ID = 1101
STATUS_DEFAULT = "pending"
DEFAULT_PCT_IF_MISSING = 0.0
FALLBACK_AFFILIATE_ID = "1"

AFFILIATE_XLSX  = "Offers Coupons.xlsx"
AFFILIATE_SHEET = "Sun & Sands"       # change if your tab is named differently (e.g., "SSS")
REPORT_PREFIX   = "SSS- Digizag Daily Report_Untitled Page_Table"  # dynamic CSV name start
OUTPUT_CSV      = "sun_sand.csv"

AED_TO_USD = 3.67

# =======================
# PATHS
# =======================
script_dir = os.path.dirname(os.path.abspath(__file__))
input_dir  = os.path.join(script_dir, '..', 'input data')
output_dir = os.path.join(script_dir, '..', 'output data')
os.makedirs(output_dir, exist_ok=True)

affiliate_xlsx_path = os.path.join(input_dir, AFFILIATE_XLSX)
output_file         = os.path.join(output_dir, OUTPUT_CSV)

# =======================
# DATE WINDOW
# =======================
today = datetime.now().date()
end_date = today                          # exclude today
start_date = end_date - timedelta(days=DAYS_BACK)
print(f"Window: {start_date} ≤ date < {end_date}  (DAYS_BACK={DAYS_BACK})")

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
    """Uppercase, trim, and take the first token if multiple codes separated by ; , or whitespace."""
    if pd.isna(x):
        return ""
    s = str(x).replace("\u00A0", " ").strip().upper()
    parts = re.split(r"[;,\s]+", s)
    return parts[0] if parts else s

def to_number(series: pd.Series) -> pd.Series:
    """Robust numeric coercion (drops commas/currency tokens)."""
    return pd.to_numeric(
        series.astype(str)
              .str.replace(",", "", regex=False)
              .str.replace("AED", "", case=False, regex=False)
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
    Accepts 'ID' or 'affiliate_ID'. 'payout' may be % (for revenue/sale) or fixed (for fixed).
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
# LOAD REPORT
# =======================
# Dynamically select the changing report CSV
input_file = find_matching_csv(input_dir, REPORT_PREFIX)
print(f"Using report file: {input_file}")

df_raw = pd.read_csv(input_file)

# Resolve key columns flexibly
date_col    = pick_col(df_raw, "date")
netsales_col= pick_col(df_raw, "net_sales", "net sales", "netsales")
coupon_col  = pick_col(df_raw, "coupon_code", "coupon code", "coupon")
status_col  = pick_col(df_raw, "final_status", "final status", "status")
geo_col     = pick_col(df_raw, "store", "market", "country")

missing = [n for n, c in {
    "Date": date_col,
    "net_sales": netsales_col,
    "coupon_code": coupon_col,
    "final_status": status_col,
    "Store/country": geo_col
}.items() if c is None]
if missing:
    raise KeyError(f"Missing expected column(s): {missing}. Columns found: {list(df_raw.columns)}")

df = df_raw.rename(columns={
    date_col:    "Date",
    netsales_col:"net_sales",
    coupon_col:  "coupon_code",
    status_col:  "final_status",
    geo_col:     "Store"
})

# Ensure Date datetime & filter window (exclude today)
df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
df = df.dropna(subset=["Date"])
print(f"Unique dates in dataset: {sorted(pd.Series(df['Date'].dt.date.unique()).astype(str))}")

# Remove Returned/Cancelled (case-insensitive, substring-safe)
status_low = df["final_status"].astype(str).str.lower()
keep_mask = (~status_low.str.contains("returned", na=False)) & (~status_low.str.contains("cancelled", na=False))
df = df[keep_mask]

df = df[(df["Date"].dt.date >= start_date) & (df["Date"].dt.date < end_date)].copy()
print(f"Filtered rows: {len(df)}")
if not df.empty:
    print(f"Filtered dates: {sorted(pd.Series(df['Date'].dt.date.unique()).astype(str))}")

# =======================
# DERIVED FIELDS
# =======================
# Convert sale amount to USD (AED -> USD)
df["sale_amount"] = to_number(df["net_sales"]).fillna(0.0) / AED_TO_USD

# Revenue: 6% flat
df["revenue"] = df["sale_amount"] * 0.06

# Normalize coupon for joining
df["coupon_norm"] = df["coupon_code"].apply(normalize_coupon)

# =======================
# JOIN AFFILIATE MAPPING (type-aware)
# =======================
map_df = load_affiliate_mapping_from_xlsx(affiliate_xlsx_path, AFFILIATE_SHEET)
df_joined = df.merge(map_df, how="left", left_on="coupon_norm", right_on="code_norm")

# Missing affiliate?
missing_aff_mask = df_joined["affiliate_ID"].isna() | (df_joined["affiliate_ID"].astype(str).str.strip() == "")

# Normalize mapping fields
df_joined["affiliate_ID"] = df_joined["affiliate_ID"].fillna("").astype(str).str.strip()
df_joined["type_norm"]    = df_joined["type_norm"].fillna("revenue")
df_joined["pct_fraction"] = df_joined["pct_fraction"].fillna(DEFAULT_PCT_IF_MISSING)

# =======================
# PAYOUT CALC
# =======================
payout = pd.Series(0.0, index=df_joined.index)

mask_rev   = df_joined["type_norm"].str.lower().eq("revenue")
mask_sale  = df_joined["type_norm"].str.lower().eq("sale")
mask_fixed = df_joined["type_norm"].str.lower().eq("fixed")

payout.loc[mask_rev]   = df_joined.loc[mask_rev, "revenue"]      * df_joined.loc[mask_rev, "pct_fraction"]
payout.loc[mask_sale]  = df_joined.loc[mask_sale, "sale_amount"] * df_joined.loc[mask_sale, "pct_fraction"]
payout.loc[mask_fixed] = df_joined.loc[mask_fixed, "fixed_amount"].fillna(0.0)

# Enforce: if no affiliate match, set affiliate_id="1", payout=0
payout.loc[missing_aff_mask] = 0.0
df_joined.loc[missing_aff_mask, "affiliate_ID"] = FALLBACK_AFFILIATE_ID

df_joined["payout"] = payout.round(2)

# Optional: User type sort (New before Repeat) if present
if "user_tag" in df_joined.columns:
    rank_map = {"New": 0, "Repeat": 1}
    df_joined["user_tag_rank"] = df_joined["user_tag"].map(rank_map)
    df_joined = df_joined.sort_values(by=["user_tag_rank"], na_position="last")

# =======================
# BUILD OUTPUT (NEW STRUCTURE)
# =======================
output_df = pd.DataFrame({
    "offer":        OFFER_ID,
    "affiliate_id": df_joined["affiliate_ID"],
    "date":         df_joined["Date"].dt.strftime("%m-%d-%Y"),
    "status":       STATUS_DEFAULT,
    "payout":       df_joined["payout"],
    "revenue":      df_joined["revenue"].round(2),
    "sale amount":  df_joined["sale_amount"].round(2),
    "coupon":       df_joined["coupon_norm"],
    "geo":          df_joined["Store"],
})

# =======================
# SAVE
# =======================
output_df.to_csv(output_file, index=False)

print(f"Saved: {output_file}")
print(f"Rows: {len(output_df)} | No-affiliate coupons (set aff={FALLBACK_AFFILIATE_ID}, payout=0): {int(missing_aff_mask.sum())}")
if not output_df.empty:
    print(f"Date range processed: {output_df['date'].min()} to {output_df['date'].max()}")
else:
    print("No rows after processing.")
