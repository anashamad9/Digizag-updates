import pandas as pd
from datetime import datetime, timedelta
import os
import re

# =======================
# CONFIG
# =======================
OFFER_ID = 1326  # <-- set the correct offer ID
STATUS_FIXED = "Completed"
FALLBACK_AFFILIATE_ID = "1"
DEFAULT_PCT_IF_MISSING = 0.0

DAYS_BACK = 30  # <-- change how many days back you want (excludes today)

SOURCE_XLSX      = "Affiliates Report - Digizag.xlsx"
SOURCE_SHEET_FMT = "{month} {year}"         # e.g., "September 2025"
AFFILIATE_XLSX   = "Offers Coupons.xlsx"
AFFILIATE_SHEET  = "KenzWoman"
OUTPUT_CSV       = "kenzwomen.csv"

# =======================
# PATHS
# =======================
script_dir = os.path.dirname(os.path.abspath(__file__))
input_dir  = os.path.join(script_dir, '..', 'input data')
output_dir = os.path.join(script_dir, '..', 'output data')
os.makedirs(output_dir, exist_ok=True)

source_xlsx_path = os.path.join(input_dir, SOURCE_XLSX)
aff_map_path     = os.path.join(input_dir, AFFILIATE_XLSX)
output_path      = os.path.join(output_dir, OUTPUT_CSV)

# =======================
# HELPERS
# =======================
def xl_col_to_index(col_letters: str) -> int:
    col_letters = col_letters.strip().upper()
    n = 0
    for ch in col_letters:
        n = n * 26 + (ord(ch) - ord('A') + 1)
    return n - 1

def normalize_coupon(x: str) -> str:
    if pd.isna(x):
        return ""
    s = str(x).strip().upper()
    parts = re.split(r"[;,\s]+", s)
    return parts[0] if parts else s

def load_affiliate_mapping_from_xlsx(xlsx_path: str, sheet_name: str) -> pd.DataFrame:
    df_sheet = pd.read_excel(xlsx_path, sheet_name=sheet_name, dtype=str)
    cols_lower = {c.lower().strip(): c for c in df_sheet.columns}

    code_col   = cols_lower.get("code")
    aff_col    = cols_lower.get("id") or cols_lower.get("affiliate_id")
    type_col   = cols_lower.get("type")
    payout_col = (cols_lower.get("payout")
                  or cols_lower.get("new customer payout")
                  or cols_lower.get("old customer payout"))

    if not code_col or not aff_col or not type_col or not payout_col:
        raise ValueError(f"[{sheet_name}] must have: Code, ID(or affiliate_ID), type, payout")

    payout_raw = df_sheet[payout_col].astype(str).str.replace("%", "", regex=False).str.strip()
    payout_num = pd.to_numeric(payout_raw, errors="coerce")
    type_norm  = df_sheet[type_col].astype(str).str.strip().str.lower().replace({"": None})

    pct_fraction = payout_num.where(type_norm.isin(["revenue", "sale"])).apply(
        lambda v: (v/100.0) if pd.notna(v) and v > 1 else (v if pd.notna(v) else DEFAULT_PCT_IF_MISSING)
    )
    fixed_amount = payout_num.where(type_norm.eq("fixed"))

    return (pd.DataFrame({
        "code_norm": df_sheet[code_col].apply(normalize_coupon),
        "affiliate_ID": df_sheet[aff_col].fillna("").astype(str).str.strip(),
        "type_norm": type_norm.fillna("revenue"),
        "pct_fraction": pct_fraction.fillna(DEFAULT_PCT_IF_MISSING),
        "fixed_amount": fixed_amount
    })
    .dropna(subset=["code_norm"])
    .drop_duplicates(subset=["code_norm"], keep="last"))

# =======================
# DATE WINDOW
# =======================
today = datetime.now().date()
start_date = today - timedelta(days=DAYS_BACK)
print(f"Today: {today} | Start date (days_back={DAYS_BACK}): {start_date}")

# =======================
# SHEET NAME (current month)
# =======================
sheet_name = SOURCE_SHEET_FMT.format(month=datetime.now().strftime("%B"), year=datetime.now().strftime("%Y"))
print(f"Reading sheet: {sheet_name}")

# =======================
# LOAD SOURCE SHEET (A=date, D=coupon, F=sale amount, M=revenue)
# =======================
df_raw = pd.read_excel(source_xlsx_path, sheet_name=sheet_name, header=0)

idx_date   = xl_col_to_index("A")
idx_coupon = xl_col_to_index("D")
idx_sale   = xl_col_to_index("F")
idx_rev    = xl_col_to_index("M")

df = pd.DataFrame({
    "date_raw":   df_raw.iloc[:, idx_date],
    "coupon_raw": df_raw.iloc[:, idx_coupon],
    "sale_raw":   df_raw.iloc[:, idx_sale],
    "revenue_raw":df_raw.iloc[:, idx_rev],
})

# Parse fields
df["date"]       = pd.to_datetime(df["date_raw"], errors="coerce")
df["coupon_norm"]= df["coupon_raw"].apply(normalize_coupon)
df["sale_amount"]= pd.to_numeric(df["sale_raw"], errors="coerce")
df["revenue"]    = pd.to_numeric(df["revenue_raw"], errors="coerce")

# Keep only valid date + revenue
df = df.dropna(subset=["date", "revenue"]).copy()

# Apply DAYS_BACK filter
df = df[(df["date"].dt.date >= start_date) & (df["date"].dt.date < today)]

# =======================
# JOIN AFFILIATE MAP (type-aware payout)
# =======================
map_df = load_affiliate_mapping_from_xlsx(aff_map_path, AFFILIATE_SHEET)
df_joined = df.merge(map_df, how="left", left_on="coupon_norm", right_on="code_norm")

missing_aff = df_joined["affiliate_ID"].isna() | (df_joined["affiliate_ID"].astype(str).str.strip() == "")

# Compute payout by type
payout = pd.Series(0.0, index=df_joined.index)
mask_rev   = df_joined["type_norm"].str.lower().eq("revenue")
mask_sale  = df_joined["type_norm"].str.lower().eq("sale")
mask_fixed = df_joined["type_norm"].str.lower().eq("fixed")

payout.loc[mask_rev]   = df_joined.loc[mask_rev,   "revenue"]     * df_joined.loc[mask_rev,   "pct_fraction"].fillna(DEFAULT_PCT_IF_MISSING)
payout.loc[mask_sale]  = df_joined.loc[mask_sale,  "sale_amount"] * df_joined.loc[mask_sale,  "pct_fraction"].fillna(DEFAULT_PCT_IF_MISSING)
payout.loc[mask_fixed] = df_joined.loc[mask_fixed, "fixed_amount"].fillna(0.0)

# Fallback if no affiliate match
payout.loc[missing_aff] = 0.0
df_joined.loc[missing_aff, "affiliate_ID"] = FALLBACK_AFFILIATE_ID
df_joined["payout"] = payout.round(2)

# =======================
# OUTPUT
# =======================
output_df = pd.DataFrame({
    "offer": OFFER_ID,
    "affiliate_id": df_joined["affiliate_ID"],
    "date": df_joined["date"].dt.strftime("%m-%d-%Y"),
    "status": STATUS_FIXED,     # Always "Completed"
    "payout": df_joined["payout"],
    "revenue": df_joined["revenue"].round(2),
    "sale amount": df_joined["sale_amount"].round(2),
    "coupon": df_joined["coupon_norm"],
    "geo": "ksa",
})

output_df.to_csv(output_path, index=False)

print(f"Saved: {output_path}")
print(f"Rows: {len(output_df)} | Fallback affiliate rows: {int(missing_aff.sum())}")
print(f"Sheet used: {sheet_name} | Window: {start_date} ≤ date < {today}")
