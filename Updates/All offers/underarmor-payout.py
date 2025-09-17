import pandas as pd
from datetime import datetime, timedelta
import os
import re

# =======================
# CONFIG
# =======================
days_back = 20
OFFER_ID = 1103
STATUS_DEFAULT = "pending"
DEFAULT_PCT_IF_MISSING = 0.0
FALLBACK_AFFILIATE_ID = "1"

# Files
REPORT_PREFIX   = "UA - Digizag Data Studio Report by GMG_Untitled Page_Table"  # dynamic CSV name start
AFFILIATE_XLSX  = "Offers Coupons.xlsx"
AFFILIATE_SHEET = "Under Armour"
OUTPUT_CSV      = "underarmour.csv"

AED_TO_USD = 3.67  # divisor

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
# DATE WINDOW (exclude today)
# =======================
today = datetime.now().date()
start_date = today - timedelta(days=days_back)
print(f"Window: {start_date} ≤ date < {today}  (days_back={days_back})")

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
    """Uppercase, trim, take the first token if multiple codes separated by ; , or whitespace."""
    if pd.isna(x):
        return ""
    s = str(x).replace("\u00A0", " ").strip().upper()
    parts = re.split(r"[;,\s]+", s)
    return parts[0] if parts else s

def pick_col(df: pd.DataFrame, *cands):
    """Case/space-insensitive resolver with startswith fallback."""
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

def num(series: pd.Series) -> pd.Series:
    """Coerce numbers (strip commas/currency)."""
    return pd.to_numeric(
        series.astype(str)
              .str.replace(",", "", regex=False)
              .str.replace("$", "", regex=False)
              .str.replace("AED", "", regex=False)
              .str.strip(),
        errors="coerce"
    )

def load_affiliate_mapping_from_xlsx(xlsx_path: str, sheet_name: str) -> pd.DataFrame:
    """
    Returns mapping with: code_norm, affiliate_ID, type_norm, pct_fraction, fixed_amount
    Accepts 'ID' or 'affiliate_ID'. Coalesces payout from: payout → new customer payout → old customer payout.
    """
    df_sheet = pd.read_excel(xlsx_path, sheet_name=sheet_name, dtype=str)
    df_sheet.columns = [str(c).strip() for c in df_sheet.columns]

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
        raise ValueError(f"[{sheet_name}] must contain a payout-like column.")

    payout_raw = df_sheet[payout_col].astype(str).str.replace("%", "", regex=False).str.strip()
    payout_num = pd.to_numeric(payout_raw, errors="coerce")
    type_norm  = df_sheet[type_col].astype(str).str.strip().str.lower().replace({"": None})

    pct_fraction = payout_num.where(type_norm.isin(["revenue", "sale"])).apply(
        lambda v: (v/100.0) if pd.notna(v) and v > 1 else (v if pd.notna(v) else DEFAULT_PCT_IF_MISSING)
    )
    fixed_amount = payout_num.where(type_norm.eq("fixed"))

    aff_series = (
        df_sheet[aff_col].fillna("").astype(str).str.strip()
        .str.replace(r"\.0$", "", regex=True)
    )

    out = pd.DataFrame({
        "code_norm": df_sheet[code_col].apply(normalize_coupon),
        "affiliate_ID": aff_series,
        "type_norm": type_norm.fillna("revenue"),
        "pct_fraction": pct_fraction.fillna(DEFAULT_PCT_IF_MISSING),
        "fixed_amount": fixed_amount
    }).dropna(subset=["code_norm"])

    # Last definition per code wins
    return out.drop_duplicates(subset=["code_norm"], keep="last")

# =======================
# LOAD REPORT
# =======================
# Dynamically select the changing report CSV
input_file = find_matching_csv(input_dir, REPORT_PREFIX)
print(f"Using report file: {input_file}")

df_raw = pd.read_csv(input_file)
df_raw.columns = [c.strip() for c in df_raw.columns]

order_date_col = pick_col(df_raw, "Order Date")
coupon_col     = pick_col(df_raw, "Coupon Code", "Coupon")
netsales_col   = pick_col(df_raw, "Net Sales (in AED)", "Net Sales in AED", "Net Sales")

missing = [name for name, col in {
    "Order Date": order_date_col,
    "Coupon":     coupon_col,
    "Net Sales":  netsales_col
}.items() if col is None]
if missing:
    raise KeyError(f"Missing expected column(s): {missing}. Columns present: {list(df_raw.columns)}")

df = df_raw.rename(columns={
    order_date_col: "order_date",
    coupon_col: "coupon_code",
    netsales_col: "net_sales_aed",
})

# Parse dates & filter window (exclude today)
df["order_date"] = pd.to_datetime(df["order_date"], format="%b %d, %Y", errors="coerce")
before = len(df)
df = df.dropna(subset=["order_date"])
print(f"Total rows before filtering: {before} | dropped invalid dates: {before - len(df)}")
df = df[(df["order_date"].dt.date >= start_date) & (df["order_date"].dt.date < today)].copy()
print(f"Rows after date filter: {len(df)}")

# =======================
# DERIVED FIELDS
# =======================
df["sale_amount"] = num(df["net_sales_aed"]).fillna(0.0) / AED_TO_USD
df["revenue"]     = df["sale_amount"] * 0.08
df["coupon_norm"] = df["coupon_code"].apply(normalize_coupon)

# =======================
# JOIN AFFILIATE MAPPING (type-aware)
# =======================
map_df   = load_affiliate_mapping_from_xlsx(affiliate_xlsx_path, AFFILIATE_SHEET)
df_joined = df.merge(map_df, how="left", left_on="coupon_norm", right_on="code_norm")

missing_aff_mask = df_joined["affiliate_ID"].isna() | (df_joined["affiliate_ID"].astype(str).str.strip() == "")

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

payout.loc[mask_rev]   = df_joined.loc[mask_rev,   "revenue"]     * df_joined.loc[mask_rev,   "pct_fraction"]
payout.loc[mask_sale]  = df_joined.loc[mask_sale,  "sale_amount"] * df_joined.loc[mask_sale,  "pct_fraction"]
payout.loc[mask_fixed] = df_joined.loc[mask_fixed, "fixed_amount"].fillna(0.0)

# Fallback: no affiliate → affiliate_id="1", payout=0
payout.loc[missing_aff_mask] = 0.0
df_joined.loc[missing_aff_mask, "affiliate_ID"] = FALLBACK_AFFILIATE_ID

df_joined["payout"] = payout.round(2)

# =======================
# OUTPUT
# =======================
output_df = pd.DataFrame({
    "offer":        OFFER_ID,
    "affiliate_id": df_joined["affiliate_ID"],
    "date":         df_joined["order_date"].dt.strftime("%m-%d-%Y"),
    "status":       STATUS_DEFAULT,
    "payout":       df_joined["payout"],
    "revenue":      df_joined["revenue"].round(2),
    "sale amount":  df_joined["sale_amount"].round(2),
    "coupon":       df_joined["coupon_norm"],
    "geo":          "no-geo",   # set to a mapping if needed (e.g., from a 'Country' column)
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
