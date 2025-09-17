import pandas as pd
from datetime import datetime, timedelta
import os
import re
import unicodedata

# =======================
# CONFIG
# =======================
# Choose how many days back to include (rows from [today - days_back, today), i.e., exclude today)
days_back = 30

OFFER_ID = 1192
STATUS_DEFAULT = "pending"
DEFAULT_PCT_IF_MISSING = 0.0
FALLBACK_AFFILIATE_ID = "1"

# Local files
AFFILIATE_XLSX   = "Offers Coupons.xlsx"
AFFILIATE_SHEET  = "Mumzworld"
REPORT_PREFIX    = "DigiZag Dashboard_Commission Dashboard_Table"  # suffix like " (1).csv" is OK
OUTPUT_CSV       = "mumzworld.csv"

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
# HELPERS
# =======================
def normalize_coupon(s: str) -> str:
    """
    Aggressive normalizer so sheet & report codes match:
    - cast to str, replace NBSP, strip
    - Unicode NFKC normalize
    - uppercase
    - keep only A–Z and 0–9 (remove dashes, spaces, emojis, etc.)
    """
    if pd.isna(s):
        return ""
    s = str(s).replace("\u00A0", " ").strip()
    s = unicodedata.normalize("NFKC", s)
    s = s.upper()
    s = re.sub(r"[^A-Z0-9]+", "", s)
    return s

def _as_pct_fraction(series: pd.Series) -> pd.Series:
    """
    Accept 73, 73%, or 0.73 → returns fraction in [0..1]
    """
    raw = series.astype(str).str.replace("%", "", regex=False).str.strip()
    num = pd.to_numeric(raw, errors="coerce")
    return num.apply(lambda v: (v/100.0) if pd.notna(v) and v > 1 else (v if pd.notna(v) else DEFAULT_PCT_IF_MISSING))

def load_affiliate_mapping_from_xlsx(xlsx_path: str, sheet_name: str) -> pd.DataFrame:
    """
    Return mapping with columns:
      code_norm, affiliate_ID, type_norm, pct_fraction, fixed_amount
    Uses any of: Code / Coupon Code, ID / affiliate_ID, type, payout/new customer payout/old customer payout
    """
    df_sheet = pd.read_excel(xlsx_path, sheet_name=sheet_name, dtype=str)

    # Safe header lookup
    cols_lower = {}
    for c in df_sheet.columns:
        key = str(c).strip().lower()
        if key and key != "nan":
            cols_lower[key] = c

    def pick(*alts):
        for a in alts:
            if a in cols_lower:
                return cols_lower[a]
        return None

    code_col   = pick("code", "coupon code", "coupon", "coupon_code")
    aff_col    = pick("id", "affiliate_id", "affiliate id")
    type_col   = pick("type", "payout type", "commission type")
    payout_col = pick("payout", "new customer payout", "old customer payout", "commission", "rate")

    if not code_col:
        raise ValueError(f"[{sheet_name}] must contain a Code/Coupon Code column.")
    if not aff_col:
        raise ValueError(f"[{sheet_name}] must contain an ID/affiliate_ID column.")
    if not type_col:
        raise ValueError(f"[{sheet_name}] must contain a 'type' column (revenue/sale/fixed).")
    if not payout_col:
        raise ValueError(f"[{sheet_name}] must contain a payout column (payout / new customer payout / old customer payout).")

    type_norm = (
        df_sheet[type_col]
        .astype(str).str.strip().str.lower()
        .replace({"": None})
        .fillna("revenue")
    )

    payout_raw = df_sheet[payout_col].astype(str).str.replace("%", "", regex=False).str.strip()
    payout_num = pd.to_numeric(payout_raw, errors="coerce")

    pct_fraction = payout_num.where(type_norm.isin(["revenue", "sale"]))
    pct_fraction = pct_fraction.apply(
        lambda v: (v/100.0) if pd.notna(v) and v > 1 else (v if pd.notna(v) else DEFAULT_PCT_IF_MISSING)
    )
    fixed_amount = payout_num.where(type_norm.eq("fixed"))

    out = (
        pd.DataFrame({
            "code_norm": df_sheet[code_col].apply(normalize_coupon),
            "affiliate_ID": df_sheet[aff_col].fillna("").astype(str).str.strip(),
            "type_norm": type_norm,
            "pct_fraction": pct_fraction.fillna(DEFAULT_PCT_IF_MISSING),
            "fixed_amount": fixed_amount
        })
        .dropna(subset=["code_norm"])
    )

    # Prefer rows that actually have an affiliate ID when duplicates
    out["has_aff"] = out["affiliate_ID"].astype(str).str.len() > 0
    out = (
        out.sort_values(by=["code_norm", "has_aff"], ascending=[True, False])
           .drop_duplicates(subset=["code_norm"], keep="first")
           .drop(columns=["has_aff"])
    )
    return out

def find_latest_csv_by_prefix(directory: str, prefix: str) -> str:
    """
    Return the path to the most recently modified CSV whose *base name* starts with `prefix`.
    Matches e.g. 'DigiZag Dashboard_Commission Dashboard_Table.csv' or '... (3).csv'
    """
    prefix_norm = prefix.lower().strip()
    candidates = []
    for f in os.listdir(directory):
        if not f.lower().endswith(".csv"):
            continue
        base = os.path.splitext(f)[0].lower().strip()
        if base.startswith(prefix_norm):
            candidates.append(os.path.join(directory, f))
    if not candidates:
        avail = [f for f in os.listdir(directory) if f.lower().endswith(".csv")]
        raise FileNotFoundError(
            f"No CSV starting with '{prefix}' in {directory}. Available CSVs: {avail}"
        )
    return max(candidates, key=os.path.getmtime)

# =======================
# LOAD & PREP REPORT
# =======================
end_date = datetime.now().date()             # exclusive upper bound (we exclude "today")
start_date = end_date - timedelta(days=days_back)
print(f"Window: {start_date} ≤ date < {end_date} (exclude today)")

input_file = find_latest_csv_by_prefix(input_dir, REPORT_PREFIX)
print(f"Using input file: {os.path.basename(input_file)}")

df = pd.read_csv(input_file)

# Ensure Date_ordered is datetime & within the window (exclude today)
df['Date_ordered'] = pd.to_datetime(df['Date_ordered'], format='%b %d, %Y', errors='coerce')
df = df.dropna(subset=['Date_ordered'])
df = df[(df['Date_ordered'].dt.date >= start_date) & (df['Date_ordered'].dt.date < end_date)]

# Expand rows by # Orders New/Repeat and compute per-order sale_amount + platform revenue
expanded = []
for _, row in df.iterrows():
    new_orders    = int(row.get('# Orders New Customers', 0) or 0)
    repeat_orders = int(row.get('# Orders Repeat Customers', 0) or 0)
    order_date    = row['Date_ordered']  # keep datetime
    coupon_raw    = row.get('follower_code')

    # New
    if new_orders > 0 and pd.notnull(row.get('New Cust Revenue')):
        try:
            total_new_rev = float(row['New Cust Revenue'])
        except Exception:
            total_new_rev = 0.0
        sale_per = (total_new_rev / new_orders) if new_orders else 0.0
        for _ in range(new_orders):
            expanded.append({
                'order_date': order_date,
                'country': row.get('Country'),
                'user_type': 'New',
                'sale_amount': sale_per,
                'coupon_code': coupon_raw,
                # platform revenue (per your earlier logic)
                'revenue': sale_per * 0.08
            })

    # Repeat
    if repeat_orders > 0 and pd.notnull(row.get('Repeat Cust Revenue')):
        try:
            total_rep_rev = float(row['Repeat Cust Revenue'])
        except Exception:
            total_rep_rev = 0.0
        sale_per = (total_rep_rev / repeat_orders) if repeat_orders else 0.0
        for _ in range(repeat_orders):
            expanded.append({
                'order_date': order_date,
                'country': row.get('Country'),
                'user_type': 'Repeat',
                'sale_amount': sale_per,
                'coupon_code': coupon_raw,
                'revenue': sale_per * 0.03
            })

df_expanded = pd.DataFrame(expanded)
if df_expanded.empty:
    # Nothing to output; still create an empty file with headers
    pd.DataFrame(columns=[
        'offer','affiliate_id','date','status','payout','revenue','sale amount','coupon','geo'
    ]).to_csv(output_file, index=False)
    print("No rows after expansion; empty file written.")
    raise SystemExit(0)

df_expanded['order_date']  = pd.to_datetime(df_expanded['order_date'])
df_expanded['coupon_norm'] = df_expanded['coupon_code'].apply(normalize_coupon)

# =======================
# JOIN AFFILIATE MAPPING (type-aware)
# =======================
map_df = load_affiliate_mapping_from_xlsx(affiliate_xlsx_path, AFFILIATE_SHEET)
df_joined = df_expanded.merge(map_df, how="left", left_on="coupon_norm", right_on="code_norm")

# Debug aide: show a few unmatched coupon norms
missing_aff_mask = df_joined['affiliate_ID'].isna() | (df_joined['affiliate_ID'].astype(str).str.strip() == "")
if missing_aff_mask.any():
    miss = df_joined.loc[missing_aff_mask, ['coupon_norm']].drop_duplicates().sort_values('coupon_norm')
    print("Unmatched coupons (first 30):", miss.head(30).to_dict(orient="list"))

# Normalize mapping fields
df_joined['affiliate_ID'] = df_joined['affiliate_ID'].fillna("").astype(str).str.strip()
df_joined['type_norm']    = df_joined['type_norm'].fillna("revenue")
df_joined['pct_fraction'] = df_joined['pct_fraction'].fillna(DEFAULT_PCT_IF_MISSING)

# =======================
# COMPUTE PAYOUT (by type)
# =======================
payout = pd.Series(0.0, index=df_joined.index)

mask_rev   = df_joined['type_norm'].str.lower().eq('revenue')
mask_sale  = df_joined['type_norm'].str.lower().eq('sale')
mask_fixed = df_joined['type_norm'].str.lower().eq('fixed')

payout.loc[mask_rev]   = df_joined.loc[mask_rev,   'revenue']     * df_joined.loc[mask_rev,   'pct_fraction']
payout.loc[mask_sale]  = df_joined.loc[mask_sale,  'sale_amount'] * df_joined.loc[mask_sale,  'pct_fraction']
payout.loc[mask_fixed] = df_joined.loc[mask_fixed, 'fixed_amount'].fillna(0.0)

# Fallback for unmatched coupons: affiliate_id="1", payout=0
payout.loc[missing_aff_mask] = 0.0
df_joined.loc[missing_aff_mask, 'affiliate_ID'] = FALLBACK_AFFILIATE_ID

df_joined['payout'] = payout.round(2)

# =======================
# BUILD OUTPUT (NEW STRUCTURE)
# =======================
output_df = pd.DataFrame({
    'offer':        OFFER_ID,
    'affiliate_id': df_joined['affiliate_ID'],
    'date':         df_joined['order_date'].dt.strftime('%m-%d-%Y'),
    'status':       STATUS_DEFAULT,
    'payout':       df_joined['payout'],
    'revenue':      df_joined['revenue'].round(2),
    'sale amount':  df_joined['sale_amount'].round(2),
    'coupon':       df_joined['coupon_norm'],
    'geo':          df_joined['country'],
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
if not output_df.empty:
    print(f"Date range processed: {output_df['date'].min()} → {output_df['date'].max()}")
