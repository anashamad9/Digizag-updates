import pandas as pd
from datetime import datetime, timedelta
import os
import re

# =======================
# CONFIG
# =======================
days_back = 50
OFFER_ID = 1283
GEO = "ksa"
STATUS_DEFAULT = "pending"
DEFAULT_PCT_IF_MISSING = 0.0  # fraction fallback when percent missing (0.30 == 30%)

# Local files
AFFILIATE_XLSX = "Offers Coupons.xlsx"   # multi-sheet Excel you uploaded
REPORT_PREFIX  = "Individual-Item-Report"  # any CSV starting with this will match

# Offer -> worksheet name mapping
OFFER_SHEET_BY_ID = {
    1283: "Adidas",
    # add others later if needed
}

# =======================
# PATHS
# =======================
script_dir = os.path.dirname(os.path.abspath(__file__))
input_dir = os.path.join(script_dir, '..', 'input data')
output_dir = os.path.join(script_dir, '..', 'output data')
os.makedirs(output_dir, exist_ok=True)

affiliate_xlsx_path = os.path.join(input_dir, AFFILIATE_XLSX)
output_file = os.path.join(output_dir, 'adidasssss_with_payout.csv')

# =======================
# HELPERS
# =======================
def normalize_coupon(x: str) -> str:
    """Uppercase, trim, and take the first token if multiple codes separated by ; , or whitespace."""
    if pd.isna(x):
        return ""
    s = str(x).strip().upper()
    parts = re.split(r"[;,\s]+", s)
    return parts[0] if parts else s

def pick_payout_column(cols_lower_map):
    """Priority for payout column: payout > new customer payout > old customer payout."""
    for candidate in ["payout", "new customer payout", "old customer payout"]:
        if candidate in cols_lower_map:
            return cols_lower_map[candidate]
    return None

def load_affiliate_mapping_from_xlsx(xlsx_path: str, offer_id: int) -> pd.DataFrame:
    """
    Load affiliate mapping for a given offer (sheet) and return:
      code_norm, affiliate_ID (from 'ID' or 'affiliate_ID'), type_norm,
      pct_fraction (for 'revenue'/'sale' types), fixed_amount (for 'fixed')
    """
    sheet_name = OFFER_SHEET_BY_ID.get(offer_id)
    if not sheet_name:
        raise ValueError(f"No sheet mapping defined for offer {offer_id}. Please add it to OFFER_SHEET_BY_ID.")

    df_sheet = pd.read_excel(xlsx_path, sheet_name=sheet_name, dtype=str)

    # Case-insensitive column resolver
    cols_lower = {c.lower().strip(): c for c in df_sheet.columns}

    code_col = cols_lower.get("code")
    # affiliate id can be 'ID' or 'affiliate_ID'
    aff_col  = cols_lower.get("id") or cols_lower.get("affiliate_id")
    type_col = cols_lower.get("type")
    payout_col = pick_payout_column(cols_lower)

    if not code_col:
        raise ValueError(f"[{sheet_name}] must contain a 'Code' column.")
    if not aff_col:
        raise ValueError(f"[{sheet_name}] must contain an 'ID' (or 'affiliate_ID') column.")
    if not type_col:
        raise ValueError(f"[{sheet_name}] must contain a 'type' column with values 'revenue'/'sale'/'fixed'.")
    if not payout_col:
        raise ValueError(f"[{sheet_name}] must contain a payout column (e.g., 'payout').")

    # Clean and parse the payout column:
    payout_raw = (
        df_sheet[payout_col]
        .astype(str)
        .str.replace("%", "", regex=False)
        .str.strip()
    )
    payout_num = pd.to_numeric(payout_raw, errors="coerce")

    # Normalize type
    type_norm = (
        df_sheet[type_col]
        .astype(str)
        .str.strip()
        .str.lower()
        .replace({"": None})
    )

    # For percentage types ('revenue'/'sale'): convert >1 to fraction; else keep as-is; fill default when NaN
    pct_fraction = payout_num.where(type_norm.isin(["revenue", "sale"]))
    pct_fraction = pct_fraction.apply(
        lambda v: (v / 100.0) if pd.notna(v) and v > 1 else (v if pd.notna(v) else DEFAULT_PCT_IF_MISSING)
    )

    # For fixed type: keep numeric as-is; otherwise NaN
    fixed_amount = payout_num.where(type_norm.eq("fixed"))

    out = pd.DataFrame({
        "code_norm": df_sheet[code_col].apply(normalize_coupon),
        "affiliate_ID": df_sheet[aff_col].fillna("").astype(str).str.strip(),
        "type_norm": type_norm,
        "pct_fraction": pct_fraction,   # used when type is 'revenue' or 'sale'
        "fixed_amount": fixed_amount    # used when type is 'fixed'
    }).dropna(subset=["code_norm"])

    # Deduplicate by code (last wins)
    out = out.drop_duplicates(subset=["code_norm"], keep="last")
    return out

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

# Find the changing-named report file dynamically
input_file = find_matching_csv(input_dir, REPORT_PREFIX)

# =======================
# LOAD MAIN REPORT
# =======================
df = pd.read_csv(input_file, skiprows=range(4))

# Convert 'Transaction Date' to datetime, drop NaT
df['Transaction Date'] = pd.to_datetime(df['Transaction Date'], format='%m/%d/%y', errors='coerce')
df = df.dropna(subset=['Transaction Date'])

end_date = datetime.now().date()
start_date = end_date - timedelta(days=days_back)
today = datetime.now().date()

# Filter for Adidas KSA within range, excluding current day
df_filtered = df[
    (df['Advertiser Name'] == 'Adidas KSA') &
    (df['Transaction Date'].dt.date >= start_date) &
    (df['Transaction Date'].dt.date < today)
]

# Split rows with # of Items > 1 into per-item rows
split_rows = []
for _, row in df_filtered.iterrows():
    items = int(row['# of Items']) if pd.notnull(row['# of Items']) else 1
    total_sales = float(row['Sales']) if pd.notnull(row['Sales']) else 0.0
    sales_per_item = (total_sales / items) if items > 0 else 0.0
    for _ in range(items):
        split_rows.append({
            'Order Coupon Code(s)': row.get('Order Coupon Code(s)', ''),
            'Transaction Date': row['Transaction Date'],
            'Sales': sales_per_item,
            '# of Items': 1
        })

df_split = pd.DataFrame(split_rows)

# Compute sale_amount and revenue
df_split['sale_amount'] = df_split['Sales'] * 1.31
df_split['revenue'] = df_split['sale_amount'] * 0.07

# Normalize coupon for joining
df_split['coupon_norm'] = df_split['Order Coupon Code(s)'].apply(normalize_coupon)

# =======================
# JOIN AFFILIATE MAPPING (type-aware)
# =======================
map_df = load_affiliate_mapping_from_xlsx(affiliate_xlsx_path, OFFER_ID)
df_joined = df_split.merge(map_df, how="left", left_on="coupon_norm", right_on="code_norm")

# Ensure required fields exist
df_joined['affiliate_ID'] = df_joined['affiliate_ID'].fillna("").astype(str).str.strip()
df_joined['type_norm'] = df_joined['type_norm'].fillna("revenue")  # default to 'revenue' logic if missing
df_joined['pct_fraction'] = df_joined['pct_fraction'].fillna(DEFAULT_PCT_IF_MISSING)

# =======================
# COMPUTE PAYOUT BASED ON TYPE
# =======================
payout = pd.Series(0.0, index=df_joined.index)

# revenue-based %
mask_rev = df_joined['type_norm'].str.lower().eq('revenue')
payout.loc[mask_rev] = (df_joined.loc[mask_rev, 'revenue'] * df_joined.loc[mask_rev, 'pct_fraction'])

# sale-based %
mask_sale = df_joined['type_norm'].str.lower().eq('sale')
payout.loc[mask_sale] = (df_joined.loc[mask_sale, 'sale_amount'] * df_joined.loc[mask_sale, 'pct_fraction'])

# fixed amount
mask_fixed = df_joined['type_norm'].str.lower().eq('fixed')
payout.loc[mask_fixed] = df_joined.loc[mask_fixed, 'fixed_amount'].fillna(0.0)

# Force payout = 0 when affiliate_id is missing/empty
mask_no_aff = (df_joined['affiliate_ID'] == "")
payout.loc[mask_no_aff] = 0.0

df_joined['payout'] = payout.round(2)

# =======================
# BUILD FINAL OUTPUT (NEW STRUCTURE)
# =======================
output_df = pd.DataFrame({
    'offer': OFFER_ID,
    'affiliate_id': df_joined['affiliate_ID'],
    'date': df_joined['Transaction Date'].dt.strftime('%m-%d-%Y'),
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
    f"Type counts -> revenue: {int(mask_rev.sum())}, sale: {int(mask_sale.sum())}, fixed: {int(mask_fixed.sum())}"
)
