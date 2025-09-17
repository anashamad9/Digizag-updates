import pandas as pd
from datetime import datetime, timedelta
import os
import re

# =======================
# CONFIG
# =======================
days_back = 2
OFFER_ID = 1325
STATUS_DEFAULT = "pending"
DEFAULT_PCT_IF_MISSING = 0.0
FALLBACK_AFFILIATE_ID = "1"

# Any file that STARTS WITH this prefix will be matched (case-insensitive)
REPORT_PREFIX   = "DigiZag X 6thStreet Performance Tracker"
AFFILIATE_XLSX  = "Offers Coupons.xlsx"
AFFILIATE_SHEET = "6th Street"   # change if your tab name differs
OUTPUT_CSV      = "6th.csv"

# =======================
# DATES
# =======================
end_date = datetime.now().date()
start_date = end_date - timedelta(days=days_back - 1)
today = datetime.now().date()
print(f"Current date: {end_date}, Start date (days_back={days_back}): {start_date}")

# =======================
# HELPERS
# =======================
def normalize_coupon(x: str) -> str:
    """Uppercase, trim, keep first token if multiple codes separated by ; , or whitespace."""
    if pd.isna(x):
        return ""
    s = str(x).strip().upper()
    parts = re.split(r"[;,\s]+", s)
    return parts[0] if parts else s

def load_affiliate_mapping_from_xlsx(xlsx_path: str, sheet_name: str) -> pd.DataFrame:
    """
    Returns mapping with: code_norm, affiliate_ID, type_norm, pct_fraction, fixed_amount.
    Accepts 'ID' or 'affiliate_ID'. 'payout' can be % (for revenue/sale) or fixed (for 'fixed').
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
        raise ValueError(f"[{sheet_name}] must contain a 'type' column (revenue/sale/fixed).")
    if not payout_col:
        raise ValueError(f"[{sheet_name}] must contain a payout column (e.g., 'payout').")

    payout_raw = df_sheet[payout_col].astype(str).str.replace("%", "", regex=False).str.strip()
    payout_num = pd.to_numeric(payout_raw, errors="coerce")
    type_norm  = df_sheet[type_col].astype(str).str.strip().str.lower().replace({"": None})

    # percentage for revenue/sale
    pct_fraction = payout_num.where(type_norm.isin(["revenue", "sale"])).apply(
        lambda v: (v/100.0) if pd.notna(v) and v > 1 else (v if pd.notna(v) else DEFAULT_PCT_IF_MISSING)
    )
    # fixed for 'fixed'
    fixed_amount = payout_num.where(type_norm.eq("fixed"))

    out = pd.DataFrame({
        "code_norm": df_sheet[code_col].apply(normalize_coupon),
        "affiliate_ID": df_sheet[aff_col].fillna("").astype(str).str.strip(),
        "type_norm": type_norm,
        "pct_fraction": pct_fraction,
        "fixed_amount": fixed_amount
    }).dropna(subset=["code_norm"])

    return out.drop_duplicates(subset=["code_norm"], keep="last")

def find_matching_report_file(directory: str, prefix: str) -> str:
    """
    Find an .xlsx file in `directory` whose filename starts with `prefix` (case-insensitive).
    - Ignores temporary files like '~$...'
    - Prefers exact '<prefix>.xlsx' if present
    - Otherwise returns the most recently modified matching file
    Raises FileNotFoundError with a helpful message if none found.
    """
    prefix_lower = prefix.lower()
    candidates = []
    for fname in os.listdir(directory):
        if fname.startswith("~$"):  # ignore Excel temp/lock files
            continue
        if not fname.lower().endswith(".xlsx"):
            continue
        if os.path.splitext(fname)[0].lower().startswith(prefix_lower):
            candidates.append(os.path.join(directory, fname))

    if not candidates:
        available = [f for f in os.listdir(directory) if f.lower().endswith(".xlsx")]
        raise FileNotFoundError(
            f"No .xlsx file starting with '{prefix}' found in: {directory}\n"
            f"Available .xlsx files: {available}"
        )

    # Prefer exact match
    exact = [p for p in candidates if os.path.basename(p).lower() == (prefix_lower + ".xlsx")]
    if exact:
        return exact[0]

    # Otherwise choose the newest by mtime
    return max(candidates, key=os.path.getmtime)

# =======================
# PATHS
# =======================
script_dir = os.path.dirname(os.path.abspath(__file__))
input_dir = os.path.join(script_dir, '..', 'input data')
output_dir = os.path.join(script_dir, '..', 'output data')
os.makedirs(output_dir, exist_ok=True)

# Find the changing-named report file dynamically
input_file = find_matching_report_file(input_dir, REPORT_PREFIX)

affiliate_xlsx_path = os.path.join(input_dir, AFFILIATE_XLSX)
output_file = os.path.join(output_dir, OUTPUT_CSV)

# =======================
# LOAD & FILTER SOURCE
# =======================
df = pd.read_excel(input_file)

df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')
df = df.dropna(subset=['order_date'])

df_filtered = df[
    (df['order_date'].dt.date >= start_date) &
    (df['order_date'].dt.date <= end_date) &
    (df['status'].astype(str).str.lower() != 'canceled')
].copy()

# =======================
# EXPAND BY QTY
# =======================
expanded_rows = []
for _, row in df_filtered.iterrows():
    qty = int(pd.to_numeric(row.get('qty'), errors='coerce') or 0)
    if qty <= 0:
        continue
    sale_amount_per_order = (pd.to_numeric(row.get('gmv'), errors='coerce') or 0.0) / qty
    for _ in range(qty):
        expanded_rows.append({
            'order_date': row['order_date'],
            'country': row.get('country'),
            'user_type': row.get('user_type'),
            'sale_amount': sale_amount_per_order,
            'discount_code': row.get('discount_code')
        })

df_expanded = pd.DataFrame(expanded_rows)

# =======================
# REVENUE & CURRENCY
# =======================
df_expanded['sale_amount_usd'] = pd.to_numeric(df_expanded['sale_amount'], errors='coerce').fillna(0.0) / 3.67
df_expanded['revenue'] = df_expanded.apply(
    lambda r: r['sale_amount_usd'] * 0.10 if str(r.get('user_type')) == 'FTU' else r['sale_amount_usd'] * 0.05,
    axis=1
)

# =======================
# JOIN AFFILIATE MAP (type-aware)
# =======================
df_expanded['coupon_norm'] = df_expanded['discount_code'].apply(normalize_coupon)
map_df = load_affiliate_mapping_from_xlsx(affiliate_xlsx_path, AFFILIATE_SHEET)
df_joined = df_expanded.merge(map_df, how="left", left_on="coupon_norm", right_on="code_norm")

# Missing affiliate?
missing_aff_mask = df_joined['affiliate_ID'].isna() | (df_joined['affiliate_ID'].astype(str).str.strip() == "")

# Normalize mapping fields
df_joined['affiliate_ID'] = df_joined['affiliate_ID'].fillna("").astype(str).str.strip()
df_joined['type_norm'] = df_joined['type_norm'].fillna("revenue")
df_joined['pct_fraction'] = df_joined['pct_fraction'].fillna(DEFAULT_PCT_IF_MISSING)

# =======================
# PAYOUT (by type)
# =======================
payout = pd.Series(0.0, index=df_joined.index)

mask_rev   = df_joined['type_norm'].str.lower().eq('revenue')
mask_sale  = df_joined['type_norm'].str.lower().eq('sale')
mask_fixed = df_joined['type_norm'].str.lower().eq('fixed')

payout.loc[mask_rev]   = df_joined.loc[mask_rev,   'revenue']           * df_joined.loc[mask_rev,   'pct_fraction']
payout.loc[mask_sale]  = df_joined.loc[mask_sale,  'sale_amount_usd']   * df_joined.loc[mask_sale,  'pct_fraction']
payout.loc[mask_fixed] = df_joined.loc[mask_fixed, 'fixed_amount'].fillna(0.0)

# Enforce fallback: no coupon match → affiliate_id="1", payout=0
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
    'sale amount': df_joined['sale_amount_usd'].round(2),
    'coupon': df_joined['coupon_norm'],
    'geo': df_joined['country'],
})

# =======================
# SAVE
# =======================
output_dir = os.path.join(script_dir, '..', 'output data')
os.makedirs(output_dir, exist_ok=True)
output_file = os.path.join(output_dir, OUTPUT_CSV)
output_df.to_csv(output_file, index=False)

print(f"Using report file: {input_file}")
print(f"Saved: {output_file}")
print(
    f"Rows: {len(output_df)} | "
    f"No-affiliate coupons (set aff={FALLBACK_AFFILIATE_ID}, payout=0): {int(missing_aff_mask.sum())}"
)
print(f"Date range processed: {start_date} to {end_date}")
