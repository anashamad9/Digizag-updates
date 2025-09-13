import pandas as pd
from datetime import datetime, timedelta
import os
import re

# =======================
# CONFIG
# =======================
days_back = 1
STATUS_DEFAULT = "pending"
DEFAULT_PCT_IF_MISSING = 0.0
FALLBACK_AFFILIATE_ID = "1"

REPORT_CSV      = "Social Affiliate - digizag_Untitled Page_Pivot table (5).csv"
AFFILIATE_XLSX  = "Offers Coupons.xlsx"
OUTPUT_CSV      = "shaye3.csv"

# Brand -> Offer
brand_to_offer = {
    'vs': 1208,   # Victoria Secret
    'pk': 1250,   # Pottery Barn Kids
    'nb': 1161,   # New Balance
    'mc': 1146,   # Mothercare
    'hm': 1132,   # H&M
    'fl': 1160,   # Footlocker
    'bbw': 1130,  # Bath & Body Works
    'aeo': 1133,  # American Eagle
    'pb': 1176,   # Pottery Barn
    'wes': 1131   # WestELM
}

# Offer -> Sheet name in Offers Coupons.xlsx
offer_to_sheet = {
    1208: "Victoria Secret",
    1250: "Pottery Barn Kids",
    1161: "New Balance",
    1146: "Mothercare",
    1132: "H&M",
    1160: "Footlocker",
    1130: "Bath & Body Works",
    1133: "American Eagle",
    1176: "Pottery Barn",
    1131: "WestELM",
}

# =======================
# PATHS
# =======================
script_dir = os.path.dirname(os.path.abspath(__file__))
input_dir  = os.path.join(script_dir, '..', 'input data')
output_dir = os.path.join(script_dir, '..', 'output data')
os.makedirs(output_dir, exist_ok=True)

input_file = os.path.join(input_dir, REPORT_CSV)
affiliate_xlsx_path = os.path.join(input_dir, AFFILIATE_XLSX)
output_file = os.path.join(output_dir, OUTPUT_CSV)

# =======================
# DATE WINDOW
# =======================
end_date = datetime.now().date()
start_date = end_date - timedelta(days=days_back)
print(f"Current date: {end_date}, Start date (days_back={days_back}): {start_date}")

# =======================
# HELPERS
# =======================
def normalize_coupon(x: str) -> str:
    """Uppercase, trim, take first token if multiple codes separated by ; , or whitespace."""
    if pd.isna(x):
        return ""
    s = str(x).strip().upper()
    parts = re.split(r"[;,\s]+", s)
    return parts[0] if parts else s

def load_affiliate_mapping_for_offer(xlsx_path: str, sheet_name: str, offer_id: int) -> pd.DataFrame:
    """
    Load a single sheet and return a mapping DataFrame with:
      ['offer','code_norm','affiliate_ID','type_norm','pct_fraction','fixed_amount']
    - Accepts 'ID' or 'affiliate_ID' for affiliate id.
    - 'payout' may be % (for revenue/sale) or fixed (for fixed).
    """
    df_sheet = pd.read_excel(xlsx_path, sheet_name=sheet_name, dtype=str)
    cols_lower = {c.lower().strip(): c for c in df_sheet.columns}

    code_col   = cols_lower.get("code")
    aff_col    = cols_lower.get("id") or cols_lower.get("affiliate_id")
    type_col   = cols_lower.get("type")
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

    # Parse payout as % or fixed
    payout_raw = df_sheet[payout_col].astype(str).str.replace("%", "", regex=False).str.strip()
    payout_num = pd.to_numeric(payout_raw, errors="coerce")
    type_norm  = df_sheet[type_col].astype(str).str.strip().str.lower().replace({"": None})

    pct_fraction = payout_num.where(type_norm.isin(["revenue", "sale"])).apply(
        lambda v: (v/100.0) if pd.notna(v) and v > 1 else (v if pd.notna(v) else DEFAULT_PCT_IF_MISSING)
    )
    fixed_amount = payout_num.where(type_norm.eq("fixed"))

    out = pd.DataFrame({
        "offer": offer_id,
        "code_norm": df_sheet[code_col].apply(normalize_coupon),
        "affiliate_ID": df_sheet[aff_col].fillna("").astype(str).str.strip(),
        "type_norm": type_norm.fillna("revenue"),
        "pct_fraction": pct_fraction.fillna(DEFAULT_PCT_IF_MISSING),
        "fixed_amount": fixed_amount
    }).dropna(subset=["code_norm"])

    # Deduplicate per offer+code
    return out.drop_duplicates(subset=["offer", "code_norm"], keep="last")

def build_master_affiliate_map(xlsx_path: str, offer_to_sheet_map: dict) -> pd.DataFrame:
    frames = []
    for offer_id, sheet in offer_to_sheet_map.items():
        frames.append(load_affiliate_mapping_for_offer(xlsx_path, sheet, offer_id))
    return pd.concat(frames, ignore_index=True)

# =======================
# LOAD REPORT
# =======================
df = pd.read_csv(input_file)

# Convert 'Date' and filter
df['Date'] = pd.to_datetime(df['Date'], format='%b %d, %Y', errors='coerce')
before = len(df)
df = df.dropna(subset=['Date'])
print(f"Total rows before filtering: {before}")
print(f"Rows with invalid dates dropped: {before - len(df)}")

df = df[(df['Date'].dt.date >= start_date) & (df['Date'].dt.date <= end_date)].copy()
print(f"Rows after filtering date range: {len(df)}")

# Map Brand -> Offer
df['offer'] = df['Brand'].map(brand_to_offer)

# Remove rows where Affiliate Cost is 0
df = df[df['Affiliate Cost'] != 0].copy()

# Rename for clarity and normalize coupon for join
df['sale_amount'] = pd.to_numeric(df['Affiliate Revenue'], errors='coerce').fillna(0.0)
df['revenue']     = pd.to_numeric(df['Affiliate Cost'], errors='coerce').fillna(0.0)
df['coupon_norm'] = df['Coupon Code'].apply(normalize_coupon)

# =======================
# BUILD MASTER AFFILIATE MAP (all offers)
# =======================
master_map = build_master_affiliate_map(affiliate_xlsx_path, offer_to_sheet)

# =======================
# JOIN ON (offer, coupon)
# =======================
df_joined = df.merge(master_map, how="left", left_on=["offer", "coupon_norm"], right_on=["offer", "code_norm"])

# Missing affiliate?
missing_aff_mask = df_joined['affiliate_ID'].isna() | (df_joined['affiliate_ID'].astype(str).str.strip() == "")

# =======================
# PAYOUT (by type)
# =======================
payout = pd.Series(0.0, index=df_joined.index)

mask_rev   = df_joined['type_norm'].str.lower().eq('revenue')
mask_sale  = df_joined['type_norm'].str.lower().eq('sale')
mask_fixed = df_joined['type_norm'].str.lower().eq('fixed')

payout.loc[mask_rev]   = df_joined.loc[mask_rev,   'revenue']     * df_joined.loc[mask_rev,   'pct_fraction'].fillna(DEFAULT_PCT_IF_MISSING)
payout.loc[mask_sale]  = df_joined.loc[mask_sale,  'sale_amount'] * df_joined.loc[mask_sale,  'pct_fraction'].fillna(DEFAULT_PCT_IF_MISSING)
payout.loc[mask_fixed] = df_joined.loc[mask_fixed, 'fixed_amount'].fillna(0.0)

# Enforce fallback: no coupon match → affiliate_id="1", payout=0
payout.loc[missing_aff_mask] = 0.0
df_joined.loc[missing_aff_mask, 'affiliate_ID'] = FALLBACK_AFFILIATE_ID

df_joined['payout'] = payout.round(2)

# =======================
# BUILD OUTPUT (NEW STRUCTURE)
# =======================
output_df = pd.DataFrame({
    'offer': df_joined['offer'],
    'affiliate_id': df_joined['affiliate_ID'],
    'date': df_joined['Date'].dt.strftime('%m-%d-%Y'),
    'status': STATUS_DEFAULT,
    'payout': df_joined['payout'],
    'revenue': df_joined['revenue'].round(2),
    'sale amount': df_joined['sale_amount'].round(2),
    'coupon': df_joined['coupon_norm'],
    'geo': df_joined['Market'],
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
