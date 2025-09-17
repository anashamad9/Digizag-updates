import pandas as pd
from datetime import datetime, timedelta
import os
import re

# =======================
# CONFIG
# =======================
days_back = 14
OFFER_ID = 1277
STATUS_DEFAULT = "pending"          # always "pending"
DEFAULT_PCT_IF_MISSING = 0.0        # fallback fraction for % values
FALLBACK_AFFILIATE_ID = "1"         # when no affiliate match: set to "1" and payout=0
GEO = "no-geo"

# Files
AFFILIATE_XLSX   = "Offers Coupons.xlsx"
AFFILIATE_SHEET  = "MetroBrazil"           # coupons sheet name for this offer
REPORT_PREFIX    = "DigiZag New 30-days"   # any tail like "(2).xlsx" is OK
REPORT_SHEET     = "DigiZag"
OUTPUT_CSV       = "Metro_brazil.csv"

# =======================
# PATHS
# =======================
script_dir = os.path.dirname(os.path.abspath(__file__))
input_dir = os.path.join(script_dir, '..', 'input data')
output_dir = os.path.join(script_dir, '..', 'output data')
os.makedirs(output_dir, exist_ok=True)

affiliate_xlsx_path = os.path.join(input_dir, AFFILIATE_XLSX)
output_file = os.path.join(output_dir, OUTPUT_CSV)

# =======================
# HELPERS
# =======================
def _norm_name(s: str) -> str:
    return re.sub(r"\s+", " ", str(s).strip()).lower()

def find_latest_xlsx_by_prefix(directory: str, prefix: str) -> str:
    pref = _norm_name(prefix)
    cands = []
    for f in os.listdir(directory):
        if f.startswith("~$"):
            continue
        if not f.lower().endswith(".xlsx"):
            continue
        base = os.path.splitext(f)[0]
        if _norm_name(base).startswith(pref):
            cands.append(os.path.join(directory, f))
    if not cands:
        avail = [f for f in os.listdir(directory) if f.lower().endswith(".xlsx")]
        raise FileNotFoundError(
            f"No .xlsx starting with '{prefix}' in {directory}. Available: {avail}"
        )
    return max(cands, key=os.path.getmtime)

def safe_read_excel(path: str, preferred_sheet: str) -> pd.DataFrame:
    xls = pd.ExcelFile(path)
    if preferred_sheet in xls.sheet_names:
        return pd.read_excel(path, sheet_name=preferred_sheet)
    # fallback: first sheet
    return pd.read_excel(path, sheet_name=xls.sheet_names[0])

def normalize_coupon(x: str) -> str:
    if pd.isna(x):
        return ""
    s = str(x).strip().upper()
    parts = re.split(r"[;,\s]+", s)
    return parts[0] if parts else s

def load_affiliate_mapping_from_xlsx(xlsx_path: str, sheet_name: str) -> pd.DataFrame:
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
        raise ValueError(f"[{sheet_name}] must contain a 'type' column with values 'revenue'/'sale'/'fixed'.")
    if not payout_col:
        raise ValueError(f"[{sheet_name}] must contain a payout column (e.g., 'payout').")

    payout_raw = df_sheet[payout_col].astype(str).str.replace("%", "", regex=False).str.strip()
    payout_num = pd.to_numeric(payout_raw, errors="coerce")
    type_norm  = df_sheet[type_col].astype(str).str.strip().str.lower().replace({"": None})

    pct_fraction = payout_num.where(type_norm.isin(["revenue", "sale"])).apply(
        lambda v: (v/100.0) if pd.notna(v) and v > 1 else (v if pd.notna(v) else DEFAULT_PCT_IF_MISSING)
    )
    fixed_amount = payout_num.where(type_norm.eq("fixed"))

    out = pd.DataFrame({
        "code_norm": df_sheet[code_col].apply(normalize_coupon),
        "affiliate_ID": df_sheet[aff_col].fillna("").astype(str).str.strip(),
        "type_norm": type_norm,
        "pct_fraction": pct_fraction,
        "fixed_amount": fixed_amount
    }).dropna(subset=["code_norm"])

    return out.drop_duplicates(subset=["code_norm"], keep="last")

# =======================
# LOAD & PREP DATA
# =======================
today = datetime.now().date()
end_date = today
start_date = end_date - timedelta(days=days_back)
print(f"Current date: {today}, Start date (days_back={days_back}): {start_date}")

report_path = find_latest_xlsx_by_prefix(input_dir, REPORT_PREFIX)
print(f"Using report file: {os.path.basename(report_path)}")

df = safe_read_excel(report_path, REPORT_SHEET)

# Normalize expected columns (accept tiny variants)
colmap = {c.strip().lower(): c for c in df.columns.astype(str)}
DateCol         = colmap.get("date", "Date")
OrderCountCol   = colmap.get("order count", "Order Count")
NetSalesCol     = colmap.get("net sales", "Net Sales")
DiscountCodeCol = colmap.get("discount code", "Discount Code")
OrderNameCol    = colmap.get("order name", "Order Name")

# Parse Date
df[DateCol] = pd.to_datetime(df[DateCol], errors='coerce')
df = df.dropna(subset=[DateCol])

# Date filter (exclude today)
df_filtered = df[(df[DateCol].dt.date >= start_date) & (df[DateCol].dt.date < today)].copy()

# =======================
# SPLIT ROWS BY ORDER COUNT
# =======================
split_rows = []
for _, row in df_filtered.iterrows():
    try:
        oc = int(pd.to_numeric(row.get(OrderCountCol, 1), errors="coerce") or 1)
    except Exception:
        oc = 1
    oc = max(1, oc)

    net_sales = float(pd.to_numeric(row.get(NetSalesCol, 0.0), errors="coerce") or 0.0)
    per_order_net = net_sales / oc if oc else 0.0

    for _ in range(oc):
        split_rows.append({
            'Discount Code': row.get(DiscountCodeCol, ""),
            'Order Name': row.get(OrderNameCol, ""),
            'Date': row[DateCol],
            'Order Count': 1,
            'Net Sales': per_order_net
        })

df_split = pd.DataFrame(split_rows)

# =======================
# DERIVED FIELDS
# =======================
df_split['sale_amount'] = pd.to_numeric(df_split['Net Sales'], errors='coerce').fillna(0.0) / 3.75
df_split['revenue'] = df_split['sale_amount'] * 0.10
df_split['coupon_norm'] = df_split['Discount Code'].apply(normalize_coupon)

# =======================
# JOIN AFFILIATE MAPPING (type-aware)
# =======================
map_df = load_affiliate_mapping_from_xlsx(affiliate_xlsx_path, AFFILIATE_SHEET)
df_joined = df_split.merge(map_df, how="left", left_on="coupon_norm", right_on="code_norm")

missing_aff_mask = df_joined['affiliate_ID'].isna() | (df_joined['affiliate_ID'].astype(str).str.strip() == "")

df_joined['affiliate_ID'] = df_joined['affiliate_ID'].fillna("").astype(str).str.strip()
df_joined['type_norm'] = df_joined['type_norm'].fillna("revenue")
df_joined['pct_fraction'] = df_joined['pct_fraction'].fillna(DEFAULT_PCT_IF_MISSING)

# =======================
# COMPUTE PAYOUT (by type)
# =======================
payout = pd.Series(0.0, index=df_joined.index)

mask_rev = df_joined['type_norm'].str.lower().eq('revenue')
payout.loc[mask_rev] = df_joined.loc[mask_rev, 'revenue'] * df_joined.loc[mask_rev, 'pct_fraction']

mask_sale = df_joined['type_norm'].str.lower().eq('sale')
payout.loc[mask_sale] = df_joined.loc[mask_sale, 'sale_amount'] * df_joined.loc[mask_sale, 'pct_fraction']

mask_fixed = df_joined['type_norm'].str.lower().eq('fixed')
payout.loc[mask_fixed] = df_joined.loc[mask_fixed, 'fixed_amount'].fillna(0.0)

# Fallback: if no affiliate match, set affiliate_id="1", payout=0
payout.loc[missing_aff_mask] = 0.0
df_joined.loc[missing_aff_mask, 'affiliate_ID'] = FALLBACK_AFFILIATE_ID

df_joined['payout'] = payout.round(2)

# =======================
# BUILD FINAL OUTPUT (NEW STRUCTURE)
# =======================
output_df = pd.DataFrame({
    'offer': OFFER_ID,
    'affiliate_id': df_joined['affiliate_ID'],
    'date': pd.to_datetime(df_joined['Date']).dt.strftime('%m-%d-%Y'),
    'status': STATUS_DEFAULT,
    'payout': df_joined['payout'],
    'revenue': df_joined['revenue'].round(2),
    'sale amount': df_joined['sale_amount'].round(2),
    'coupon': df_joined['coupon_norm'],
    'geo': GEO,
})

# Save
output_df.to_csv(output_file, index=False)

print(f"Saved: {output_file}")
print(
    f"Rows: {len(output_df)} | "
    f"Coupons with no affiliate (set aff={FALLBACK_AFFILIATE_ID}, payout=0): {int(missing_aff_mask.sum())} | "
    f"Type counts -> revenue: {int(mask_rev.sum())}, sale: {int(mask_sale.sum())}, fixed: {int(mask_fixed.sum())}"
)
print(f"Date range processed: {output_df['date'].min() if not output_df.empty else 'N/A'} to {output_df['date'].max() if not output_df.empty else 'N/A'}")
