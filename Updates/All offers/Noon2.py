import pandas as pd
from datetime import datetime, timedelta
import os
import re
from numpy import mean

# =======================
# CONFIG
# =======================
days_back = 42
OFFER_ID = 1166
STATUS_DEFAULT = "pending"
DEFAULT_PCT_IF_MISSING = 0.0
FALLBACK_AFFILIATE_ID = "1"

# Files
REPORT_PREFIX = "7010"
# REPORT_XLSX_PATTERN = r"^sales \(\d+\)\.xlsx$"  # fallback: newest matching file
AFFILIATE_XLSX  = "Offers Coupons.xlsx"
AFFILIATE_SHEET = "Noon GCC"

# =======================
# PATHS
# =======================
script_dir = os.path.dirname(os.path.abspath(__file__))
input_dir  = os.path.join(script_dir, '..', 'input data')
output_dir = os.path.join(script_dir, '..', 'output data')
os.makedirs(output_dir, exist_ok=True)


affiliate_xlsx_path = os.path.join(input_dir, AFFILIATE_XLSX)
output_file = os.path.join(output_dir, '7010_noon.csv')

# =======================
# HELPERS
# =======================
# def pick_report_path() -> str:
#     """Use the default report if present; otherwise pick the newest 'sales (N).xlsx'."""
#     default_path = os.path.join(input_dir, REPORT_XLSX_DEFAULT)
#     if os.path.exists(default_path):
#         return default_path
#     rx = re.compile(REPORT_XLSX_PATTERN, re.IGNORECASE)
#     cands = [f for f in os.listdir(input_dir) if rx.match(f)]
#     if not cands:
#         raise FileNotFoundError(
#             f"No report found. Expected '{REPORT_XLSX_DEFAULT}' or files matching '{REPORT_XLSX_PATTERN}'."
#         )
#     newest = max(cands, key=lambda f: os.path.getmtime(os.path.join(input_dir, f)))
#     return os.path.join(input_dir, newest)

def normalize_coupon(x: str) -> str:
    if pd.isna(x):
        return ""
    s = str(x).replace("\u00A0", " ").strip().upper()
    parts = re.split(r"[;,\s]+", s)
    return parts[0] if parts else s

def get_col(df: pd.DataFrame, *candidates: str) -> str:
    """Find a column by case-insensitive, space-normalized name; raise if none found."""
    low = {re.sub(r"\s+", " ", str(c)).strip().lower(): c for c in df.columns}
    for cand in candidates:
        key = re.sub(r"\s+", " ", cand).strip().lower()
        if key in low:
            return low[key]
    raise KeyError(f"None of the expected columns found: {candidates}")

def load_affiliate_mapping_from_xlsx(xlsx_path: str, sheet_name: str) -> pd.DataFrame:
    """
    Returns mapping with new/old payout values:
      code_norm, affiliate_ID, type_norm, pct_new, pct_old, fixed_new, fixed_old
    Accepts 'ID' or 'affiliate_ID' and parses payout columns as % (for revenue/sale) or
    fixed amounts (for fixed).
    """
    df_sheet = pd.read_excel(xlsx_path, sheet_name=sheet_name, dtype=str)
    cols_lower = {str(c).lower().strip(): c for c in df_sheet.columns}

    def need(name: str) -> str:
        col = cols_lower.get(name)
        if not col:
            raise ValueError(f"[{sheet_name}] must contain '{name}' column.")
        return col

    code_col = need("code")
    aff_col = cols_lower.get("id") or cols_lower.get("affiliate_id")
    type_col = need("type")
    payout_col = cols_lower.get("payout")
    new_col = cols_lower.get("new customer payout")
    old_col = cols_lower.get("old customer payout")

    if not aff_col:
        raise ValueError(f"[{sheet_name}] must contain an 'ID' (or 'affiliate_ID') column.")
    if not (payout_col or new_col or old_col):
        raise ValueError(
            f"[{sheet_name}] must contain at least one payout column (e.g., 'payout', 'new customer payout')."
        )

    def extract_numeric(col_name: str) -> pd.Series:
        if not col_name:
            return pd.Series([pd.NA] * len(df_sheet), dtype="Float64")
        raw = df_sheet[col_name].astype(str).str.replace("%", "", regex=False).str.strip()
        return pd.to_numeric(raw, errors="coerce")

    payout_any = extract_numeric(payout_col)
    payout_new_raw = extract_numeric(new_col).fillna(payout_any)
    payout_old_raw = extract_numeric(old_col).fillna(payout_any)

    type_norm = df_sheet[type_col].astype(str).str.strip().str.lower().replace({"": None})
    type_norm = type_norm.fillna("revenue")

    def pct_from(values: pd.Series, type_series: pd.Series) -> pd.Series:
        pct = values.where(type_series.isin(["revenue", "sale"]))
        return pct.apply(
            lambda v: (v / 100.0) if pd.notna(v) and v > 1 else (v if pd.notna(v) else pd.NA)
        )

    def fixed_from(values: pd.Series, type_series: pd.Series) -> pd.Series:
        return values.where(type_series.eq("fixed"))

    pct_new = pct_from(payout_new_raw, type_norm)
    pct_old = pct_from(payout_old_raw, type_norm)

    # If only one payout value exists, reuse it for the missing side.
    pct_new = pct_new.fillna(pct_old)
    pct_old = pct_old.fillna(pct_new)

    pct_new = pd.to_numeric(pct_new, errors='coerce')
    pct_old = pd.to_numeric(pct_old, errors='coerce')

    fixed_new = fixed_from(payout_new_raw, type_norm)
    fixed_old = fixed_from(payout_old_raw, type_norm)
    fixed_new = fixed_new.fillna(fixed_old)
    fixed_old = fixed_old.fillna(fixed_new)

    fixed_new = pd.to_numeric(fixed_new, errors='coerce')
    fixed_old = pd.to_numeric(fixed_old, errors='coerce')

    out = pd.DataFrame({
        "code_norm": df_sheet[code_col].apply(normalize_coupon),
        "affiliate_ID": df_sheet[aff_col].fillna("").astype(str).str.strip(),
        "type_norm": type_norm,
        "pct_new": pct_new.fillna(DEFAULT_PCT_IF_MISSING),
        "pct_old": pct_old.fillna(DEFAULT_PCT_IF_MISSING),
        "fixed_new": fixed_new,
        "fixed_old": fixed_old,
    }).dropna(subset=["code_norm"])

    return out.drop_duplicates(subset=["code_norm"], keep="last")

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
# DATE WINDOW
# =======================
end_date = datetime.now().date() + timedelta(days=1)  # include 'today'
start_date = end_date - timedelta(days=days_back + 1)
today = datetime.now().date()
print(f"Current date: {today}, Start date (days_back={days_back}): {start_date}")

# =======================
# LOAD REPORT
# =======================
input_file = find_latest_csv_by_prefix(input_dir, REPORT_PREFIX)
aff_file = load_affiliate_mapping_from_xlsx(affiliate_xlsx_path, AFFILIATE_SHEET)

df_raw = pd.read_csv(input_file)

df_raw['order_date'] = pd.to_datetime(df_raw['order_date'])


def determine_rev(sale_aed: float) -> float:
    if sale_aed <= 100.0:
        return 3.0
    elif sale_aed > 100 and sale_aed <= 200.0:
        return 6.0
    else:
        return 12.0



df_interm = df_raw.merge(aff_file, "left", left_on="coupon_code", right_on="code_norm")

del df_raw

df_interm['payout'] = 0.0

rev_mask = df_interm['type_norm'] == 'revenue'
fixed_mask = df_interm['type_norm'] == 'fixed'

df_interm['revenue'] = df_interm['gmv'].apply(lambda x: determine_rev(x))

df_interm.loc[rev_mask, 'payout'] = df_interm.loc[rev_mask, 'revenue'] * df_interm.loc[rev_mask, 'pct_new']
df_interm.loc[fixed_mask, 'payout'] = df_interm.loc[fixed_mask, 'fixed_new']

# print(df_interm.columns)

countries = {
    'om': 'omn',
    'kw': 'kwt',
    'bh': 'bhr',
    'qa': 'qtr'
}

df_final = pd.DataFrame({
    'offer': OFFER_ID,
    'affiliate_id': df_interm['affiliate_ID'],
    'date': df_interm['order_date'],
    'status': 'pending',
    'payout': df_interm['payout'],
    'revenue': df_interm['revenue'],
    'sale amount': df_interm['gmv'] / 3.67,
    'coupon': df_interm['code_norm'],
    'geo': df_interm['main_country'].apply(lambda x: countries[x])
    # 'type': df_interm['type_norm']
})

# Format date as month/day/year for Noon2 output.
df_final['date'] = pd.to_datetime(df_final['date'], errors='coerce').dt.strftime('%m/%d/%Y')

df_final.to_csv(output_file, index=False)
