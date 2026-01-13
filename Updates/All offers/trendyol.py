import pandas as pd
from datetime import datetime, timedelta
import os
import re

# =======================
# CONFIG
# =======================
days_back = 14
OFFER_ID = 1264
STATUS_DEFAULT = "pending"          # always "pending"
DEFAULT_PCT_IF_MISSING = 0.0        # fallback fraction for % values (0.30 == 30%)
FALLBACK_AFFILIATE_ID = "1"         # when no affiliate match: set to "1" and payout=0

# Local files
AFFILIATE_XLSX  = "Offers Coupons.xlsx"
AFFILIATE_SHEET = " Trendyol"     # coupons sheet name for this offer

# Report filename prefix (any tail like '(4).csv' is OK)
REPORT_PREFIX   = "TrendFam"
OUTPUT_CSV      = "trendyol.csv"

#geo mapping
COUNTRY_GEO = {"Saudi Arabia": "ksa", "United Arab Emirates": "uae", "Kuwait": "kwt",
               "Oman": "omn", "Qatar": "qtr", "Bahrain": "bhr"}

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
    """Lowercase + collapse spaces for robust comparisons."""
    return re.sub(r"\s+", " ", str(s).strip()).lower()

def find_latest_csv_by_prefix(directory: str, prefix: str) -> str:
    """
    Find the newest CSV whose base filename starts with `prefix`
    (case/space-insensitive). Falls back to modified time.
    """
    prefix_n = _norm_name(prefix)
    candidates = []
    for fname in os.listdir(directory):
        if not fname.lower().endswith(".csv"):
            continue
        base = os.path.splitext(fname)[0]
        if _norm_name(base).startswith(prefix_n):
            candidates.append(os.path.join(directory, fname))
    if not candidates:
        avail = [f for f in os.listdir(directory) if f.lower().endswith(".csv")]
        raise FileNotFoundError(
            f"No CSV starting with '{prefix}' in: {directory}\nAvailable CSVs: {avail}"
        )
    return max(candidates, key=os.path.getmtime)

def normalize_coupon(x: str) -> str:
    """Uppercase, trim, first token if multiple codes separated by ; , or whitespace (handles NBSP)."""
    if pd.isna(x):
        return ""
    s = str(x).replace("\u00A0", " ").strip().upper()  # NBSP -> space
    parts = re.split(r"[;,\s]+", s)
    return parts[0] if parts else s

def load_affiliate_mapping_from_xlsx(xlsx_path: str, sheet_name: str) -> pd.DataFrame:
    """Return mapping with columns code_norm, affiliate_ID, type_norm, pct_new, pct_old, fixed_new, fixed_old."""
    df_sheet = pd.read_excel(xlsx_path, sheet_name=sheet_name, dtype=str)
    
    df_sheet = df_sheet.iloc[:,0:3]

    df_sheet.columns = ['Code Name', 'affiliate_ID', 'Payout_Perc']

    df_sheet['Code Name'] = df_sheet['Code Name'].apply(normalize_coupon)

    df_sheet['affiliate_ID'] = df_sheet['affiliate_ID'].fillna("1")

    return df_sheet


def resolve_required_columns(df: pd.DataFrame):
    """
    Accept exact names used in your file; add light fallbacks for minor variants.
    """
    cols = {str(c).strip().lower(): c for c in df.columns}

    def get(*cands):
        for c in cands:
            if c in cols:
                return cols[c]
        return None

    created_date = get("created_date", "created date", "created")
    aed_net      = get("aed_gross_amount", "aed gross amount", "aed_gross")
    country      = get("country")
    coupon       = get("aff_coupon", "coupon", "coupon code", "affiliate coupon")
    fp_or_mp     = get("fp_or_mp", "fp or mp")

    missing = [nm for nm, col in {
        "created_date": created_date,
        "AED_net_amount": aed_net,
        "aff_coupon": coupon,
        "FP_or_MP": fp_or_mp,
    }.items() if not col]

    if missing:
        raise KeyError(f"Missing required columns: {missing}. Found: {list(df.columns)}")

    return created_date, aed_net, country, coupon, fp_or_mp

# =======================
# DATE WINDOW
# =======================
end_date = datetime.now().date()
start_date = end_date - timedelta(days=days_back)
print(f"Current date: {end_date}, Start date (days_back={days_back}): {start_date}")

# =======================
# PICK REPORT BY PREFIX
# =======================
report_path = find_latest_csv_by_prefix(input_dir, REPORT_PREFIX)
print(f"Using report file: {os.path.basename(report_path)}")

aff_sheet = load_affiliate_mapping_from_xlsx(affiliate_xlsx_path, AFFILIATE_SHEET)
# =======================
# LOAD REPORT
# =======================
df_raw = pd.read_csv(report_path)

chosen_cols = ['Date', 'Code Name', 'Country', 'Social Media Name', 'Email', 'Total Earnings']

# print(df_raw[chosen_cols])

df = df_raw[chosen_cols]
df = df[df['Total Earnings'] != 0]
df['Code Name'] = df['Code Name'].apply(normalize_coupon)

df = df.merge(aff_sheet, "left", "Code Name")

df['Country'] = df['Country'].apply(lambda x: COUNTRY_GEO[x])

final_df = pd.DataFrame({
    'offer': OFFER_ID,
    'affiliate_id': df['affiliate_ID'],
    'date': pd.to_datetime(df['Date']).dt.strftime('%m-%d-%Y'),
    'status': 'pending',
    'payout': df['Total Earnings'] * df['Payout_Perc'].apply(float),
    'revenue': df['Total Earnings'],
    'sale amount': df['Total Earnings'] * 1.3,
    'coupon': df['Code Name'],
    'geo': df['Country'],
    'social_media_name': df['Social Media Name'],
    'email': df['Email']
})

final_df['affiliate_id'] = final_df['affiliate_id'].fillna("MISSING")
final_df['payout'] = final_df['payout'].fillna(0.0)
final_df.loc[final_df['affiliate_id'] == '1','payout'] = 0.0

final_df.to_csv(output_file, index=False)