import pandas as pd
from datetime import datetime, timedelta
import os
import re
import zipfile

# =======================
# CONFIG
# =======================
days_back = 99               # <-- CHOOSE how many days back, inclusive of TODAY (2 == yesterday+today)
OFFER_ID = 910
STATUS_DEFAULT = "pending"
DEFAULT_PCT_IF_MISSING = 0.0
FALLBACK_AFFILIATE_ID = "1"
GEO = "no-geo"

# Files (dynamic starts-with prefixes)
FTU_PREFIX       = "FTU"
RTU_PREFIX       = "RTU"
AFFILIATE_XLSX   = "Offers Coupons.xlsx"
AFFILIATE_SHEET  = "VogaCloset"
OUTPUT_CSV       = "voga.csv"

# =======================
# PATHS
# =======================
script_dir = os.path.dirname(os.path.abspath(__file__))
updates_dir = os.path.dirname(os.path.dirname(script_dir))
input_dir = os.path.join(updates_dir, 'Input data')
output_dir = os.path.join(updates_dir, 'output data')
os.makedirs(output_dir, exist_ok=True)

affiliate_xlsx_path = os.path.join(input_dir, AFFILIATE_XLSX)
output_file = os.path.join(output_dir, OUTPUT_CSV)

# =======================
# DATE WINDOW (last N days incl. today)
# =======================
today = datetime.now().date()
# inclusive start date, exclusive end date (tomorrow)
start_date = today - timedelta(days=max(0, days_back - 1))
end_exclusive = today + timedelta(days=1)
print(f"Window: {start_date} ≤ date < {end_exclusive} (last {days_back} day(s), incl. today)")

# =======================
# HELPERS
# =======================
def find_matching_zip(directory: str, prefix: str) -> str:
    """
    Find a .zip in `directory` whose base filename starts with `prefix` (case-insensitive).
    Returns newest zip (preferring exact '<prefix>.zip' when present).
    """
    prefix_lower = prefix.lower()
    candidates = []
    for fname in os.listdir(directory):
        if fname.startswith("~$"):
            continue
        if not fname.lower().endswith(".zip"):
            continue
        base = os.path.splitext(fname)[0].lower()
        if base.startswith(prefix_lower):
            candidates.append(os.path.join(directory, fname))

    if not candidates:
        available = [f for f in os.listdir(directory) if f.lower().endswith(".zip")]
        raise FileNotFoundError(
            f"No .zip file starting with '{prefix}' found in: {directory}\n"
            f"Available .zip files: {available}"
        )

    exact = [p for p in candidates if os.path.basename(p).lower() == (prefix_lower + ".zip")]
    if exact:
        return exact[0]
    return max(candidates, key=os.path.getmtime)

def read_first_csv_from_zip(zip_path: str, prefix_hint: str) -> pd.DataFrame:
    """
    Read the first CSV from the zip file.
    Preference order:
      1. CSV whose base name starts with prefix_hint
      2. Any CSV (alphabetical)
    """
    with zipfile.ZipFile(zip_path) as zf:
        csv_names = [n for n in zf.namelist() if not n.endswith("/") and n.lower().endswith(".csv")]
        if not csv_names:
            raise FileNotFoundError(f"{zip_path} does not contain a .csv file")

        prefix_lower = prefix_hint.lower()
        pref = [n for n in csv_names if os.path.splitext(os.path.basename(n))[0].lower().startswith(prefix_lower)]
        target = pref[0] if pref else sorted(csv_names)[0]
        with zf.open(target) as fh:
            return pd.read_csv(fh)

def normalize_coupon(x: str) -> str:
    """Trim/uppercase. If multiple codes separated by ; , or whitespace, take the first token."""
    if pd.isna(x):
        return ""
    s = str(x).strip().upper().replace("\u00A0", " ")  # NBSP -> space
    parts = re.split(r"[;,\s]+", s)
    return parts[0] if parts else s

def _as_pct_fraction(series: pd.Series) -> pd.Series:
    """Accept either 0.73 or 73 (percent). Coerce to fraction."""
    raw = series.astype(str).str.replace("%", "", regex=False).str.strip()
    num = pd.to_numeric(raw, errors="coerce")
    return num.apply(lambda v: (v/100.0) if pd.notna(v) and v > 1 else (v if pd.notna(v) else DEFAULT_PCT_IF_MISSING))

def load_affiliate_mapping_from_xlsx(xlsx_path: str, sheet_name: str) -> pd.DataFrame:
    """
    Returns mapping with:
      code_norm, affiliate_ID, type_norm,
      pct_new, pct_old, fixed_new, fixed_old
    Uses columns: Code, ID(or affiliate_ID), type, 'new customer payout', 'old customer payout'
    """
    df_sheet = pd.read_excel(xlsx_path, sheet_name=sheet_name, dtype=str)
    cols = {c.lower().strip(): c for c in df_sheet.columns}

    code_col = cols.get("code")
    aff_col  = cols.get("id") or cols.get("affiliate_id")
    type_col = cols.get("type")
    new_col  = cols.get("new customer payout")
    old_col  = cols.get("old customer payout")

    if not all([code_col, aff_col, type_col, new_col, old_col]):
        raise ValueError(f"[{sheet_name}] must have columns: Code, ID(or affiliate_ID), type, new customer payout, old customer payout")

    type_norm = (df_sheet[type_col].astype(str).str.strip().str.lower().replace({"": None}).fillna("revenue"))

    # Parse both payout columns
    new_raw = df_sheet[new_col].astype(str).str.strip()
    old_raw = df_sheet[old_col].astype(str).str.strip()

    pct_new  = _as_pct_fraction(new_raw.where(type_norm.isin(["revenue","sale"])))
    pct_old  = _as_pct_fraction(old_raw.where(type_norm.isin(["revenue","sale"])))
    fix_new  = pd.to_numeric(new_raw.where(type_norm.eq("fixed")), errors="coerce")
    fix_old  = pd.to_numeric(old_raw.where(type_norm.eq("fixed")), errors="coerce")

    m = pd.DataFrame({
        "code_norm": df_sheet[code_col].apply(normalize_coupon),
        "affiliate_ID": df_sheet[aff_col].fillna("").astype(str).str.strip(),
        "type_norm": type_norm,
        "pct_new": pct_new.fillna(DEFAULT_PCT_IF_MISSING),
        "pct_old": pct_old.fillna(DEFAULT_PCT_IF_MISSING),
        "fixed_new": fix_new,
        "fixed_old": fix_old,
    }).dropna(subset=["code_norm"])

    # Prefer rows that actually have an affiliate_ID if duplicates
    m["has_aff"] = m["affiliate_ID"].astype(str).str.len() > 0
    m = (m.sort_values(by=["code_norm","has_aff"], ascending=[True, False])
           .drop_duplicates(subset=["code_norm"], keep="first")
           .drop(columns=["has_aff"]))
    return m

def prep_and_expand(df: pd.DataFrame, pct: float, user_tag: str) -> pd.DataFrame:
    """
    - Parse Period
    - Filter to window (start_date ≤ date < end_exclusive)
    - Expand by 'Number of Uses'
    - Compute sale_amount & revenue (using pct argument for platform revenue)
    - Keep user_tag ('FTU' or 'RTU') for payout routing
    """
    df = df.copy()
    df['Period'] = pd.to_datetime(df['Period'], format='%d %b %Y', errors='coerce')
    df = df.dropna(subset=['Period'])

    # filter to configurable window
    df = df[(df['Period'].dt.date >= start_date) & (df['Period'].dt.date < end_exclusive)]

    uses = pd.to_numeric(df['Number of Uses'], errors='coerce').fillna(0).astype(int).clip(lower=0)
    df_exp = df.loc[df.index.repeat(uses)].reset_index(drop=True)

    denom = pd.to_numeric(df_exp['Number of Uses'], errors='coerce').replace(0, pd.NA).fillna(1)
    sales_total = pd.to_numeric(df_exp['Sales Total Amount (USD)'], errors='coerce').fillna(0.0)
    df_exp['sale_amount'] = (sales_total / denom).astype(float)

    # Platform revenue (still needed regardless of affiliate payout type)
    df_exp['revenue'] = df_exp['sale_amount'] * pct
    df_exp['coupon_norm'] = df_exp['Coupon Code'].apply(normalize_coupon)
    df_exp['date_out'] = df_exp['Period'].dt.strftime('%m-%d-%Y')
    df_exp['user_tag'] = user_tag  # 'FTU' or 'RTU'
    return df_exp

# =======================
# LOAD & PREP FTU / RTU
# =======================
# Dynamically select the changing report ZIPs
ftu_zip = find_matching_zip(input_dir, FTU_PREFIX)
rtu_zip = find_matching_zip(input_dir, RTU_PREFIX)
print(f"Using FTU zip: {ftu_zip}")
print(f"Using RTU zip: {rtu_zip}")

df_ftu = read_first_csv_from_zip(ftu_zip, FTU_PREFIX)
df_rtu = read_first_csv_from_zip(rtu_zip, RTU_PREFIX)

# Adjust these base platform revenue rates if needed
ftu_exp = prep_and_expand(df_ftu, pct=0.20, user_tag="FTU")  # 20% FTU
rtu_exp = prep_and_expand(df_rtu, pct=0.05, user_tag="RTU")  # 5%  RTU

df_all = pd.concat([ftu_exp, rtu_exp], ignore_index=True)

# =======================
# JOIN AFFILIATE MAPPING
# =======================
map_df = load_affiliate_mapping_from_xlsx(affiliate_xlsx_path, AFFILIATE_SHEET)
dfj = df_all.merge(map_df, how="left", left_on="coupon_norm", right_on="code_norm")

# Who is missing an affiliate?
missing_aff_mask = dfj['affiliate_ID'].isna() | (dfj['affiliate_ID'].astype(str).str.strip() == "")
dfj['affiliate_ID'] = dfj['affiliate_ID'].fillna("").astype(str).str.strip()
dfj['type_norm'] = dfj['type_norm'].fillna("revenue")

# =======================
# COMPUTE PAYOUT (NEW vs OLD)
# =======================
payout = pd.Series(0.0, index=dfj.index)

# FTU uses "new customer payout"
is_ftu = dfj['user_tag'].eq("FTU")
is_rtu = dfj['user_tag'].eq("RTU")

mask_rev_ftu   = is_ftu & dfj['type_norm'].str.lower().eq('revenue')
mask_sale_ftu  = is_ftu & dfj['type_norm'].str.lower().eq('sale')
mask_fixed_ftu = is_ftu & dfj['type_norm'].str.lower().eq('fixed')

payout.loc[mask_rev_ftu]   = dfj.loc[mask_rev_ftu,   'revenue']     * dfj.loc[mask_rev_ftu,   'pct_new']
payout.loc[mask_sale_ftu]  = dfj.loc[mask_sale_ftu,  'sale_amount'] * dfj.loc[mask_sale_ftu,  'pct_new']
payout.loc[mask_fixed_ftu] = dfj.loc[mask_fixed_ftu, 'fixed_new'].fillna(0.0)

# RTU uses "old customer payout"
mask_rev_rtu   = is_rtu & dfj['type_norm'].str.lower().eq('revenue')
mask_sale_rtu  = is_rtu & dfj['type_norm'].str.lower().eq('sale')
mask_fixed_rtu = is_rtu & dfj['type_norm'].str.lower().eq('fixed')

payout.loc[mask_rev_rtu]   = dfj.loc[mask_rev_rtu,   'revenue']     * dfj.loc[mask_rev_rtu,   'pct_old']
payout.loc[mask_sale_rtu]  = dfj.loc[mask_sale_rtu,  'sale_amount'] * dfj.loc[mask_sale_rtu,  'pct_old']
payout.loc[mask_fixed_rtu] = dfj.loc[mask_fixed_rtu, 'fixed_old'].fillna(0.0)

# Fallback for unmatched coupons
payout.loc[missing_aff_mask] = 0.0
dfj.loc[missing_aff_mask, 'affiliate_ID'] = FALLBACK_AFFILIATE_ID

dfj['payout'] = payout.round(2)

# =======================
# BUILD OUTPUT (NEW STRUCTURE)
# =======================
output_df = pd.DataFrame({
    'offer': OFFER_ID,
    'affiliate_id': dfj['affiliate_ID'],
    'date': dfj['date_out'],
    'status': STATUS_DEFAULT,
    'payout': dfj['payout'],
    'revenue': dfj['revenue'].round(2),
    'sale amount': dfj['sale_amount'].round(2),
    'coupon': dfj['coupon_norm'],
    'geo': GEO,
})

# =======================
# SAVE
# =======================
output_df.to_csv(output_file, index=False)

print(f"Saved: {output_file}")
print(f"Rows: {len(output_df)} | No-affiliate coupons (aff={FALLBACK_AFFILIATE_ID}): {int(missing_aff_mask.sum())}")
print(f"Payout breakdown -> FTU rows: {int(is_ftu.sum())}, RTU rows: {int(is_rtu.sum())}")
print(f"Date window used: {start_date} to {end_exclusive} (exclusive)")
