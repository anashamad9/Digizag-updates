import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Optional
import os
import re

# =======================
# CONFIG
# =======================
OFFER_ID = 1326                   # <-- set the correct offer ID
STATUS_FIXED = "Completed"
FALLBACK_AFFILIATE_ID = "1"
DEFAULT_PCT_IF_MISSING = 0.0

DAYS_BACK = 30                    # rolling window; excludes today
TYPE_ALIAS_MAP = {
    "revenue": "revenue",
    "rev": "revenue",
    "revenueshare": "revenue",
    "revshare": "revenue",
    "sale": "sale",
    "sales": "sale",
    "gmv": "sale",
    "order": "sale",
    "orders": "sale",
    "fixed": "fixed",
    "flat": "fixed",
    "amount": "fixed",
}

# Dynamic report file (prefix only; any tail like (1) is OK)
SOURCE_PREFIX   = "Affiliates Report - Digizag"
AFFILIATE_XLSX  = "Offers Coupons.xlsx"
AFFILIATE_SHEET = "KenzWoman"
OUTPUT_CSV      = "kenzwomen.csv"

# Sheet name format is "Month YYYY" (e.g., "September 2025")
SHEET_FMT = "{month} {year}"

# Canonical source columns
COLUMN_ALIASES = {
    "date": ["date"],
    "coupon": ["coupon", "coupon code"],
    "sale_amount": ["amt in usd", "amount in usd", "amt usd"],
    "revenue": ["commission (usd)", "commission usd", "commission"],
    "geo": ["country"],
}
MONTH_NAME_MAP = {
    "jan": 1,
    "january": 1,
    "feb": 2,
    "february": 2,
    "mar": 3,
    "march": 3,
    "apr": 4,
    "april": 4,
    "may": 5,
    "jun": 6,
    "june": 6,
    "jul": 7,
    "july": 7,
    "aug": 8,
    "august": 8,
    "sep": 9,
    "sept": 9,
    "september": 9,
    "oct": 10,
    "october": 10,
    "nov": 11,
    "november": 11,
    "dec": 12,
    "december": 12,
}

# =======================
# PATHS
# =======================
script_dir = os.path.dirname(os.path.abspath(__file__))
input_dir  = os.path.join(script_dir, '..', 'input data')
output_dir = os.path.join(script_dir, '..', 'output data')
os.makedirs(output_dir, exist_ok=True)

aff_map_path = os.path.join(input_dir, AFFILIATE_XLSX)
output_path  = os.path.join(output_dir, OUTPUT_CSV)

# =======================
# HELPERS
# =======================
def _norm_name(s: str) -> str:
    """Lowercase + collapse spaces for robust comparisons."""
    return re.sub(r"\s+", " ", str(s).strip()).lower()

def find_matching_xlsx(directory: str, prefix: str) -> str:
    """
    Find .xlsx whose *basename* starts with prefix (case/space-insensitive).
    Prefer exact '<prefix>.xlsx' if present; otherwise newest by mtime.
    """
    prefix_n = _norm_name(prefix)
    candidates = []
    for fname in os.listdir(directory):
        if fname.startswith("~$"):               # skip temp files
            continue
        if not fname.lower().endswith(".xlsx"):
            continue
        base = os.path.splitext(fname)[0]
        if _norm_name(base).startswith(prefix_n):
            candidates.append(os.path.join(directory, fname))
    if not candidates:
        avail = [f for f in os.listdir(directory) if f.lower().endswith(".xlsx")]
        raise FileNotFoundError(
            f"No .xlsx starting with '{prefix}' in: {directory}\nAvailable: {avail}"
        )
    exact = [p for p in candidates if _norm_name(os.path.splitext(os.path.basename(p))[0]) == prefix_n]
    if exact:
        return exact[0]
    return max(candidates, key=os.path.getmtime)

def month_sheet_name(dt: datetime.date) -> str:
    return SHEET_FMT.format(month=dt.strftime("%B"), year=dt.strftime("%Y"))

def months_in_window(start_d: datetime.date, end_d: datetime.date):
    """Yield unique Month-YYYY boundaries touching [start_d, end_d]."""
    cur = datetime(start_d.year, start_d.month, 1).date()
    stop = datetime(end_d.year, end_d.month, 1).date()
    while cur <= stop:
        yield cur
        # next month
        if cur.month == 12:
            cur = datetime(cur.year + 1, 1, 1).date()
        else:
            cur = datetime(cur.year, cur.month + 1, 1).date()
    return

def normalize_coupon(x: str) -> str:
    if pd.isna(x):
        return ""
    s = str(x).strip().upper()
    parts = re.split(r"[;,\s]+", s)
    return parts[0] if parts else s

def normalize_type_label(value: object) -> str:
    if pd.isna(value):
        return "revenue"
    text = str(value).strip().lower()
    if not text:
        return "revenue"
    key = re.sub(r"[^a-z]+", "", text)
    return TYPE_ALIAS_MAP.get(key, "revenue")


def parse_numeric_series(series: pd.Series) -> pd.Series:
    cleaned = (
        series.astype(str)
        .str.replace('%', '', regex=False)
        .str.replace(',', '', regex=False)
        .str.strip()
    )
    cleaned = cleaned.replace({'': np.nan, 'nan': np.nan, 'none': np.nan, '-': np.nan})
    return pd.to_numeric(cleaned, errors='coerce')


def normalize_affiliate_id(value: object) -> str:
    if pd.isna(value):
        return FALLBACK_AFFILIATE_ID
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none"}:
        return FALLBACK_AFFILIATE_ID
    if re.fullmatch(r"\d+(\.0+)?", text):
        try:
            return str(int(float(text)))
        except ValueError:
            pass
    return text



def load_affiliate_mapping_from_xlsx(xlsx_path: str, sheet_name: str) -> pd.DataFrame:
    """Return mapping with columns coupon_norm, affiliate_id, type_norm, pct_value, fixed_value."""
    columns = ['coupon_norm', 'affiliate_id', 'type_norm', 'pct_value', 'fixed_value']

    if not os.path.exists(xlsx_path):
        return pd.DataFrame(columns=columns)

    df_sheet = pd.read_excel(xlsx_path, sheet_name=sheet_name, dtype=str)
    if df_sheet.empty:
        return pd.DataFrame(columns=columns)

    def find_column(candidates: list[str]) -> Optional[str]:
        normalized = {str(col).strip().lower(): col for col in df_sheet.columns}
        for cand in candidates:
            label = cand.strip().lower()
            if label in normalized:
                return normalized[label]
        for col in df_sheet.columns:
            cleaned = re.sub(r"[^a-z0-9]+", "", str(col).lower())
            if cleaned in {re.sub(r"[^a-z0-9]+", "", c.lower()) for c in candidates}:
                return col
        return None

    code_col = find_column(['code', 'coupon code', 'coupon'])
    id_col = find_column(['id', 'affiliate id', 'affiliate_id'])
    type_col = find_column(['type', 'payout type'])
    payout_col = find_column(['payout', 'payout value', 'value', 'rate'])
    new_col = find_column(['new customer payout', 'new payout', 'new customer'])
    old_col = find_column(['old customer payout', 'old payout', 'old customer'])

    if not code_col:
        raise ValueError(f"[{sheet_name}] must contain a 'Code' column.")

    base = df_sheet[[code_col]].copy()
    base['coupon_norm'] = base[code_col].apply(normalize_coupon)
    base['affiliate_id'] = (
        df_sheet[id_col].fillna("").astype(str).str.strip() if id_col else ""
    )
    base['type_norm'] = (
        df_sheet[type_col].apply(normalize_type_label) if type_col else "revenue"
    )

    payout_numeric = parse_numeric_series(df_sheet[payout_col]) if payout_col else pd.Series(np.nan, index=df_sheet.index, dtype=float)
    new_numeric = parse_numeric_series(df_sheet[new_col]) if new_col else pd.Series(np.nan, index=df_sheet.index, dtype=float)
    old_numeric = parse_numeric_series(df_sheet[old_col]) if old_col else pd.Series(np.nan, index=df_sheet.index, dtype=float)

    combined = payout_numeric.combine_first(new_numeric).combine_first(old_numeric)

    pct_mask = base['type_norm'].isin(['revenue', 'sale'])
    pct_values = combined.where(pct_mask)
    pct_values = pct_values.apply(lambda v: (v / 100.0) if pd.notna(v) and v > 1 else v)
    fixed_values = combined.where(base['type_norm'].eq('fixed'), np.nan)

    mapping = base.assign(
        pct_value=pct_values,
        fixed_value=fixed_values,
    )

    mapping = mapping[mapping['coupon_norm'].str.len() > 0]
    mapping['affiliate_id'] = mapping['affiliate_id'].apply(normalize_affiliate_id)

    return mapping[columns].drop_duplicates(subset='coupon_norm', keep='last')


# =======================
# DATE WINDOW
# =======================
today = datetime.now().date()
start_date = today - timedelta(days=DAYS_BACK)
yesterday = today - timedelta(days=1)
print(f"Today: {today} | Start date: {start_date} | Window: [{start_date} .. {yesterday}]")

# =======================
# PICK SOURCE FILE + SHEETS
# =======================
source_xlsx_path = find_matching_xlsx(input_dir, SOURCE_PREFIX)
print(f"Using source workbook: {os.path.basename(source_xlsx_path)}")

xls = pd.ExcelFile(source_xlsx_path)

def first_of_month(d: datetime.date) -> datetime.date:
    return datetime(d.year, d.month, 1).date()

def parse_sheet_month(sheet_name: str) -> Optional[datetime.date]:
    cleaned = re.sub(r"[^a-z0-9 ]+", " ", _norm_name(sheet_name))
    tokens = [tok for tok in cleaned.split() if tok]
    month = None
    year = None
    for tok in tokens:
        if tok in MONTH_NAME_MAP and month is None:
            month = MONTH_NAME_MAP[tok]
        elif re.fullmatch(r"\d{4}", tok) and year is None:
            year = int(tok)
    if month and year:
        return datetime(year, month, 1).date()
    return None

sheet_month_lookup: dict[datetime.date, str] = {}
for sheet in xls.sheet_names:
    month_key = parse_sheet_month(sheet)
    if month_key:
        sheet_month_lookup[month_key] = sheet

wanted_months = {first_of_month(m) for m in months_in_window(start_date, yesterday)}
selected_sheets = []
missing_months = []
for month_key in wanted_months:
    sheet_name = sheet_month_lookup.get(month_key)
    if sheet_name:
        selected_sheets.append(sheet_name)
    else:
        missing_months.append(month_sheet_name(month_key))

if not selected_sheets:
    raise KeyError(
        "No sheets matching months in window. "
        f"Expected one of: {sorted(month_sheet_name(m) for m in wanted_months)} | "
        f"Found: {xls.sheet_names}"
    )

if missing_months:
    print(f"Warning: missing sheets for months: {sorted(missing_months)}")

print(f"Sheets to read: {selected_sheets}")

# =======================
# COLUMN INDEXES (A/D/F/M)
# =======================

def resolve_column(df: pd.DataFrame, logical_name: str) -> str:
    """Resolve a real column name for the given logical identifier."""
    candidates = COLUMN_ALIASES.get(logical_name, [])
    lookup = {_norm_name(col): col for col in df.columns}
    for cand in candidates:
        norm = _norm_name(cand)
        if norm in lookup:
            return lookup[norm]
    # fallback: match cleaned text (remove punctuation)
    cleaned_map = {re.sub(r"[^a-z0-9]+", "", _norm_name(col)): col for col in df.columns}
    for cand in candidates:
        key = re.sub(r"[^a-z0-9]+", "", _norm_name(cand))
        if key in cleaned_map:
            return cleaned_map[key]
    raise KeyError(f"Could not find column for '{logical_name}' in sheet columns: {list(df.columns)}")


# =======================
# READ + FILTER + CONCAT
# =======================
frames = []
for sh in selected_sheets:
    raw = pd.read_excel(xls, sheet_name=sh, header=0)
    raw.columns = [str(col).strip() for col in raw.columns]

    try:
        date_col = resolve_column(raw, "date")
        coupon_col = resolve_column(raw, "coupon")
        sale_col = resolve_column(raw, "sale_amount")
        rev_col = resolve_column(raw, "revenue")
    except KeyError as exc:
        print(f"Skipping sheet '{sh}': {exc}")
        continue

    try:
        geo_col = resolve_column(raw, "geo")
        geo_series = raw[geo_col].astype(str).str.strip()
    except KeyError:
        geo_series = pd.Series(["ksa"] * len(raw), index=raw.index)

    df = pd.DataFrame({
        "date": pd.to_datetime(raw[date_col], errors="coerce"),
        "coupon_norm": raw[coupon_col].apply(normalize_coupon),
        "sale_amount": pd.to_numeric(raw[sale_col], errors="coerce"),
        "revenue": pd.to_numeric(raw[rev_col], errors="coerce"),
        "geo": geo_series,
    })

    # Keep valid rows
    df = df.dropna(subset=["date", "revenue"])
    df = df[df["coupon_norm"].str.len() > 0]
    # Window filter (exclude today)
    df = df[(df["date"].dt.date >= start_date) & (df["date"].dt.date <= yesterday)]
    if not df.empty:
        frames.append(df)

if frames:
    df_all = pd.concat(frames, ignore_index=True)
else:
    df_all = pd.DataFrame(columns=["date", "coupon_norm", "sale_amount", "revenue", "geo"])

print(f"Rows after window filter across sheets: {len(df_all)}")

# =======================
# JOIN AFFILIATE MAP + PAYOUT
# =======================
map_df = load_affiliate_mapping_from_xlsx(aff_map_path, AFFILIATE_SHEET)
df_joined = df_all.merge(map_df, how="left", on="coupon_norm")

if 'affiliate_id' not in df_joined.columns:
    df_joined['affiliate_id'] = np.nan
missing_aff_mask = df_joined['affiliate_id'].isna() | (df_joined['affiliate_id'].astype(str).str.strip() == "")
df_joined.loc[missing_aff_mask, 'affiliate_id'] = FALLBACK_AFFILIATE_ID
df_joined['affiliate_id'] = df_joined['affiliate_id'].apply(normalize_affiliate_id)

if 'type_norm' not in df_joined.columns:
    df_joined['type_norm'] = 'revenue'
df_joined['type_norm'] = df_joined['type_norm'].apply(normalize_type_label)

if 'pct_value' not in df_joined.columns:
    df_joined['pct_value'] = np.nan
df_joined['pct_value'] = pd.to_numeric(df_joined['pct_value'], errors='coerce')

if 'fixed_value' not in df_joined.columns:
    df_joined['fixed_value'] = np.nan
df_joined['fixed_value'] = pd.to_numeric(df_joined['fixed_value'], errors='coerce')

payout = pd.Series(0.0, index=df_joined.index, dtype=float)
mask_rev = df_joined['type_norm'].eq('revenue')
mask_sale = df_joined['type_norm'].eq('sale')
mask_fixed = df_joined['type_norm'].eq('fixed')

payout.loc[mask_rev] = (
    df_joined.loc[mask_rev, 'revenue']
    * df_joined.loc[mask_rev, 'pct_value'].fillna(DEFAULT_PCT_IF_MISSING)
)
payout.loc[mask_sale] = (
    df_joined.loc[mask_sale, 'sale_amount']
    * df_joined.loc[mask_sale, 'pct_value'].fillna(DEFAULT_PCT_IF_MISSING)
)
payout.loc[mask_fixed] = df_joined.loc[mask_fixed, 'fixed_value'].fillna(0.0)

df_joined['payout'] = payout.round(2)

# =======================
# OUTPUT
# =======================
output_df = pd.DataFrame({
    "offer": OFFER_ID,
    "affiliate_id": df_joined["affiliate_id"],
    "date": df_joined["date"].dt.strftime("%m-%d-%Y"),
    "status": STATUS_FIXED,     # Always "Completed"
    "payout": df_joined["payout"],
    "revenue": df_joined["revenue"].round(2),
    "sale amount": df_joined["sale_amount"].round(2),
    "coupon": df_joined["coupon_norm"],
    "geo": (
        df_joined["geo"].fillna("").replace("", "ksa")
        if "geo" in df_joined.columns
        else pd.Series(["ksa"] * len(df_joined), index=df_joined.index)
    ),
})

output_df.to_csv(output_path, index=False)

print(f"Saved: {output_path}")
print(f"Rows: {len(output_df)} | Fallback affiliate rows: {int(missing_aff_mask.sum())}")
print(f"Workbook: {os.path.basename(source_xlsx_path)} | Sheets used: {selected_sheets}")
print(f"Window: {start_date} ≤ date ≤ {yesterday}")
