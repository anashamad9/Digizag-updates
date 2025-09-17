import pandas as pd
from datetime import datetime, timedelta
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

# Dynamic report file (prefix only; any tail like (1) is OK)
SOURCE_PREFIX   = "Affiliates Report - Digizag"
AFFILIATE_XLSX  = "Offers Coupons.xlsx"
AFFILIATE_SHEET = "KenzWoman"
OUTPUT_CSV      = "kenzwomen.csv"

# Sheet name format is "Month YYYY" (e.g., "September 2025")
SHEET_FMT = "{month} {year}"

# Source column letters (A=date, D=coupon, F=sale amount, M=revenue)
DATE_COL_LETTERS   = "A"
COUPON_COL_LETTERS = "D"
SALE_COL_LETTERS   = "F"
REV_COL_LETTERS    = "M"

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

def xl_col_to_index(col_letters: str) -> int:
    col_letters = col_letters.strip().upper()
    n = 0
    for ch in col_letters:
        n = n * 26 + (ord(ch) - ord('A') + 1)
    return n - 1

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

def normalize_coupon(x: str) -> str:
    if pd.isna(x):
        return ""
    s = str(x).strip().upper()
    parts = re.split(r"[;,\s]+", s)
    return parts[0] if parts else s

def load_affiliate_mapping_from_xlsx(xlsx_path: str, sheet_name: str) -> pd.DataFrame:
    df_sheet = pd.read_excel(xlsx_path, sheet_name=sheet_name, dtype=str)
    cols_lower = {c.lower().strip(): c for c in df_sheet.columns}

    code_col   = cols_lower.get("code")
    aff_col    = cols_lower.get("id") or cols_lower.get("affiliate_id")
    type_col   = cols_lower.get("type")
    payout_col = (cols_lower.get("payout")
                  or cols_lower.get("new customer payout")
                  or cols_lower.get("old customer payout"))

    if not code_col or not aff_col or not type_col or not payout_col:
        raise ValueError(f"[{sheet_name}] must have: Code, ID(or affiliate_ID), type, payout")

    payout_raw = df_sheet[payout_col].astype(str).str.replace("%", "", regex=False).str.strip()
    payout_num = pd.to_numeric(payout_raw, errors="coerce")
    type_norm  = df_sheet[type_col].astype(str).str.strip().str.lower().replace({"": None})

    pct_fraction = payout_num.where(type_norm.isin(["revenue", "sale"])).apply(
        lambda v: (v/100.0) if pd.notna(v) and v > 1 else (v if pd.notna(v) else DEFAULT_PCT_IF_MISSING)
    )
    fixed_amount = payout_num.where(type_norm.eq("fixed"))

    out = (pd.DataFrame({
        "code_norm": df_sheet[code_col].apply(normalize_coupon),
        "affiliate_ID": df_sheet[aff_col].fillna("").astype(str).str.strip(),
        "type_norm": type_norm.fillna("revenue"),
        "pct_fraction": pct_fraction.fillna(DEFAULT_PCT_IF_MISSING),
        "fixed_amount": fixed_amount
    })
    .dropna(subset=["code_norm"])
    .drop_duplicates(subset=["code_norm"], keep="last"))
    return out

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

# Build the set of sheet names we expect based on months in the window
wanted_names = {month_sheet_name(m) for m in months_in_window(start_date, yesterday)}
# Map workbook sheet names case-insensitively
sheet_map = {_norm_name(s): s for s in xls.sheet_names}
selected_sheets = [sheet_map[_norm_name(n)] for n in wanted_names if _norm_name(n) in sheet_map]

if not selected_sheets:
    raise KeyError(
        f"No sheets matching months in window. "
        f"Expected one of: {sorted(wanted_names)} | Found: {xls.sheet_names}"
    )

print(f"Sheets to read: {selected_sheets}")

# =======================
# COLUMN INDEXES (A/D/F/M)
# =======================
idx_date   = xl_col_to_index(DATE_COL_LETTERS)
idx_coupon = xl_col_to_index(COUPON_COL_LETTERS)
idx_sale   = xl_col_to_index(SALE_COL_LETTERS)
idx_rev    = xl_col_to_index(REV_COL_LETTERS)

# =======================
# READ + FILTER + CONCAT
# =======================
frames = []
for sh in selected_sheets:
    raw = pd.read_excel(xls, sheet_name=sh, header=0)

    # Guard against short column count
    needed_max = max(idx_date, idx_coupon, idx_sale, idx_rev)
    if raw.shape[1] <= needed_max:
        print(f"Skipping sheet '{sh}' (not enough columns).")
        continue

    df = pd.DataFrame({
        "date":        pd.to_datetime(raw.iloc[:, idx_date], errors="coerce"),
        "coupon_norm": raw.iloc[:, idx_coupon].apply(normalize_coupon),
        "sale_amount": pd.to_numeric(raw.iloc[:, idx_sale], errors="coerce"),
        "revenue":     pd.to_numeric(raw.iloc[:, idx_rev], errors="coerce"),
    })

    # Keep valid rows
    df = df.dropna(subset=["date", "revenue"])
    # Window filter (exclude today)
    df = df[(df["date"].dt.date >= start_date) & (df["date"].dt.date <= yesterday)]
    if not df.empty:
        frames.append(df)

if frames:
    df_all = pd.concat(frames, ignore_index=True)
else:
    df_all = pd.DataFrame(columns=["date","coupon_norm","sale_amount","revenue"])

print(f"Rows after window filter across sheets: {len(df_all)}")

# =======================
# JOIN AFFILIATE MAP + PAYOUT
# =======================
map_df = load_affiliate_mapping_from_xlsx(aff_map_path, AFFILIATE_SHEET)
df_joined = df_all.merge(map_df, how="left", left_on="coupon_norm", right_on="code_norm")

missing_aff = df_joined["affiliate_ID"].isna() | (df_joined["affiliate_ID"].astype(str).str.strip() == "")

payout = pd.Series(0.0, index=df_joined.index)
mask_rev   = df_joined["type_norm"].str.lower().eq("revenue")
mask_sale  = df_joined["type_norm"].str.lower().eq("sale")
mask_fixed = df_joined["type_norm"].str.lower().eq("fixed")

payout.loc[mask_rev]   = df_joined.loc[mask_rev,   "revenue"]     * df_joined.loc[mask_rev,   "pct_fraction"].fillna(DEFAULT_PCT_IF_MISSING)
payout.loc[mask_sale]  = df_joined.loc[mask_sale,  "sale_amount"] * df_joined.loc[mask_sale,  "pct_fraction"].fillna(DEFAULT_PCT_IF_MISSING)
payout.loc[mask_fixed] = df_joined.loc[mask_fixed, "fixed_amount"].fillna(0.0)

# Fallback if no affiliate match
payout.loc[missing_aff] = 0.0
df_joined.loc[missing_aff, "affiliate_ID"] = FALLBACK_AFFILIATE_ID
df_joined["payout"] = payout.round(2)

# =======================
# OUTPUT
# =======================
output_df = pd.DataFrame({
    "offer": OFFER_ID,
    "affiliate_id": df_joined["affiliate_ID"],
    "date": df_joined["date"].dt.strftime("%m-%d-%Y"),
    "status": STATUS_FIXED,     # Always "Completed"
    "payout": df_joined["payout"],
    "revenue": df_joined["revenue"].round(2),
    "sale amount": df_joined["sale_amount"].round(2),
    "coupon": df_joined["coupon_norm"],
    "geo": "ksa",
})

output_df.to_csv(output_path, index=False)

print(f"Saved: {output_path}")
print(f"Rows: {len(output_df)} | Fallback affiliate rows: {int(missing_aff.sum())}")
print(f"Workbook: {os.path.basename(source_xlsx_path)} | Sheets used: {selected_sheets}")
print(f"Window: {start_date} ≤ date ≤ {yesterday}")
