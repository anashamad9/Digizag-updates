import pandas as pd
from datetime import datetime, timedelta
import os
import re

# =======================
# CONFIG (Grasse)
# =======================
OFFER_ID = 1346
GEO = "no-geo"
STATUS_DEFAULT = "pending"
DEFAULT_PCT_IF_MISSING = 0.0
FALLBACK_AFFILIATE_ID = "1"

# Choose how many days back to include
days_back = 30

# Files (match your tree)
REPORT_PREFIX   = "مبيعات المسوقين بالعمولة"  # dynamic: any .xlsx whose name starts with this
AFFILIATE_XLSX  = "Offers Coupons.xlsx"
AFFILIATE_SHEET = "GRASSE PERFUME"
OUTPUT_CSV      = "grasse.csv"

# Column letters (primary intent)
CODE_LETTER   = 'A'  # coupon code
ORDERS_LETTER = 'D'  # number of orders
SALE_LETTER   = 'E'  # sale amount (to be /3.75)
DATE_LETTER   = None # optional; e.g. 'B' to force a date column

# Fallback column name candidates (EN + AR)
CODE_NAME_CANDIDATES   = ["code", "coupon", "coupon code", "promo code", "voucher", "voucher code",
                          "رمز", "كود", "كوبون", "كود القسيمة"]
ORDERS_NAME_CANDIDATES = ["orders", "number of orders", "order count", "qty", "quantity", "count",
                          "عدد الطلبات", "الطلبات", "كمية", "عدد"]
SALE_NAME_CANDIDATES   = ["sale amount", "sales", "amount", "total", "revenue", "gmv", "net sales",
                          "المبيعات", "قيمة الطلبات", "المبلغ", "إجمالي", "الإجمالي"]
DATE_NAME_CANDIDATES   = ["date", "order date", "action date", "created", "created at",
                          "التاريخ", "تاريخ الطلب", "تاريخ", "انشاء", "تاريخ الانشاء"]

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
def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", str(s).strip()).lower()

def find_matching_xlsx(directory: str, prefix: str) -> str:
    """
    Find an .xlsx in `directory` whose base filename starts with `prefix` (space/case-insensitive).
    Prefers exact '<prefix>.xlsx' (normalized), else newest by modified time.
    """
    prefix_n = _norm(prefix)
    candidates = []
    for fname in os.listdir(directory):
        if fname.startswith("~$"):
            continue
        if not fname.lower().endswith(".xlsx"):
            continue
        base = os.path.splitext(fname)[0]
        if _norm(base).startswith(prefix_n):
            candidates.append(os.path.join(directory, fname))
    if not candidates:
        available = [f for f in os.listdir(directory) if f.lower().endswith(".xlsx")]
        raise FileNotFoundError(
            f"No .xlsx starting with '{prefix}' in: {directory}\nAvailable: {available}"
        )
    exact = [p for p in candidates if _norm(os.path.splitext(os.path.basename(p))[0]) == prefix_n]
    if exact:
        return exact[0]
    return max(candidates, key=os.path.getmtime)

def normalize_coupon(x: str) -> str:
    if pd.isna(x): return ""
    s = str(x).strip().upper()
    parts = re.split(r"[;,\s]+", s)
    return parts[0] if parts else s

def col_by_letter(df: pd.DataFrame, letter: str):
    idx = ord(letter.upper()) - ord('A')
    return df.columns[idx] if letter and 0 <= idx < len(df.columns) else None

def resolve_by_name(df: pd.DataFrame, candidates):
    low = {str(c).lower().strip(): c for c in df.columns}
    for cand in candidates:
        k = cand.lower().strip()
        if k in low: return low[k]
    for actual_lower, actual in low.items():
        for cand in candidates:
            if actual_lower.startswith(cand.lower().strip()):
                return actual
    return None

def load_affiliate_mapping_from_xlsx(xlsx_path: str, sheet_name: str) -> pd.DataFrame:
    df_sheet = pd.read_excel(xlsx_path, sheet_name=sheet_name, dtype=str)
    df_sheet.columns = [str(c).strip() for c in df_sheet.columns]
    cols_lower = {c.lower().strip(): c for c in df_sheet.columns}

    def res(cands):
        for cand in cands:
            k = cand.lower().strip()
            if k in cols_lower: return cols_lower[k]
        for actual_lower, actual in cols_lower.items():
            for cand in cands:
                if actual_lower.startswith(cand.lower().strip()): return actual
        raise ValueError(f"[{sheet_name}] missing any of {cands}. Columns: {list(df_sheet.columns)}")

    code_col = res(["Code"])
    aff_col  = res(["ID", "affiliate_ID"])
    type_col = res(["type"])

    payout_cols = []
    for nm in ["payout", "new customer payout", "old customer payout"]:
        try: payout_cols.append(res([nm]))
        except ValueError: pass
    if not payout_cols:
        raise ValueError(f"[{sheet_name}] needs a payout-like column")

    payout_num = None
    for pc in payout_cols:
        s = pd.to_numeric(df_sheet[pc].astype(str).str.replace("%","",regex=False).str.strip(), errors="coerce")
        payout_num = s if payout_num is None else payout_num.fillna(s)

    type_norm = df_sheet[type_col].astype(str).str.strip().str.lower().replace({"": None})
    pct_fraction = payout_num.where(type_norm.isin(["revenue","sale"])).apply(
        lambda v: (v/100.0) if pd.notna(v) and v > 1 else (v if pd.notna(v) else DEFAULT_PCT_IF_MISSING)
    )
    fixed_amount = payout_num.where(type_norm.eq("fixed"))

    aff_series = (df_sheet[aff_col].fillna("").astype(str).str.strip().str.replace(r"\.0$","",regex=True))

    out = pd.DataFrame({
        "code_norm": df_sheet[code_col].apply(normalize_coupon),
        "affiliate_ID": aff_series,
        "type_norm": type_norm,
        "pct_fraction": pct_fraction,
        "fixed_amount": fixed_amount,
    }).dropna(subset=["code_norm"]).drop_duplicates(subset=["code_norm"], keep="last")
    return out

def find_needed_columns(df: pd.DataFrame):
    code_col   = col_by_letter(df, CODE_LETTER) or resolve_by_name(df, CODE_NAME_CANDIDATES)
    orders_col = col_by_letter(df, ORDERS_LETTER) or resolve_by_name(df, ORDERS_NAME_CANDIDATES)
    sale_col   = col_by_letter(df, SALE_LETTER) or resolve_by_name(df, SALE_NAME_CANDIDATES)
    date_col = None
    if DATE_LETTER:
        date_col = col_by_letter(df, DATE_LETTER)
    if date_col is None:
        date_col = resolve_by_name(df, DATE_NAME_CANDIDATES)
    return code_col, orders_col, sale_col, date_col

def try_parse_sheet_date(name: str):
    """
    Parse sheet names like:
      - '01092025'  -> 01-09-2025 (DDMMYYYY)
      - '1882025'   -> 18-08-2025 (DDMYYYY; single-digit month)
      - '14-09-2025', '14/09/2025', '14.09.2025' (delimited)
    Returns a date or None.
    """
    s = str(name).strip()
    if s.lower() in {"codes", "sheet", "sheet1", "sheet2"}:
        return None
    cleaned = re.sub(r"[.\-_/\\\s]", "", s)
    if cleaned.isdigit():
        y = cleaned[-4:]
        dm = cleaned[:-4]
        if len(dm) == 4:
            d, m = dm[:2], dm[2:]
        elif len(dm) == 3:
            d, m = dm[:2], dm[2:]
        elif len(dm) == 2:
            return None
        else:
            try:
                dt = pd.to_datetime(s, errors="raise", dayfirst=True)
                return dt.date()
            except Exception:
                return None
        try:
            return datetime(int(y), int(m), int(d)).date()
        except Exception:
            return None
    dt = pd.to_datetime(s, errors='coerce', dayfirst=True)
    return None if pd.isna(dt) else dt.date()

# =======================
# LOAD REPORT (SCAN DATE-NAMED SHEETS)
# =======================
today = datetime.now().date()
start_date = today - timedelta(days=days_back)
yesterday = today - timedelta(days=1)
print(f"Running Grasse (Offer {OFFER_ID}) at {today} | days_back={days_back} | window: {start_date} to {yesterday}")

# Pick report file dynamically
report_path = find_matching_xlsx(input_dir, REPORT_PREFIX)
print(f"Using report file: {os.path.basename(report_path)}")

xls = pd.ExcelFile(report_path)

# pick only sheets whose names parse to a date within [start_date, yesterday]
eligible = []
for sh in xls.sheet_names:
    d = try_parse_sheet_date(sh)
    if d and (start_date <= d <= yesterday):
        eligible.append((d, sh))

if not eligible:
    raise KeyError(f"No date-named sheets in range. Sheets={xls.sheet_names}, window={start_date}..{yesterday}")

eligible.sort()  # by date

# Load mapping once
map_df = load_affiliate_mapping_from_xlsx(affiliate_xlsx_path, AFFILIATE_SHEET)

all_outputs = []

for sheet_date, sheet_name in eligible:
    df0 = pd.read_excel(xls, sheet_name=sheet_name)
    df0.columns = [str(c).strip() for c in df0.columns]

    code_col, orders_col, sale_col, date_col = find_needed_columns(df0)
    if not all([code_col, orders_col, sale_col]):
        print(f"Skipping sheet '{sheet_name}' (missing columns). Columns: {list(df0.columns)}")
        continue

    # numerics
    df0[orders_col] = pd.to_numeric(df0[orders_col], errors='coerce').fillna(0.0)
    df0[sale_col]   = pd.to_numeric(df0[sale_col], errors='coerce').fillna(0.0)

    # optional in-sheet date filter (if a date col exists, keep rows within the window and up to the sheet date)
    if date_col:
        df0[date_col] = pd.to_datetime(df0[date_col], errors='coerce')
        df0 = df0.dropna(subset=[date_col])
        df0 = df0[(df0[date_col].dt.date >= start_date) & (df0[date_col].dt.date <= sheet_date)]

    # your rule: skip zero/empty orders or sale
    df = df0[(df0[orders_col] > 0) & (df0[sale_col] > 0)].copy()
    if df.empty:
        continue

    # currency conversion + per-order split
    df['sale_total_converted'] = df[sale_col] / 3.75

    orders_int = df[orders_col].round().astype(int).clip(lower=1)
    df_expanded = df.loc[df.index.repeat(orders_int)].copy()

    df_expanded['__orders_orig'] = orders_int.loc[df_expanded.index].values
    df_expanded['sale_amount'] = (df_expanded['sale_total_converted'] / df_expanded['__orders_orig']).astype(float)

    # 12% revenue (matches code)
    df_expanded['revenue'] = df_expanded['sale_amount'] * 0.12

    # coupon
    df_expanded['coupon_norm'] = df_expanded[code_col].apply(normalize_coupon)

    # join mapping
    dfj = df_expanded.merge(map_df, how="left", left_on="coupon_norm", right_on="code_norm")

    # normalize mapping fields
    dfj['affiliate_ID'] = dfj['affiliate_ID'].fillna("").astype(str).str.strip()
    dfj['type_norm'] = dfj['type_norm'].fillna("revenue")
    dfj['pct_fraction'] = dfj['pct_fraction'].fillna(DEFAULT_PCT_IF_MISSING)

    # payout
    payout = pd.Series(0.0, index=dfj.index)
    mask_rev = dfj['type_norm'].str.lower().eq('revenue')
    payout.loc[mask_rev] = dfj.loc[mask_rev, 'revenue'] * dfj.loc[mask_rev, 'pct_fraction']
    mask_sale = dfj['type_norm'].str.lower().eq('sale')
    payout.loc[mask_sale] = dfj.loc[mask_sale, 'sale_amount'] * dfj.loc[mask_sale, 'pct_fraction']
    mask_fixed = dfj['type_norm'].str.lower().eq('fixed')
    payout.loc[mask_fixed] = dfj.loc[mask_fixed, 'fixed_amount'].fillna(0.0)

    mask_no_aff = dfj['affiliate_ID'].astype(str).str.strip().eq("")
    payout.loc[mask_no_aff] = 0.0
    dfj.loc[mask_no_aff, 'affiliate_ID'] = FALLBACK_AFFILIATE_ID

    dfj['payout'] = payout.round(2)

    # dates: prefer in-sheet date col; else sheet date
    if date_col and date_col in dfj.columns and pd.api.types.is_datetime64_any_dtype(dfj[date_col]):
        date_series = dfj[date_col].dt.strftime('%m-%d-%Y')
    else:
        date_series = pd.Series(sheet_date.strftime('%m-%d-%Y'), index=dfj.index)

    out = pd.DataFrame({
        'offer': OFFER_ID,
        'affiliate_id': dfj['affiliate_ID'],
        'date': date_series,
        'status': STATUS_DEFAULT,
        'payout': dfj['payout'],
        'revenue': dfj['revenue'].round(2),
        'sale amount': dfj['sale_amount'].round(2),
        'coupon': dfj['coupon_norm'],
        'geo': GEO,
    })

    all_outputs.append(out)

# =======================
# SAVE
# =======================
if all_outputs:
    output_df = pd.concat(all_outputs, ignore_index=True)
else:
    output_df = pd.DataFrame(columns=['offer','affiliate_id','date','status','payout','revenue','sale amount','coupon','geo'])

output_df.to_csv(output_file, index=False)

print(f"Saved: {output_file}")
print(f"Sheets processed: {len(all_outputs)} | Rows: {len(output_df)}")
