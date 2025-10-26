import pandas as pd
from datetime import datetime, timedelta
import os
import re

# =======================
# CONFIG (ToYou)
# =======================
OFFER_ID = 1186
STATUS_DEFAULT = "pending"
DEFAULT_PCT_IF_MISSING = 0.0  # 0.30 == 30%

# Window: [today - days_back, today) — excludes today. Set 1 for "yesterday only".
days_back = 100

REPORT_PREFIX  = "DigiZag Promo External Report_ Digizag External"  # dynamic CSV name start
AFFILIATE_XLSX = "Offers Coupons.xlsx"
AFFILIATE_SHEET = "ToYou"
OUTPUT_CSV     = "toyou.csv"

# =======================
# PATHS
# =======================
script_dir = os.path.dirname(os.path.abspath(__file__))
input_dir  = os.path.join(script_dir, "..", "input data")
output_dir = os.path.join(script_dir, "..", "output data")
os.makedirs(output_dir, exist_ok=True)

affiliate_xlsx_path = os.path.join(input_dir, AFFILIATE_XLSX)
output_file         = os.path.join(output_dir, OUTPUT_CSV)

# =======================
# HELPERS
# =======================
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

def normalize_coupon(x: str) -> str:
    """Uppercase, trim, and take the first token if multiple codes separated by ; , or whitespace."""
    if pd.isna(x):
        return ""
    s = str(x).strip().upper()
    parts = re.split(r"[;,\s]+", s)
    return parts[0] if parts else s

def infer_is_new_customer(df: pd.DataFrame) -> pd.Series:
    """Infer a boolean new-customer flag from common columns; default False when no signal."""
    if df.empty:
        return pd.Series(False, index=df.index, dtype=bool)

    candidates = [
        'customer_type',
        'customer type',
        'customer_type',
        'customer type',
        'customer segment',
        'customersegment',
        'new_vs_old',
        'new vs old',
        'new/old',
        'new old',
        'new_vs_existing',
        'new vs existing',
        'user_type',
        'user type',
        'usertype',
        'type_customer',
        'type customer',
        'audience',
    ]

    new_tokens = {
        'new', 'newuser', 'newusers', 'newcustomer', 'newcustomers',
        'ftu', 'first', 'firstorder', 'firsttime', 'acquisition', 'prospect'
    }
    old_tokens = {
        'old', 'olduser', 'oldcustomer', 'existing', 'existinguser', 'existingcustomer',
        'return', 'returning', 'repeat', 'rtu', 'retention', 'loyal', 'existingusers'
    }

    columns_map = {str(c).strip().lower(): c for c in df.columns}
    result = pd.Series(False, index=df.index, dtype=bool)
    resolved = pd.Series(False, index=df.index, dtype=bool)

    def tokenize(value) -> set:
        if pd.isna(value):
            return set()
        text = ''.join(ch if ch.isalnum() else ' ' for ch in str(value).lower())
        return {tok for tok in text.split() if tok}

    for key in candidates:
        actual = columns_map.get(key)
        if not actual:
            continue
        tokens_series = df[actual].apply(tokenize)
        is_new = tokens_series.apply(lambda toks: bool(toks & new_tokens))
        is_old = tokens_series.apply(lambda toks: bool(toks & old_tokens))
        recognized = (is_new | is_old) & ~resolved
        if recognized.any():
            result.loc[recognized] = is_new.loc[recognized]
            resolved.loc[recognized] = True
        if resolved.all():
            break
    return result


def pick_col(df: pd.DataFrame, *cands):
    """Case/space-insensitive column resolver with startswith fallback."""
    norm = {str(c).strip().lower(): c for c in df.columns}
    for cand in cands:
        key = str(cand).strip().lower()
        if key in norm:
            return norm[key]
    for cand in cands:
        key = str(cand).strip().lower()
        for low, actual in norm.items():
            if low.startswith(key):
                return actual
    return None

def to_number(series: pd.Series) -> pd.Series:
    """Coerce numbers safely (strip commas/currency)."""
    return pd.to_numeric(
        series.astype(str)
              .str.replace(",", "", regex=False)
              .str.replace("$", "", regex=False)
              .str.replace("SAR", "", regex=False)
              .str.replace("AED", "", regex=False)
              .str.strip(),
        errors="coerce"
    )


def load_affiliate_mapping_from_xlsx(xlsx_path: str, sheet_name: str) -> pd.DataFrame:
    """Return mapping with columns code_norm, affiliate_ID, type_norm, pct_new, pct_old, fixed_new, fixed_old."""
    df_sheet = pd.read_excel(xlsx_path, sheet_name=sheet_name, dtype=str)

    code_col = pick_col(df_sheet, 'code', 'coupon code', 'coupon')
    aff_col = pick_col(df_sheet, 'id', 'affiliate_id', 'affiliate id')
    type_col = pick_col(df_sheet, 'type', 'payout type', 'commission type')
    payout_col = pick_col(df_sheet, 'payout', 'commission', 'rate')
    new_col = pick_col(df_sheet, 'new customer payout', 'new payout', 'ftu payout', 'acquisition payout')
    old_col = pick_col(df_sheet, 'old customer payout', 'existing customer payout', 'returning customer payout', 'rtu payout')

    if not code_col:
        raise ValueError(f"[{sheet_name}] must contain a 'Code' column.")
    if not aff_col:
        raise ValueError(f"[{sheet_name}] must contain an 'ID' (or 'affiliate_ID') column.")
    if not type_col:
        raise ValueError(f"[{sheet_name}] must contain a 'type' column (revenue/sale/fixed).")
    if not (payout_col or new_col or old_col):
        raise ValueError(f"[{sheet_name}] must contain at least one payout-like column.")

    def extract_numeric(col_name: str) -> pd.Series:
        if not col_name:
            return pd.Series([pd.NA] * len(df_sheet), dtype='Float64')
        raw = df_sheet[col_name].astype(str).str.replace('%', '', regex=False).str.strip()
        return pd.to_numeric(raw, errors='coerce')

    payout_any = extract_numeric(payout_col)
    payout_new_raw = extract_numeric(new_col).fillna(payout_any)
    payout_old_raw = extract_numeric(old_col).fillna(payout_any)

    type_norm = (
        df_sheet[type_col]
        .astype(str)
        .str.strip()
        .str.lower()
        .replace({'': None})
        .fillna('revenue')
    )

    def pct_from(values: pd.Series) -> pd.Series:
        pct = values.where(type_norm.isin(['revenue', 'sale']))
        return pct.apply(lambda v: (v / 100.0) if pd.notna(v) and v > 1 else (v if pd.notna(v) else pd.NA))

    def fixed_from(values: pd.Series) -> pd.Series:
        return values.where(type_norm.eq('fixed'))

    pct_new = pct_from(payout_new_raw)
    pct_old = pct_from(payout_old_raw)
    pct_new = pct_new.fillna(pct_old)
    pct_old = pct_old.fillna(pct_new)

    fixed_new = fixed_from(payout_new_raw)
    fixed_old = fixed_from(payout_old_raw)
    fixed_new = fixed_new.fillna(fixed_old)
    fixed_old = fixed_old.fillna(fixed_new)

    out = pd.DataFrame({
        'code_norm': df_sheet[code_col].apply(normalize_coupon),
        'affiliate_ID': df_sheet[aff_col].fillna('').astype(str).str.strip(),
        'type_norm': type_norm,
        'pct_new': pd.to_numeric(pct_new, errors='coerce').fillna(DEFAULT_PCT_IF_MISSING),
        'pct_old': pd.to_numeric(pct_old, errors='coerce').fillna(DEFAULT_PCT_IF_MISSING),
        'fixed_new': pd.to_numeric(fixed_new, errors='coerce'),
        'fixed_old': pd.to_numeric(fixed_old, errors='coerce'),
    }).dropna(subset=['code_norm'])

    return out.drop_duplicates(subset=['code_norm'], keep='last')


# =======================
# LOAD INPUT
# =======================
today = datetime.now().date()
start_date = today - timedelta(days=days_back)
print(f"Window: {start_date} ≤ date < {today}  (days_back={days_back})")

# Dynamically select the changing report CSV
input_file = find_matching_csv(input_dir, REPORT_PREFIX)
print(f"Using report file: {input_file}")

df_raw = pd.read_csv(input_file)
df_raw.columns = [c.strip() for c in df_raw.columns]

# Resolve columns flexibly
date_col     = pick_col(df_raw, "date", "order date")
ctype_col    = pick_col(df_raw, "customer type", "cust type", "user type")
amount_col   = pick_col(df_raw, "amount", "sale amount", "value", "order value")
coupon_col   = pick_col(df_raw, "coupon", "coupon code", "code")
country_col  = pick_col(df_raw, "country", "geo", "market")

missing = [n for n, c in {
    "date": date_col,
    "customer type": ctype_col,
    "amount": amount_col,
    "coupon": coupon_col,
    "country": country_col
}.items() if c is None]
if missing:
    raise KeyError(f"Missing expected column(s): {missing}. Columns present: {list(df_raw.columns)}")

df = df_raw.rename(columns={
    date_col: "date",
    ctype_col: "customer_type",
    amount_col: "amount",
    coupon_col: "coupon",
    country_col: "country",
})

# Clean & types
df["date"] = pd.to_datetime(df["date"], errors="coerce")
df = df.dropna(subset=["date"])

# Filter rolling window (yesterday-only when days_back=1)
df = df[(df["date"].dt.date >= start_date) & (df["date"].dt.date < today)].copy()

# Build revenue: new = 10.0, returning = 0.5
df["customer_type"] = df["customer_type"].astype(str).str.strip().str.lower()
df["revenue"] = df["customer_type"].map({"new": 10.0, "returning": 0.5}).fillna(0.0)

# sale_amount from 'amount' (assume already USD in source)
df["sale_amount"] = to_number(df["amount"]).fillna(0.0)

# Normalize coupon for join
df["coupon_norm"] = df["coupon"].apply(normalize_coupon)

# =======================
# JOIN AFFILIATE MAPPING
# =======================
map_df = load_affiliate_mapping_from_xlsx(affiliate_xlsx_path, AFFILIATE_SHEET)
dfj = df.merge(map_df, how="left", left_on="coupon_norm", right_on="code_norm")

# Normalize mapping fields
dfj["affiliate_ID"] = dfj["affiliate_ID"].fillna("").astype(str).str.strip()
dfj["type_norm"] = dfj["type_norm"].fillna("revenue")
for col in ['pct_new', 'pct_old']:
    dfj[col] = pd.to_numeric(dfj.get(col), errors='coerce').fillna(DEFAULT_PCT_IF_MISSING)
for col in ['fixed_new', 'fixed_old']:
    dfj[col] = pd.to_numeric(dfj.get(col), errors='coerce')
is_new_customer = infer_is_new_customer(dfj)
pct_effective = dfj['pct_new'].where(is_new_customer, dfj['pct_old'])
dfj['pct_fraction'] = pd.to_numeric(pct_effective, errors='coerce').fillna(DEFAULT_PCT_IF_MISSING)
fixed_effective = dfj['fixed_new'].where(is_new_customer, dfj['fixed_old'])
dfj['fixed_amount'] = pd.to_numeric(fixed_effective, errors='coerce')

# =======================
# COMPUTE PAYOUT
# =======================
payout = pd.Series(0.0, index=dfj.index)

mask_rev   = dfj["type_norm"].str.lower().eq("revenue")
mask_sale  = dfj["type_norm"].str.lower().eq("sale")
mask_fixed = dfj["type_norm"].str.lower().eq("fixed")

payout.loc[mask_rev]   = dfj.loc[mask_rev,   "revenue"]     * dfj.loc[mask_rev,   "pct_fraction"]
payout.loc[mask_sale]  = dfj.loc[mask_sale,  "sale_amount"] * dfj.loc[mask_sale,  "pct_fraction"]
payout.loc[mask_fixed] = dfj.loc[mask_fixed, "fixed_amount"].fillna(0.0)

# Empty affiliate -> set payout=0 and affiliate_ID="1"
mask_no_aff = dfj["affiliate_ID"].eq("")
payout.loc[mask_no_aff] = 0.0
dfj.loc[mask_no_aff, "affiliate_ID"] = "1"

dfj["payout"] = payout.round(2)

# =======================
# BUILD FINAL OUTPUT (standard schema)
# =======================
output_df = pd.DataFrame({
    "offer":        OFFER_ID,
    "affiliate_id": dfj["affiliate_ID"],
    "date":         dfj["date"].dt.strftime("%m-%d-%Y"),
    "status":       STATUS_DEFAULT,
    "payout":       dfj["payout"],
    "revenue":      dfj["revenue"].round(2),
    "sale amount":  dfj["sale_amount"].round(2),
    "coupon":       dfj["coupon_norm"],
    "geo":          dfj["country"],
})

# =======================
# SAVE
# =======================
output_df.to_csv(output_file, index=False)

print(f"Saved: {output_file}")
print(
    f"Rows: {len(output_df)} | "
    f"Coupons w/ fallback affiliate_id=1: {int(mask_no_aff.sum())} | "
    f"Type counts -> revenue: {int(mask_rev.sum())}, sale: {int(mask_sale.sum())}, fixed: {int(mask_fixed.sum())}"
)
if not output_df.empty:
    print(f"Date range processed: {output_df['date'].min()} → {output_df['date'].max()}")
else:
    print("No rows after processing.")
