import os
import re
from datetime import datetime, timedelta

import pandas as pd

# =======================
# CONFIG (Osma)
# =======================
days_back = 16
OFFER_ID = 1342
GEO = "ksa"
STATUS_DEFAULT = "pending"
DEFAULT_PCT_IF_MISSING = 0.0
FALLBACK_AFFILIATE_ID = "1"

REPORT_PREFIX = "Reef& Osma Digizag update"
REPORT_SHEET = "Osma"
AFFILIATE_XLSX = "Offers Coupons.xlsx"
AFFILIATE_SHEET = "Osma"
OUTPUT_CSV = "osma.csv"

# keep all statuses; no filtering required
STATUS_ALLOWLIST = None
SALE_COL = "صافى المبيعات"
COUPON_COL = "كود الكوبون"
YEAR_COL = "Date - Year"
MONTH_COL = "Date - Month"
DAY_COL = "Date - Day"
STATUS_COL = "حالة الطلب"
GEO_COL = "الدول"

# =======================
# PATHS
# =======================
script_dir = os.path.dirname(os.path.abspath(__file__))
input_dir = os.path.join(script_dir, "..", "input data")
output_dir = os.path.join(script_dir, "..", "output data")
os.makedirs(output_dir, exist_ok=True)

def resolve_report_path(prefix: str, directory: str, extension: str = ".xlsx") -> str:
    candidates = []
    for name in os.listdir(directory):
        if not name.startswith(prefix):
            continue
        if not name.lower().endswith(extension.lower()):
            continue
        full_path = os.path.join(directory, name)
        if os.path.isfile(full_path):
            candidates.append(full_path)
    if not candidates:
        raise FileNotFoundError(f"No report file starting with '{prefix}' found in {directory}")
    candidates.sort(key=os.path.getmtime, reverse=True)
    return candidates[0]


report_path = resolve_report_path(REPORT_PREFIX, input_dir)

affiliate_xlsx_path = os.path.join(input_dir, AFFILIATE_XLSX)
output_file = os.path.join(output_dir, OUTPUT_CSV)

# =======================
# HELPERS
# =======================
def _normalize_geo_key(value: str) -> str:
    text = str(value).strip().lower()
    replacements = {
        "إ": "ا",
        "أ": "ا",
        "آ": "ا",
        "ة": "ه",
        "ى": "ي",
        "ؤ": "و",
        "ئ": "ي",
        "ً": "",
        "ٌ": "",
        "ٍ": "",
        "َ": "",
        "ُ": "",
        "ِ": "",
        "ّ": "",
        "ْ": "",
        "ـ": "",
    }
    for src, target in replacements.items():
        text = text.replace(src, target)
    text = re.sub(r"\s+", "", text)
    text = re.sub(r"[^a-z0-9\u0621-\u064a]", "", text)
    return text


GEO_VARIANTS = {
    "ksa": {"المملكة العربية السعودية", "السعودية"},
    "uae": {"الامارات", "الإمارات", "الإمارات العربية المتحدة", "الامارات العربية المتحدة"},
    "qtr": {"قطر"},
    "omn": {"عمان"},
    "usa": {"امريكا", "أمريكا", "الولايات المتحدة"},
    "kwt": {"الكويت"},
    "bhr": {"البحرين"},
    "holand": {"هولندا"},
    "uk": {"انجلترا", "المملكة المتحدة"},
}

GEO_LOOKUP = {}
for code, variants in GEO_VARIANTS.items():
    for variant in variants:
        GEO_LOOKUP[_normalize_geo_key(variant)] = code


def map_geo(value) -> str:
    if pd.isna(value):
        return GEO
    key = _normalize_geo_key(value)
    return GEO_LOOKUP.get(key, GEO)


def normalize_coupon(value: str) -> str:
    if pd.isna(value):
        return ""
    s = str(value).strip().upper()
    parts = re.split(r"[;,\s]+", s)
    return parts[0] if parts else s


def infer_is_new_customer(df: pd.DataFrame) -> pd.Series:
    """Infer a boolean new-customer flag from common columns; default False when no signal."""
    if df.empty:
        return pd.Series(False, index=df.index, dtype=bool)

    candidates = [
        "customer_type",
        "customer type",
        "customer segment",
        "customersegment",
        "new_vs_old",
        "new vs old",
        "new/old",
        "new old",
        "new_vs_existing",
        "new vs existing",
        "user_type",
        "user type",
        "usertype",
        "type_customer",
        "type customer",
        "audience",
        "تصنيف العميل",
    ]

    new_tokens = {
        "new",
        "newuser",
        "newusers",
        "newcustomer",
        "newcustomers",
        "ftu",
        "first",
        "firstorder",
        "firsttime",
        "acquisition",
        "prospect",
        "جديد",
    }
    old_tokens = {
        "old",
        "olduser",
        "oldcustomer",
        "existing",
        "existinguser",
        "existingcustomer",
        "return",
        "returning",
        "repeat",
        "rtu",
        "retention",
        "loyal",
        "existingusers",
        "حالي",
        "قديم",
    }

    columns_map = {str(c).strip().lower(): c for c in df.columns}
    result = pd.Series(False, index=df.index, dtype=bool)
    resolved = pd.Series(False, index=df.index, dtype=bool)

    def tokenize(value) -> set:
        if pd.isna(value):
            return set()
        text = "".join(ch if ch.isalnum() else " " for ch in str(value).lower())
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


def load_affiliate_mapping_from_xlsx(xlsx_path: str, sheet_name: str) -> pd.DataFrame:
    df_sheet = pd.read_excel(xlsx_path, sheet_name=sheet_name, dtype=str)
    cols_lower = {str(c).lower().strip(): c for c in df_sheet.columns}

    def need(name: str) -> str:
        col = cols_lower.get(name)
        if not col:
            raise ValueError(f"[{sheet_name}] must contain a '{name}' column.")
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
        raise ValueError(f"[{sheet_name}] must contain at least one payout column (e.g., 'payout').")

    def extract_numeric(col_name: str) -> pd.Series:
        if not col_name:
            return pd.Series([pd.NA] * len(df_sheet), dtype="Float64")
        raw = df_sheet[col_name].astype(str).str.replace("%", "", regex=False).str.strip()
        return pd.to_numeric(raw, errors="coerce")

    payout_any = extract_numeric(payout_col)
    payout_new_raw = extract_numeric(new_col).fillna(payout_any)
    payout_old_raw = extract_numeric(old_col).fillna(payout_any)

    type_norm = (
        df_sheet[type_col]
        .astype(str)
        .str.strip()
        .str.lower()
        .replace({"": None})
        .fillna("revenue")
    )

    def pct_from(values: pd.Series) -> pd.Series:
        pct = values.where(type_norm.isin(["revenue", "sale"]))
        return pct.apply(lambda v: (v / 100.0) if pd.notna(v) and v > 1 else (v if pd.notna(v) else pd.NA))

    def fixed_from(values: pd.Series) -> pd.Series:
        return values.where(type_norm.eq("fixed"))

    pct_new = pct_from(payout_new_raw)
    pct_old = pct_from(payout_old_raw)
    pct_new = pct_new.fillna(pct_old)
    pct_old = pct_old.fillna(pct_new)

    fixed_new = fixed_from(payout_new_raw)
    fixed_old = fixed_from(payout_old_raw)
    fixed_new = fixed_new.fillna(fixed_old)
    fixed_old = fixed_old.fillna(fixed_new)

    out = pd.DataFrame(
        {
            "code_norm": df_sheet[code_col].apply(normalize_coupon),
            "affiliate_ID": df_sheet[aff_col].fillna("").astype(str).str.strip(),
            "type_norm": type_norm,
            "pct_new": pd.to_numeric(pct_new, errors="coerce").fillna(DEFAULT_PCT_IF_MISSING),
            "pct_old": pd.to_numeric(pct_old, errors="coerce").fillna(DEFAULT_PCT_IF_MISSING),
            "fixed_new": pd.to_numeric(fixed_new, errors="coerce"),
            "fixed_old": pd.to_numeric(fixed_old, errors="coerce"),
        }
    ).dropna(subset=["code_norm"])

    return out.drop_duplicates(subset=["code_norm"], keep="last")


# =======================
# LOAD / PREPARE REPORT
# =======================
if not os.path.exists(affiliate_xlsx_path):
    raise FileNotFoundError(f"Coupons mapping not found: {affiliate_xlsx_path}")

df_raw = pd.read_excel(report_path, sheet_name=REPORT_SHEET)
df_raw.columns = [str(c).strip() for c in df_raw.columns]

required_cols = {YEAR_COL, MONTH_COL, DAY_COL, COUPON_COL, SALE_COL}
missing_cols = [col for col in required_cols if col not in df_raw.columns]
if missing_cols:
    raise KeyError(f"Missing required columns in report: {missing_cols}")

date_strings = (
    df_raw[DAY_COL].astype(str).str.zfill(2)
    + " "
    + df_raw[MONTH_COL].astype(str)
    + " "
    + df_raw[YEAR_COL].astype(str)
)
df_raw["order_date"] = pd.to_datetime(date_strings, errors="coerce", dayfirst=True)

df = df_raw.dropna(subset=["order_date"]).copy()

today = datetime.now().date()
start_date = today - timedelta(days=days_back)
df = df[(df["order_date"].dt.date >= start_date) & (df["order_date"].dt.date < today)]

if STATUS_COL in df.columns and STATUS_ALLOWLIST:
    df = df[df[STATUS_COL].astype(str).str.strip().isin(STATUS_ALLOWLIST)]

df["sale_amount"] = pd.to_numeric(df[SALE_COL], errors="coerce").fillna(0.0) / 3.75
df = df[df["sale_amount"] > 0].copy()
df["coupon_norm"] = df[COUPON_COL].apply(normalize_coupon)
if GEO_COL in df.columns:
    df["geo_norm"] = df[GEO_COL].apply(map_geo)
else:
    df["geo_norm"] = GEO

df["revenue"] = df["sale_amount"] * 0.05

if df.empty:
    raise ValueError("No rows found after filtering window/status and sale amount processing.")

# =======================
# JOIN MAPPING + PAYOUT
# =======================
map_df = load_affiliate_mapping_from_xlsx(affiliate_xlsx_path, AFFILIATE_SHEET)
df_joined = df.merge(map_df, how="left", left_on="coupon_norm", right_on="code_norm")

df_joined["affiliate_ID"] = df_joined["affiliate_ID"].fillna("").astype(str).str.strip()
df_joined["type_norm"] = df_joined["type_norm"].fillna("revenue")
for col in ["pct_new", "pct_old"]:
    df_joined[col] = pd.to_numeric(df_joined.get(col), errors="coerce").fillna(DEFAULT_PCT_IF_MISSING)
for col in ["fixed_new", "fixed_old"]:
    df_joined[col] = pd.to_numeric(df_joined.get(col), errors="coerce")

is_new_customer = infer_is_new_customer(df_joined)
pct_effective = df_joined["pct_new"].where(is_new_customer, df_joined["pct_old"])
df_joined["pct_fraction"] = pd.to_numeric(pct_effective, errors="coerce").fillna(DEFAULT_PCT_IF_MISSING)
fixed_effective = df_joined["fixed_new"].where(is_new_customer, df_joined["fixed_old"])
df_joined["fixed_amount"] = pd.to_numeric(fixed_effective, errors="coerce")

payout = pd.Series(0.0, index=df_joined.index)
mask_rev = df_joined["type_norm"].str.lower().eq("revenue")
payout.loc[mask_rev] = df_joined.loc[mask_rev, "revenue"] * df_joined.loc[mask_rev, "pct_fraction"]

mask_sale = df_joined["type_norm"].str.lower().eq("sale")
payout.loc[mask_sale] = df_joined.loc[mask_sale, "sale_amount"] * df_joined.loc[mask_sale, "pct_fraction"]

mask_fixed = df_joined["type_norm"].str.lower().eq("fixed")
payout.loc[mask_fixed] = df_joined.loc[mask_fixed, "fixed_amount"].fillna(0.0)

mask_no_aff = df_joined["affiliate_ID"].astype(str).str.strip().eq("")
payout.loc[mask_no_aff] = 0.0
df_joined.loc[mask_no_aff, "affiliate_ID"] = FALLBACK_AFFILIATE_ID

df_joined["payout"] = payout.round(2)

# =======================
# OUTPUT
# =======================
output_df = pd.DataFrame(
    {
        "offer": OFFER_ID,
        "affiliate_id": df_joined["affiliate_ID"],
        "date": df_joined["order_date"].dt.strftime("%m-%d-%Y"),
        "status": STATUS_DEFAULT,
        "payout": df_joined["payout"],
        "revenue": df_joined["revenue"].round(2),
        "sale amount": df_joined["sale_amount"].round(2),
        "coupon": df_joined["coupon_norm"],
        "geo": df_joined.get("geo_norm", GEO),
    }
)

output_df.to_csv(output_file, index=False)

print(f"Using report file: {report_path}")
print(f"Saved: {output_file}")
print(
    f"Rows: {len(output_df)} | "
    f"Coupons without affiliate_id (payout forced to 0): {int(mask_no_aff.sum())} | "
    f"Type counts -> revenue: {int(mask_rev.sum())}, sale: {int(mask_sale.sum())}, fixed: {int(mask_fixed.sum())}"
)
