import os
import re
from typing import List

import pandas as pd

# =======================
# CONFIG
# =======================
OFFER_ID = 1345
STATUS_DEFAULT = "pending"
DEFAULT_PCT_IF_MISSING = 0.0
FALLBACK_AFFILIATE_ID = "1"
SAR_TO_USD = 3.75
REVENUE_RATE = 0.10  # 10% of sale amount

INPUT_XLSX = "28 Dec. order report Digizag.xlsx"
OLD_SHEET = "old system"
NEW_SHEET = "new system"
DIGIZAG_SHEET = "Digizag"
SHEET_RENAMES = {
    OLD_SHEET: {"Order Date": "order_date", "NetAmount": "final_amount", "Coupon": "coupon_code"},
    NEW_SHEET: {"Date": "order_date", "amount": "final_amount", "Coupon": "coupon_code"},
    DIGIZAG_SHEET: {
        "date": "order_date",
        "total_amount_without_tax": "final_amount",
        "Sum of total_amount_without_tax": "final_amount",
        "voucher_code": "coupon_code",
    },
}

AFFILIATE_XLSX = "Offers Coupons.xlsx"
AFFILIATE_SHEET = "Whites"

OUTPUT_CSV = "whites-temp.csv"

# =======================
# PATHS
# =======================
script_dir = os.path.dirname(os.path.abspath(__file__))
updates_dir = os.path.dirname(script_dir)
input_dir = os.path.join(updates_dir, "input data")
output_dir = os.path.join(updates_dir, "output data")
os.makedirs(output_dir, exist_ok=True)

input_path = os.path.join(input_dir, INPUT_XLSX)
affiliate_xlsx_path = os.path.join(input_dir, AFFILIATE_XLSX)
output_file = os.path.join(output_dir, OUTPUT_CSV)


# =======================
# HELPERS
# =======================
def normalize_coupon(x: str) -> str:
    """Uppercase, trim, and take first token if multiple codes separated by ; , or whitespace."""
    if pd.isna(x):
        return ""
    s = str(x).strip()
    if not s or s.lower() in {"nan", "none", "null"}:
        return ""
    s = s.upper()
    parts = re.split(r"[;,\s]+", s)
    return parts[0] if parts else s


def load_affiliate_mapping_from_xlsx(xlsx_path: str, sheet_name: str) -> pd.DataFrame:
    """Return mapping with columns code_norm, affiliate_ID, type_norm, pct_new, pct_old, fixed_new, fixed_old."""
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


def infer_is_new_customer(df: pd.DataFrame) -> pd.Series:
    """Infer a boolean new-customer flag from common columns; default False when no signal."""
    if df.empty:
        return pd.Series(False, index=df.index, dtype=bool)

    candidates: List[str] = [
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


def _load_sheet(xls: pd.ExcelFile, sheet: str, rename_map: dict) -> pd.DataFrame:
    df = pd.read_excel(xls, sheet_name=sheet)
    rename_norm = {str(k).strip().lower(): v for k, v in rename_map.items()}
    df = df.rename(columns=lambda c: rename_norm.get(str(c).strip().lower(), c))

    # Fallbacks for common column name variants
    cols_lower = {str(c).strip().lower(): c for c in df.columns}
    if "final_amount" not in df.columns:
        for cand in [
            "gross_amount",
            "gross amount",
            "grosssales",
            "gross sales",
            "total_amount_without_tax",
            "sum of total_amount_without_tax",
            "sum of total amount without tax",
            "netamount",
            "net amount",
            "amount",
        ]:
            if cand in cols_lower:
                df = df.rename(columns={cols_lower[cand]: "final_amount"})
                break
    if "order_date" not in df.columns:
        for cand in ["order date", "order_date", "date"]:
            if cand in cols_lower:
                df = df.rename(columns={cols_lower[cand]: "order_date"})
                break
    if "coupon_code" not in df.columns:
        for cand in ["coupon", "voucher_code"]:
            if cand in cols_lower:
                df = df.rename(columns={cols_lower[cand]: "coupon_code"})
                break

    expected = {"order_date", "final_amount", "coupon_code"}
    missing_cols = expected - set(df.columns)
    if missing_cols:
        raise KeyError(f"Missing columns in '{sheet}': {sorted(missing_cols)}")
    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce", dayfirst=True)
    df["final_amount"] = pd.to_numeric(df["final_amount"], errors="coerce").fillna(0.0)
    df["coupon_code"] = df["coupon_code"].astype(str)
    return df[list(expected)]


# =======================
# LOAD & MERGE SOURCE SHEETS
# =======================
if not os.path.exists(input_path):
    raise FileNotFoundError(f"Input file not found: {input_path}")

xls = pd.ExcelFile(input_path)
available_sheets = [sheet for sheet in SHEET_RENAMES if sheet in xls.sheet_names]
missing_sheets = [sheet for sheet in SHEET_RENAMES if sheet not in xls.sheet_names]
if not available_sheets:
    raise ValueError(
        f"No expected sheet found in '{INPUT_XLSX}'. Looking for {list(SHEET_RENAMES)}. "
        f"Available sheets: {xls.sheet_names}"
    )
if missing_sheets:
    print(f"Skipping missing sheets (not in workbook): {missing_sheets}")

source_frames = [_load_sheet(xls, sheet, SHEET_RENAMES[sheet]) for sheet in available_sheets]
combined_df = pd.concat(source_frames, ignore_index=True)
combined_df = combined_df.dropna(subset=["order_date"])

if combined_df.empty:
    raise ValueError("No rows to process after combining sheets.")

# =======================
# DERIVED FIELDS
# =======================
combined_df["sale_amount"] = combined_df["final_amount"] / SAR_TO_USD  # convert SAR -> USD
combined_df["revenue"] = combined_df["sale_amount"] * REVENUE_RATE
combined_df = combined_df[combined_df["sale_amount"] > 0].copy()

combined_df["coupon_norm"] = combined_df["coupon_code"].apply(normalize_coupon)

# =======================
# JOIN AFFILIATE MAPPING (type-aware)
# =======================
map_df = load_affiliate_mapping_from_xlsx(affiliate_xlsx_path, AFFILIATE_SHEET)
df_joined = combined_df.merge(map_df, how="left", left_on="coupon_norm", right_on="code_norm")

missing_aff_mask = df_joined["affiliate_ID"].isna() | (df_joined["affiliate_ID"].astype(str).str.strip() == "")

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
mask_sale = df_joined["type_norm"].str.lower().eq("sale")
mask_fixed = df_joined["type_norm"].str.lower().eq("fixed")

payout.loc[mask_rev] = df_joined.loc[mask_rev, "revenue"] * df_joined.loc[mask_rev, "pct_fraction"]
payout.loc[mask_sale] = df_joined.loc[mask_sale, "sale_amount"] * df_joined.loc[mask_sale, "pct_fraction"]
payout.loc[mask_fixed] = df_joined.loc[mask_fixed, "fixed_amount"].fillna(0.0)

payout.loc[missing_aff_mask] = 0.0
df_joined.loc[missing_aff_mask, "affiliate_ID"] = FALLBACK_AFFILIATE_ID

df_joined["payout"] = payout.round(2)

# =======================
# OUTPUT
# =======================
output_df = pd.DataFrame(
    {
        "offer": OFFER_ID,
        "affiliate_id": df_joined["affiliate_ID"],
        "date": pd.to_datetime(df_joined["order_date"], errors="coerce").dt.strftime("%m-%d-%Y"),
        "status": STATUS_DEFAULT,
        "payout": df_joined["payout"],
        "revenue": df_joined["revenue"].round(2),
        "sale amount": df_joined["sale_amount"].round(2),
        "coupon": df_joined["coupon_norm"],
        "geo": "ksa",
    }
)

output_df.to_csv(output_file, index=False)

print(f"Saved: {output_file}")
print(
    f"Rows: {len(output_df)} | "
    f"No-affiliate coupons (set aff={FALLBACK_AFFILIATE_ID}, payout=0): {int(missing_aff_mask.sum())}"
)
if not output_df.empty:
    print(f"Date range processed: {output_df['date'].min()} to {output_df['date'].max()}")
else:
    print("No rows after processing.")
