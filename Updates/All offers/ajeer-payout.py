#!/usr/bin/env python3
import os
import re
from datetime import datetime
from typing import List, Optional

import pandas as pd

# =======================
# CONFIG (Ajeer)
# =======================
OFFER_ID = 1358
STATUS_DEFAULT = "pending"
DEFAULT_PCT_IF_MISSING = 0.0
FALLBACK_AFFILIATE_ID = "1"
DEFAULT_GEO = "ksa"

REPORT_PREFIX = "DigiZag Coupon Tracking(Coupon Performance)"
AFFILIATE_XLSX = "Offers Coupons.xlsx"
AFFILIATE_SHEET = "Ajeer "
OUTPUT_CSV = "ajeer.csv"

# Source file does not contain an order date column. Use a fixed date as requested.
FORCED_DATE = datetime(2025, 11, 10).date()

# =======================
# PATHS
# =======================
script_dir = os.path.dirname(os.path.abspath(__file__))
input_dir = os.path.join(script_dir, "..", "input data")
output_dir = os.path.join(script_dir, "..", "output data")
os.makedirs(output_dir, exist_ok=True)


def find_latest_csv(prefix: str, directory: str) -> str:
    """Return the newest CSV in `directory` whose filename starts with `prefix`."""
    prefix_lower = prefix.lower()
    candidates = []
    for name in os.listdir(directory):
        if name.startswith("~$"):
            continue
        if not name.lower().endswith(".csv"):
            continue
        if name.lower().startswith(prefix_lower):
            full_path = os.path.join(directory, name)
            if os.path.isfile(full_path):
                candidates.append(full_path)
    if not candidates:
        available = [f for f in os.listdir(directory) if f.lower().endswith(".csv")]
        raise FileNotFoundError(
            f"No CSV file starting with '{prefix}' found in: {directory}\n"
            f"Available CSV files: {available}"
        )
    candidates.sort(key=os.path.getmtime, reverse=True)
    return candidates[0]


# =======================
# HELPERS
# =======================
def normalize_coupon(x: str) -> str:
    """Uppercase, trim, and take first token if multiple codes separated by ; , or whitespace."""
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


def to_number(series: pd.Series) -> pd.Series:
    """Coerce strings to numeric, stripping commas and currency markers."""
    return pd.to_numeric(
        series.astype(str)
        .str.replace(",", "", regex=False)
        .str.replace("$", "", regex=False)
        .str.replace("SAR", "", regex=False)
        .str.replace("AED", "", regex=False)
        .str.strip(),
        errors="coerce",
    )


def _norm_key(value: str) -> str:
    return re.sub(r"\s+", "", str(value)).strip().lower()


def pick_column(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    """
    Return the dataframe column that matches any candidate (case/space-insensitive).
    """
    normalized = {_norm_key(c): c for c in df.columns}
    for cand in candidates:
        key = _norm_key(cand)
        if key in normalized:
            return normalized[key]
    return None


def load_affiliate_mapping_from_xlsx(xlsx_path: str, sheet_name: str) -> pd.DataFrame:
    """
    Return mapping with columns:
    code_norm, affiliate_ID, type_norm, pct_new, pct_old, fixed_new, fixed_old, geo_pref
    """
    df_sheet = pd.read_excel(xlsx_path, sheet_name=sheet_name, dtype=str)
    if df_sheet.empty:
        return pd.DataFrame(columns=["code_norm", "affiliate_ID"])

    code_col = pick_column(df_sheet, ["code", "coupon code", "coupon"])
    aff_col = pick_column(df_sheet, ["id", "affiliate_id", "affiliate id"])
    type_col = pick_column(df_sheet, ["type", "payout type", "commission type"])
    payout_col = pick_column(df_sheet, ["payout"])
    new_col = pick_column(df_sheet, ["new customer payout", "new payout", "ftu payout"])
    old_col = pick_column(df_sheet, ["old customer payout", "existing customer payout", "rtu payout"])
    geo_col = pick_column(df_sheet, ["geo", "country", "market"])

    if not code_col:
        raise ValueError(f"[{sheet_name}] must contain a 'Code' column.")
    if not aff_col:
        raise ValueError(f"[{sheet_name}] must contain an 'ID' (affiliate) column.")
    if not type_col:
        raise ValueError(f"[{sheet_name}] must contain a 'type' column.")
    if not (payout_col or new_col or old_col):
        raise ValueError(f"[{sheet_name}] must contain at least one payout column.")

    def extract_numeric(col_name: Optional[str]) -> pd.Series:
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
        return pct.apply(
            lambda v: (v / 100.0)
            if pd.notna(v) and v > 1
            else (v if pd.notna(v) else pd.NA)
        )

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

    if geo_col:
        geo_pref = df_sheet[geo_col].fillna(DEFAULT_GEO).astype(str).str.strip().str.lower()
    else:
        geo_pref = pd.Series(DEFAULT_GEO, index=df_sheet.index)

    affiliate_ids = (
        df_sheet[aff_col]
        .fillna("")
        .astype(str)
        .str.strip()
        .str.replace(r"\.0$", "", regex=True)
    )

    out = pd.DataFrame(
        {
            "code_norm": df_sheet[code_col].apply(normalize_coupon),
            "affiliate_ID": affiliate_ids,
            "type_norm": type_norm,
            "pct_new": pd.to_numeric(pct_new, errors="coerce").fillna(DEFAULT_PCT_IF_MISSING),
            "pct_old": pd.to_numeric(pct_old, errors="coerce").fillna(DEFAULT_PCT_IF_MISSING),
            "fixed_new": pd.to_numeric(fixed_new, errors="coerce"),
            "fixed_old": pd.to_numeric(fixed_old, errors="coerce"),
            "geo_pref": geo_pref,
        }
    )
    out = out.dropna(subset=["code_norm"])
    return out.drop_duplicates(subset=["code_norm"], keep="last")


# =======================
# LOAD INPUT DATA
# =======================
report_path = find_latest_csv(REPORT_PREFIX, input_dir)
affiliate_path = os.path.join(input_dir, AFFILIATE_XLSX)
output_path = os.path.join(output_dir, OUTPUT_CSV)

print(f"Using report: {report_path}")
print(f"Using affiliate mapping: {affiliate_path}::{AFFILIATE_SHEET}")

df_raw = pd.read_csv(report_path)
if df_raw.empty:
    df_orders = pd.DataFrame(columns=["coupon", "amount"])
else:
    df_raw.columns = [str(c).strip() for c in df_raw.columns]
    rename_map = {}
    for col in df_raw.columns:
        low = col.strip().lower()
        if low == "coupon code":
            rename_map[col] = "coupon"
        elif low == "amount":
            rename_map[col] = "amount"
        elif low == "date":
            rename_map[col] = "date_raw"
        elif low in {"order no.", "order no"}:
            rename_map[col] = "order_no"
    df_orders = df_raw.rename(columns=rename_map)

if "coupon" not in df_orders.columns:
    raise KeyError(f"Column 'Coupon Code' is required in {report_path}")
if "amount" not in df_orders.columns:
    raise KeyError(f"Column 'Amount' is required in {report_path}")

df_orders["coupon_norm"] = df_orders["coupon"].apply(normalize_coupon)
raw_amount = to_number(df_orders["amount"])
# Convert SAR to USD-equivalent by dividing by 3.75, then compute 6% revenue share.
df_orders["sale_amount"] = raw_amount / 3.75
df_orders["revenue"] = df_orders["sale_amount"] * 0.06

df_orders = df_orders[
    (df_orders["coupon_norm"] != "") & df_orders["revenue"].notna()
].copy()
df_orders["revenue"] = df_orders["revenue"].fillna(0.0)
df_orders["sale_amount"] = df_orders["sale_amount"].fillna(0.0)

# enforce forced date
df_orders["date"] = pd.Timestamp(FORCED_DATE)

# =======================
# JOIN WITH AFFILIATE MAP
# =======================
mapping_df = load_affiliate_mapping_from_xlsx(affiliate_path, AFFILIATE_SHEET)
dfj = df_orders.merge(mapping_df, how="left", left_on="coupon_norm", right_on="code_norm")

dfj["affiliate_ID"] = dfj["affiliate_ID"].fillna("").astype(str).str.strip()
dfj["type_norm"] = dfj["type_norm"].fillna("revenue")
for col in ["pct_new", "pct_old"]:
    dfj[col] = pd.to_numeric(dfj.get(col), errors="coerce").fillna(DEFAULT_PCT_IF_MISSING)
for col in ["fixed_new", "fixed_old"]:
    dfj[col] = pd.to_numeric(dfj.get(col), errors="coerce")

dfj["geo_pref"] = dfj["geo_pref"].fillna(DEFAULT_GEO).astype(str).str.strip().str.lower()

is_new_customer = infer_is_new_customer(dfj)
effective_pct = dfj["pct_new"].where(is_new_customer, dfj["pct_old"]).fillna(DEFAULT_PCT_IF_MISSING)
dfj["pct_fraction"] = effective_pct
effective_fixed = dfj["fixed_new"].where(is_new_customer, dfj["fixed_old"])
dfj["fixed_amount"] = pd.to_numeric(effective_fixed, errors="coerce")

# =======================
# COMPUTE PAYOUT
# =======================
payout = pd.Series(0.0, index=dfj.index)

mask_rev = dfj["type_norm"].str.lower().eq("revenue")
mask_sale = dfj["type_norm"].str.lower().eq("sale")
mask_fixed = dfj["type_norm"].str.lower().eq("fixed")

payout.loc[mask_rev] = dfj.loc[mask_rev, "revenue"] * dfj.loc[mask_rev, "pct_fraction"]
payout.loc[mask_sale] = dfj.loc[mask_sale, "sale_amount"] * dfj.loc[mask_sale, "pct_fraction"]
payout.loc[mask_fixed] = dfj.loc[mask_fixed, "fixed_amount"].fillna(0.0)

mask_no_aff = dfj["affiliate_ID"].eq("")
payout.loc[mask_no_aff] = 0.0
dfj.loc[mask_no_aff, "affiliate_ID"] = FALLBACK_AFFILIATE_ID

dfj["payout"] = payout.round(2)

# =======================
# BUILD OUTPUT
# =======================
output_df = pd.DataFrame(
    {
        "offer": OFFER_ID,
        "affiliate_id": dfj["affiliate_ID"],
        "date": pd.to_datetime(dfj["date"]).dt.strftime("%m-%d-%Y"),
        "status": STATUS_DEFAULT,
        "payout": dfj["payout"],
        "revenue": dfj["revenue"].round(2),
        "sale amount": dfj["sale_amount"].round(2),
        "coupon": dfj["coupon_norm"],
        "geo": dfj["geo_pref"].replace("", DEFAULT_GEO),
    }
)

# Drop rows with zero payout per user request
output_df = output_df[output_df["payout"] != 0].copy()

output_df.to_csv(output_path, index=False)

print(f"Saved {len(output_df)} rows to {output_path}")
if not output_df.empty:
    print(
        f"Payout summary -> revenue rows: {int(mask_rev.sum())}, "
        f"sale rows: {int(mask_sale.sum())}, fixed rows: {int(mask_fixed.sum())}"
    )
    print(f"Forced date used for all rows: {FORCED_DATE.strftime('%m-%d-%Y')}")
