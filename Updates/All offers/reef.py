import pandas as pd
from datetime import datetime, timedelta
import os
import re
from typing import Optional

# =======================
# CONFIG (edit here)
# =======================
days_back = 30
OFFER_ID = 1341                     # used if file doesn't provide offer_id
STATUS_DEFAULT = "pending"          # used if file doesn't provide status
DEFAULT_PCT_IF_MISSING = 0.0
FALLBACK_AFFILIATE_ID = "1"

REPORT_PREFIX   = "Reef 1341"       # will match "Reef 1341.csv" or any CSV starting with this
AFFILIATE_XLSX  = "Offers Coupons.xlsx"
AFFILIATE_SHEET = "Reef"            # change to the tab name for this offer
OUTPUT_CSV      = "reef-1341.csv"

# Currency handling (only used for RAW schema)
FX_DIVISOR_DEFAULT = 3.75           # SAR
FX_DIVISOR_AED     = 3.67           # AED

# Network revenue % basis (for RAW schema when type='revenue')
REVENUE_PCT = 0.10

# =======================
# PATHS
# =======================
script_dir = os.path.dirname(os.path.abspath(__file__))
input_dir  = os.path.join(script_dir, '..', 'input data')
output_dir = os.path.join(script_dir, '..', 'output data')
os.makedirs(output_dir, exist_ok=True)

affiliate_xlsx_path = os.path.join(input_dir, AFFILIATE_XLSX)
output_file = os.path.join(output_dir, OUTPUT_CSV)

# =======================
# DATE WINDOW
# =======================
today = datetime.now().date()
end_date = today
start_date = end_date - timedelta(days=days_back)
print(f"Window: {start_date} ≤ date < {end_date}  (days_back={days_back}, excl. today)")

# =======================
# HELPERS
# =======================
def find_matching_csv(directory: str, prefix: str) -> str:
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
    if pd.isna(x):
        return ""
    s = str(x).replace("\u00A0", " ").strip().upper()
    parts = re.split(r"[;,\s]+", s)
    return parts[0] if parts else s

def to_number(series: pd.Series) -> pd.Series:
    return pd.to_numeric(
        series.astype(str)
              .str.replace(",", "", regex=False)
              .str.replace("SAR", "", case=False, regex=False)
              .str.replace("AED", "", case=False, regex=False)
              .str.strip(),
        errors="coerce"
    )

def pick_col(df: pd.DataFrame, *cands) -> Optional[str]:
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

def resolve_sheet_name(xlsx_path: str, requested: str) -> str:
    """Return the actual sheet name, allowing loose matching on spelling/spacing."""
    try:
        workbook = pd.ExcelFile(xlsx_path)
    except FileNotFoundError:
        raise

    sheets = workbook.sheet_names
    if requested in sheets:
        return requested

    def _simplify(name: str) -> str:
        return re.sub(r"[^a-z0-9]+", "", str(name).lower())

    requested_simple = _simplify(requested)
    simple_map = { _simplify(name): name for name in sheets }

    if requested_simple in simple_map:
        resolved = simple_map[requested_simple]
        print(f"Note: Using sheet '{resolved}' instead of '{requested}'.")
        return resolved

    requested_lower = str(requested).lower()
    for name in sheets:
        if requested_lower in str(name).lower():
            print(f"Note: Using sheet '{name}' as a partial match for '{requested}'.")
            return name

    raise ValueError(
        f"Worksheet named '{requested}' not found; available sheets: {sheets}"
    )

def load_affiliate_mapping_from_xlsx(xlsx_path: str, sheet_name: str) -> pd.DataFrame:
    resolved_sheet = resolve_sheet_name(xlsx_path, sheet_name)
    df_sheet = pd.read_excel(xlsx_path, sheet_name=resolved_sheet, dtype=str)

    code_col   = pick_col(df_sheet, "code", "coupon code", "coupon")
    aff_col    = pick_col(df_sheet, "id", "affiliate_id", "affiliate id")
    type_col   = pick_col(df_sheet, "type", "payout type", "commission type")
    payout_col = pick_col(df_sheet, "payout", "new customer payout", "old customer payout", "commission", "rate")

    if not code_col:
        raise ValueError(f"[{sheet_name}] must contain a 'Code' column.")
    if not aff_col:
        raise ValueError(f"[{sheet_name}] must contain an 'ID' (or 'affiliate_ID') column.")
    if not type_col:
        raise ValueError(f"[{sheet_name}] must contain a 'type' column (revenue/sale/fixed).")
    if not payout_col:
        raise ValueError(f"[{sheet_name}] must contain a 'payout' column.")

    payout_raw = df_sheet[payout_col].astype(str).str.replace("%", "", regex=False).str.strip()
    payout_num = pd.to_numeric(payout_raw, errors="coerce")
    type_norm  = df_sheet[type_col].astype(str).str.strip().str.lower().replace({"": None})

    pct_fraction = payout_num.where(type_norm.isin(["revenue", "sale"])).apply(
        lambda v: (v/100.0) if pd.notna(v) and v > 1 else (v if pd.notna(v) else DEFAULT_PCT_IF_MISSING)
    )
    fixed_amount = payout_num.where(type_norm.eq("fixed"))

    out = (
        pd.DataFrame({
            "code_norm": df_sheet[code_col].apply(normalize_coupon),
            "affiliate_ID": df_sheet[aff_col].fillna("").astype(str).str.strip(),
            "type_norm": type_norm.fillna("revenue"),
            "pct_fraction": pct_fraction.fillna(DEFAULT_PCT_IF_MISSING),
            "fixed_amount": fixed_amount
        })
        .dropna(subset=["code_norm"])
    )
    out["has_aff"] = out["affiliate_ID"].astype(str).str.len() > 0
    out = (
        out.sort_values(by=["code_norm", "has_aff"], ascending=[True, False])
           .drop_duplicates(subset=["code_norm"], keep="first")
           .drop(columns=["has_aff"])
    )
    return out

def detect_fx_divisor(header_name: str, sample_values: pd.Series) -> float:
    """All order values are expressed in SAR, so always convert using 3.75."""
    return FX_DIVISOR_DEFAULT

# =======================
# LOAD & SCHEMA DETECTION
# =======================
input_file = find_matching_csv(input_dir, REPORT_PREFIX)
print(f"Using report file: {input_file}")
df_raw = pd.read_csv(input_file)

cols_lower = {c.lower() for c in df_raw.columns}

# Heuristic:
# - PROCESSED schema if it has 'datetime' and 'sale_amount' and 'revenue'
# - RAW schema otherwise (expects order value + coupon)
is_processed = {"datetime", "sale_amount", "revenue"}.issubset(cols_lower)

if is_processed:
    print("Detected schema: PROCESSED")
    # ========== map columns ==========
    date_col    = pick_col(df_raw, "datetime")
    sale_col    = pick_col(df_raw, "sale_amount")
    revenue_col = pick_col(df_raw, "revenue")
    coupon_col  = pick_col(df_raw, "affiliate_info1", "coupon", "coupon code", "code", "voucher", "promo code")
    status_col  = pick_col(df_raw, "status")
    offer_col   = pick_col(df_raw, "offer_id")
    geo_col     = pick_col(df_raw, "geo", "country", "market")

    df = df_raw.rename(columns={
        date_col: "Order Date",
        sale_col: "sale_amount",
        revenue_col: "revenue",
        (geo_col or "geo"): "geo"
    })

    # Date filter
    df["Order Date"] = pd.to_datetime(df["Order Date"], errors="coerce")
    df = df.dropna(subset=["Order Date"])
    df = df[(df["Order Date"].dt.date >= start_date) & (df["Order Date"].dt.date < end_date)].copy()
    print(f"Rows after date filter: {len(df)}")

    if "geo" not in df.columns:
        df["geo"] = "no-geo"
    else:
        df["geo"] = df["geo"].fillna("no-geo")

    # Coupon/code normalization if present
    if coupon_col:
        coupon_series = df_raw[coupon_col].reindex(df.index)
        df["coupon_norm"] = coupon_series.apply(normalize_coupon)
    else:
        df["coupon_norm"] = ""

    # Offer & status (align with filtered rows)
    if offer_col:
        df["offer"] = df_raw[offer_col].reindex(df.index).fillna(OFFER_ID)
    else:
        df["offer"] = OFFER_ID

    if status_col:
        df["status"] = df_raw[status_col].reindex(df.index).fillna(STATUS_DEFAULT)
    else:
        df["status"] = STATUS_DEFAULT

    # Affiliate mapping (only if we have a code to map)
    if df["coupon_norm"].str.len().gt(0).any():
        map_df = load_affiliate_mapping_from_xlsx(affiliate_xlsx_path, AFFILIATE_SHEET)
        df = df.merge(map_df, how="left", left_on="coupon_norm", right_on="code_norm")
    else:
        df["affiliate_ID"] = ""

    # Fallback affiliate where missing
    missing_aff_mask = df.get("affiliate_ID", pd.Series([""]*len(df))).isna() | (df.get("affiliate_ID", "").astype(str).str.strip() == "")
    if missing_aff_mask.any():
        print("Note: Missing affiliate_ID for some rows; applying fallback and zero payout.")
        df.loc[missing_aff_mask, "affiliate_ID"] = FALLBACK_AFFILIATE_ID

    # If the processed file already contains the *final* revenue and sale_amount,
    # we only need to compute payout when mapping provides type/pct/fixed.
    # Otherwise payout stays 0.0 for rows without mapping.
    payout = pd.Series(0.0, index=df.index)
    if {"type_norm", "pct_fraction"}.issubset(df.columns) or "fixed_amount" in df.columns:
        mask_rev   = df.get("type_norm", "").astype(str).str.lower().eq("revenue")
        mask_sale  = df.get("type_norm", "").astype(str).str.lower().eq("sale")
        mask_fixed = df.get("type_norm", "").astype(str).str.lower().eq("fixed")

        payout.loc[mask_rev]   = df.loc[mask_rev, "revenue"].fillna(0.0)      * df.loc[mask_rev, "pct_fraction"].fillna(DEFAULT_PCT_IF_MISSING)
        payout.loc[mask_sale]  = df.loc[mask_sale, "sale_amount"].fillna(0.0) * df.loc[mask_sale, "pct_fraction"].fillna(DEFAULT_PCT_IF_MISSING)
        payout.loc[mask_fixed] = df.loc[mask_fixed, "fixed_amount"].fillna(0.0)

        # zero payout for any row that fell back to default affiliate
        payout.loc[missing_aff_mask] = 0.0

    df["payout"] = payout.round(2)

    # Build output aligned with filtered rows
    output_df = pd.DataFrame({
        "offer": df["offer"],
        "affiliate_id": df["affiliate_ID"].astype(str),
        "date": df["Order Date"].dt.strftime("%m-%d-%Y"),
        "status": df["status"].astype(str),
        "payout": df["payout"].fillna(0.0),
        "revenue": df["revenue"].fillna(0.0).round(2),
        "sale amount": df["sale_amount"].fillna(0.0).round(2),
        "coupon": df["coupon_norm"].fillna(""),
        "geo": df["geo"].fillna("no-geo"),
    }).reset_index(drop=True)

else:
    print("Detected schema: RAW")
    # Expect typical raw headers
    date_col    = pick_col(df_raw, "order date", "transaction date", "process date", "date")
    sale_col    = pick_col(df_raw, "order value (sar)", "order value (aed)", "order value", "sale amount", "amount", "total")
    country_col = pick_col(df_raw, "country", "geo", "market")
    coupon_col  = pick_col(df_raw, "coupon", "coupon code", "aff_coupon", "code", "voucher", "promo code")

    missing = [n for n, c in {
        "Order Date": date_col,
        "Order Value": sale_col,
        "Country": country_col,
        "Coupon": coupon_col
    }.items() if c is None]
    if missing:
        raise KeyError(f"Missing expected column(s): {missing}. Columns found: {list(df_raw.columns)}")

    df = df_raw.rename(columns={
        date_col: "Order Date",
        sale_col: "Order Value",
        country_col: "country",
        coupon_col: "Coupon",
    })

    df["Order Date"] = pd.to_datetime(df["Order Date"], errors="coerce")
    df = df.dropna(subset=["Order Date"])
    df = df[(df["Order Date"].dt.date >= start_date) & (df["Order Date"].dt.date < end_date)].copy()
    print(f"Rows after date filter: {len(df)}")

    # FX detection
    fx_div = detect_fx_divisor("Order Value", df["Order Value"])

    # Derived amounts
    df["sale_amount"] = to_number(df["Order Value"]).fillna(0.0) / fx_div
    df["revenue"] = df["sale_amount"] * REVENUE_PCT

    # coupon & geo
    geo_mapping = {"KSA": "ksa", "SA": "ksa", "SAU": "ksa", "UAE": "uae", "AE": "uae", "KWT": "kwt", "KW": "kwt"}
    df["geo"] = df.get("country", pd.Series(["no-geo"]*len(df))).map(geo_mapping).fillna("no-geo")
    df["coupon_norm"] = df["Coupon"].apply(normalize_coupon)

    # Affiliate mapping
    map_df = load_affiliate_mapping_from_xlsx(affiliate_xlsx_path, AFFILIATE_SHEET)
    df = df.merge(map_df, how="left", left_on="coupon_norm", right_on="code_norm")

    missing_aff_mask = df["affiliate_ID"].isna() | (df["affiliate_ID"].astype(str).str.strip() == "")
    if missing_aff_mask.any():
        print("Unmatched coupons (sample):", df.loc[missing_aff_mask, "coupon_norm"].drop_duplicates().head(20).to_list())
        df.loc[missing_aff_mask, "affiliate_ID"] = FALLBACK_AFFILIATE_ID

    # Payout calc
    payout = pd.Series(0.0, index=df.index)
    mask_rev   = df["type_norm"].str.lower().eq("revenue")
    mask_sale  = df["type_norm"].str.lower().eq("sale")
    mask_fixed = df["type_norm"].str.lower().eq("fixed")

    payout.loc[mask_rev]   = df.loc[mask_rev, "revenue"]      * df.loc[mask_rev, "pct_fraction"]
    payout.loc[mask_sale]  = df.loc[mask_sale, "sale_amount"] * df.loc[mask_sale, "pct_fraction"]
    payout.loc[mask_fixed] = df.loc[mask_fixed, "fixed_amount"].fillna(0.0)

    payout.loc[missing_aff_mask] = 0.0
    df["payout"] = payout.round(2)

    # Build output
    output_df = pd.DataFrame({
        "offer": OFFER_ID,
        "affiliate_id": df["affiliate_ID"].astype(str),
        "date": df["Order Date"].dt.strftime("%m-%d-%Y"),
        "status": STATUS_DEFAULT,
        "payout": df["payout"],
        "revenue": df["revenue"].round(2),
        "sale amount": df["sale_amount"].round(2),
        "coupon": df["coupon_norm"],
        "geo": df["geo"],
    })

# =======================
# SAVE
# =======================
output_df.to_csv(output_file, index=False)
print(f"Saved: {output_file}")
print(f"Rows: {len(output_df)}")
if "affiliate_id" in output_df.columns:
    n_fallback = (output_df["affiliate_id"].astype(str) == FALLBACK_AFFILIATE_ID).sum()
    print(f"No-affiliate (fallback) rows: {int(n_fallback)}")
if not output_df.empty:
    date_series = pd.to_datetime(output_df["date"], format="%m-%d-%Y", errors="coerce").dropna()
    if not date_series.empty:
        print(
            "Date range processed: "
            f"{date_series.min().strftime('%m-%d-%Y')} → {date_series.max().strftime('%m-%d-%Y')}"
        )
