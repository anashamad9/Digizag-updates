import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import re

# =======================
# CONFIG
# =======================
days_back = 56
OFFER_ID = 1261
STATUS_DEFAULT = "pending"
DEFAULT_PCT_IF_MISSING = 0.0
FALLBACK_AFFILIATE_ID = "1"
GEO = "jor"
REVENUE_PER_ORDER = 1.0

AFFILIATE_XLSX = "Offers Coupons.xlsx"
AFFILIATE_SHEET = "Jeeny Jo"
SOURCE_SUBDIR = "jeeny-jor"
OUTPUT_FILENAME = "jeeny_jor_validation.csv"

# =======================
# PATHS
# =======================
script_dir = Path(__file__).resolve().parent
source_dir = script_dir / SOURCE_SUBDIR
updates_dir = script_dir.parent / "Updates"
input_dir = updates_dir / "input data"
affiliate_xlsx_path = input_dir / AFFILIATE_XLSX
output_file = source_dir / OUTPUT_FILENAME


# =======================
# HELPERS
# =======================
def normalize_coupon(x: str) -> str:
    """Uppercase, trim, take the first token if multiple codes separated by ; , or whitespace."""
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


def parse_date_column(series: pd.Series) -> pd.Series:
    """
    Robust date parsing:
    1) Try %d-%b-%Y (e.g., 21-Sep-2025)
    2) Fill remaining with %d-%b-%y (e.g., 21-Sep-25)
    3) Fill remaining with pandas inference (dayfirst)
    """
    s = pd.to_datetime(series, format='%d-%b-%Y', errors='coerce')
    need2 = s.isna()
    if need2.any():
        s2 = pd.to_datetime(series[need2], format='%d-%b-%y', errors='coerce')
        s.loc[need2] = s2
    need3 = s.isna()
    if need3.any():
        s3 = pd.to_datetime(series[need3], dayfirst=True, errors='coerce')
        s.loc[need3] = s3
    return s


def load_affiliate_mapping_from_xlsx(xlsx_path: Path, sheet_name: str) -> pd.DataFrame:
    """Return mapping with columns code_norm, affiliate_ID, type_norm, pct_new, pct_old, fixed_new, fixed_old."""
    df_sheet = pd.read_excel(xlsx_path, sheet_name=sheet_name, dtype=str)
    cols_lower = {str(c).lower().strip(): c for c in df_sheet.columns}

    def need(name: str) -> str:
        col = cols_lower.get(name)
        if not col:
            raise ValueError(f"[{sheet_name}] must contain a '{name}' column.")
        return col

    code_col = need('code')
    aff_col = cols_lower.get('id') or cols_lower.get('affiliate_id')
    type_col = need('type')
    payout_col = cols_lower.get('payout')
    new_col = cols_lower.get('new customer payout')
    old_col = cols_lower.get('old customer payout')

    if not aff_col:
        raise ValueError(f"[{sheet_name}] must contain an 'ID' (or 'affiliate_ID') column.")
    if not (payout_col or new_col or old_col):
        raise ValueError(f"[{sheet_name}] must contain at least one payout column (e.g., 'payout').")

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


def load_combined_reports(directory: Path) -> pd.DataFrame:
    csv_files = sorted(
        csv_path
        for csv_path in directory.glob("*.csv")
        if not csv_path.name.lower().startswith("jeeny_")
    )
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found inside {directory}")

    frames = []
    header = None
    for csv_path in csv_files:
        df_part = pd.read_csv(csv_path)
        if df_part.empty:
            continue
        cols = list(df_part.columns)
        if header is None:
            header = cols
        elif cols != header:
            raise ValueError(
                f"Header mismatch in {csv_path.name}. Expected {header} but found {cols}."
            )
        frames.append(df_part)

    if not frames:
        raise ValueError("All CSV files were empty; nothing to process.")
    return pd.concat(frames, ignore_index=True)


# =======================
# MAIN LOGIC
# =======================
def main():
    df = load_combined_reports(source_dir)

    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=days_back)
    today = datetime.now().date()
    print(f"Current date: {today}, Start date (days_back={days_back}): {start_date}")
    print(f"Loaded {len(df)} rows from {source_dir}")

    if 'Date' not in df.columns:
        raise ValueError("Combined CSV is missing required 'Date' column.")
    df['Date'] = parse_date_column(df['Date'])
    df = df.dropna(subset=['Date'])
    df = df[df['Date'].dt.date < today]
    df = df[(df['Date'].dt.date >= start_date) & (df['Date'].dt.date <= end_date)]

    if 'Usage' not in df.columns:
        print("WARNING: 'Usage' column missing; defaulting each row to 1.")
        df['Usage'] = 1
    if 'coupon' not in df.columns:
        raise ValueError("Combined CSV is missing required 'coupon' column.")

    usage = pd.to_numeric(df['Usage'], errors='coerce').fillna(0).astype(int).clip(lower=0)
    if (usage <= 0).all():
        print("WARNING: All 'Usage' values are 0/invalid; expansion will produce 0 rows.")
    df_expanded = df.loc[df.index.repeat(usage)].reset_index(drop=True)

    if df_expanded.empty:
        print("Expanded data is empty after applying 'Usage'. The output CSV will be empty.")

    df_expanded['date_str'] = (
        df_expanded['Date'].dt.strftime('%m-%d-%Y') if not df_expanded.empty else pd.Series(dtype=str)
    )
    df_expanded['sale_amount'] = 0.0
    df_expanded['revenue'] = REVENUE_PER_ORDER
    df_expanded['coupon_norm'] = (
        df_expanded['coupon'].apply(normalize_coupon) if not df_expanded.empty else pd.Series(dtype=str)
    )

    map_df = load_affiliate_mapping_from_xlsx(affiliate_xlsx_path, AFFILIATE_SHEET)
    df_joined = df_expanded.merge(map_df, how="left", left_on="coupon_norm", right_on="code_norm")

    missing_aff_mask = df_joined['affiliate_ID'].isna() | (df_joined['affiliate_ID'].astype(str).str.strip() == "")

    df_joined['affiliate_ID'] = df_joined['affiliate_ID'].fillna('').astype(str).str.strip()
    df_joined['type_norm'] = df_joined['type_norm'].fillna('revenue')
    for col in ['pct_new', 'pct_old']:
        df_joined[col] = pd.to_numeric(df_joined.get(col), errors='coerce').fillna(DEFAULT_PCT_IF_MISSING)
    for col in ['fixed_new', 'fixed_old']:
        df_joined[col] = pd.to_numeric(df_joined.get(col), errors='coerce')

    is_new_customer = infer_is_new_customer(df_joined)
    pct_effective = df_joined['pct_new'].where(is_new_customer, df_joined['pct_old'])
    df_joined['pct_fraction'] = pd.to_numeric(pct_effective, errors='coerce').fillna(DEFAULT_PCT_IF_MISSING)
    fixed_effective = df_joined['fixed_new'].where(is_new_customer, df_joined['fixed_old'])
    df_joined['fixed_amount'] = pd.to_numeric(fixed_effective, errors='coerce')

    payout = pd.Series(0.0, index=df_joined.index)
    mask_rev = df_joined['type_norm'].str.lower().eq('revenue')
    payout.loc[mask_rev] = df_joined.loc[mask_rev, 'revenue'] * df_joined.loc[mask_rev, 'pct_fraction']

    mask_sale = df_joined['type_norm'].str.lower().eq('sale')
    payout.loc[mask_sale] = df_joined.loc[mask_sale, 'sale_amount'] * df_joined.loc[mask_sale, 'pct_fraction']

    mask_fixed = df_joined['type_norm'].str.lower().eq('fixed')
    payout.loc[mask_fixed] = df_joined.loc[mask_fixed, 'fixed_amount'].fillna(0.0)

    payout.loc[missing_aff_mask] = 0.0
    df_joined.loc[missing_aff_mask, 'affiliate_ID'] = FALLBACK_AFFILIATE_ID
    df_joined['payout'] = payout.round(2)

    output_df = pd.DataFrame({
        'offer': OFFER_ID,
        'affiliate_id': df_joined['affiliate_ID'],
        'date': df_joined['date_str'],
        'status': STATUS_DEFAULT,
        'payout': df_joined['payout'],
        'revenue': df_joined['revenue'].round(2),
        'sale amount': df_joined['sale_amount'].round(2),
        'coupon': df_joined['coupon_norm'],
        'geo': GEO,
    })

    output_df.to_csv(output_file, index=False)
    print(f"Saved: {output_file}")
    print(
        f"Rows: {len(output_df)} | "
        f"Coupons with no affiliate (set aff={FALLBACK_AFFILIATE_ID}, payout=0): {int(missing_aff_mask.sum())} | "
        f"Type counts -> revenue: {int(mask_rev.sum())}, sale: {int(mask_sale.sum())}, fixed: {int(mask_fixed.sum())}"
    )
    rng_min = output_df['date'].min() if not output_df.empty else 'N/A'
    rng_max = output_df['date'].max() if not output_df.empty else 'N/A'
    print(f"Date range processed: {rng_min} to {rng_max}")


if __name__ == "__main__":
    main()
