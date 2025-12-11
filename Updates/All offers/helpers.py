import os
import pandas as pd
import numpy as np
import re
from typing import List, Optional

# DEFAULT_PCT_IF_MISSING = None
# OFFER_SHEET_BY_ID = None

# =======================
# HELPERS
# =======================
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
        """Remove non alpha-numeric characters."""
        if pd.isna(value):
            return set()
        # Why replace non-alphanumeric with space when new_tokens and old_tokens strings don't have spaces?
        text = ''.join(ch if ch.isalnum() else ' ' for ch in str(value).lower()) 
        return {tok for tok in text.split() if tok}

    for key in candidates: # Check if candidates match actual dataframe columns.
        actual = columns_map.get(key)
        if not actual:
            continue
        tokens_series = df[actual].apply(tokenize)
        
        # Checking if tokenenized data matches tokens in sets.
        is_new = tokens_series.apply(lambda toks: bool(toks & new_tokens))  
        is_old = tokens_series.apply(lambda toks: bool(toks & old_tokens))

        # Checking to see if data of column matches presumed data of sets 
        recognized = (is_new | is_old) & ~resolved
        if recognized.any():
            result.loc[recognized] = is_new.loc[recognized]
            resolved.loc[recognized] = True
        if resolved.all():
            break
    return result

def pick_payout_column(cols_lower_map):
    """Priority for payout column: payout > new customer payout > old customer payout."""
    for candidate in ["payout", "new customer payout", "old customer payout"]: # Prioritizes newer column.
        if candidate in cols_lower_map:
            return cols_lower_map[candidate]
    return None

def load_affiliate_mapping_from_xlsx(xlsx_path: str, offer_id: int) -> pd.DataFrame:
    """Return mapping with columns code_norm, affiliate_ID, type_norm, pct_new, pct_old, fixed_new, fixed_old."""
    sheet_name = OFFER_SHEET_BY_ID.get(offer_id)
    if not sheet_name:
        raise ValueError(f"No sheet mapping defined for offer {offer_id}. Please add it to OFFER_SHEET_BY_ID.")

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
    payout_col = pick_payout_column(cols_lower)
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

    type_norm = ( # Normalize type col data.
        df_sheet[type_col]
        .astype(str)
        .str.strip()
        .str.lower()
        .replace({'': None})
        .fillna('revenue')
    )

    def pct_from(values: pd.Series) -> pd.Series: # Get payout percentage
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

    if not candidates: # Lists every file in the input folder.
        available = [f for f in os.listdir(directory) if f.lower().endswith(".csv")]
        raise FileNotFoundError(
            f"No .csv file starting with '{prefix}' found in: {directory}\n"
            f"Available .csv files: {available}"
        )

    exact = [p for p in candidates if os.path.basename(p).lower() == (prefix_lower + ".csv")]
    # Returns if there is a file with exact name as prefix
    if exact:
        return exact[0]


    # Otherwise, returns most recent file.
    return max(candidates, key=os.path.getmtime)

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

def parse_route(route_val: str):


    """
    Parse a route like 'KSA-KSA', 'KSA > KSA', 'SAUDI ARABIA to UAE'.
    Return (origin, dest) uppercase or (None, None).
    """
    if pd.isna(route_val):
        return None, None
    s = str(route_val).strip()
    if not s:
        return None, None
    s_norm = re.sub(r"\s*(?:-|–|—|>|to|\/|→)\s*", "-", s, flags=re.IGNORECASE)
    parts = [p.strip().upper() for p in s_norm.split("-") if p.strip()]
    if len(parts) >= 2:
        return parts[0], parts[1]
    return None, None

def _canonical_csv_column(name: str) -> str:
    clean = str(name).strip().lower()
    compact = re.sub(r'[^a-z0-9]+', '', clean)

    if 'offerid' in compact:
        return 'offer_id'
    if any(token in compact for token in ['datetime', 'orderdate', 'processdate', 'transactiondate', 'createdat', 'date']):
        return 'order_date'
    if any(token in compact for token in ['couponcode', 'coupon', 'promo', 'voucher', 'affiliateinfo', 'affiliatecode', 'code']):
        return 'coupon_code'
    if any(token in compact for token in ['revenue', 'commission', 'earned', 'netrevenue']):
        return 'revenue'
    if any(token in compact for token in ['saleamount', 'ordervalue', 'orderamount', 'grossamount', 'amount', 'payoutamount']):
        return 'sale_amount'
    if 'geo' in compact or 'country' in compact or 'market' in compact:
        return 'geo'
    return None

def fetch_json_resource(resource: str, timeout: int = 60):
    if os.path.exists(resource):
        with open(resource, 'r', encoding='utf-8') as fp:
            return json.load(fp)

    if not resource.lower().startswith(('http://', 'https://')):
        raise ValueError(
            "AL_DAKHEEL_SOURCE must be an HTTP(S) endpoint or an existing file path. "
            f"Got: {resource}"
        )

    req = url_request.Request(resource, headers={'User-Agent': 'python-urllib'})
    try:
        with url_request.urlopen(req, timeout=timeout) as resp:
            status = getattr(resp, 'status', None)
            if status and status != 200:
                raise RuntimeError(
                    f"Failed to fetch data from {resource}, status code: {status}"
                )
            data = resp.read()
    except url_error.HTTPError as exc:
        raise RuntimeError(
            f"Failed to fetch data from {resource}, status code: {exc.code}"
        ) from exc
    except url_error.URLError as exc:
        raise RuntimeError(f"Failed to fetch data from {resource}: {exc}") from exc

    return json.loads(data.decode('utf-8'))

def normalize_json_to_df(payload_json) -> pd.DataFrame:
    root = pd.json_normalize(payload_json)
    if 'data' in root.columns:
        series = root['data']
        exploded = series.explode().dropna()
        df_rows = pd.json_normalize(exploded)
    else:
        df_rows = pd.json_normalize(payload_json) if isinstance(payload_json, list) else root

    rename_map = {}
    for c in list(df_rows.columns):
        low = str(c).lower().strip()
        if low in {'order_date', 'date', 'transaction_date', 'process_date', 'orderdate'}:
            rename_map[c] = 'order_date'
        elif low in {'coupon_code', 'coupon', 'code', 'promo', 'voucher', 'promo_code'}:
            rename_map[c] = 'coupon_code'
        elif low in {'revenue', 'revenue_usd', 'net_revenue', 'commission', 'earned', 'network_revenue'}:
            rename_map[c] = 'revenue'
        elif low in {
            'sale_amount', 'order_value', 'final_amount', 'amount', 'total_amount',
            'gross_amount', 'cart_value', 'order_amount', 'order_total'
        }:
            rename_map[c] = 'sale_amount'

    if rename_map:
        df_rows = df_rows.rename(columns=rename_map)

    if df_rows.columns.duplicated().any():
        df_rows = df_rows.loc[:, ~df_rows.columns.duplicated()]

    required_basic = ['order_date', 'coupon_code']
    missing_basic = [c for c in required_basic if c not in df_rows.columns]
    if missing_basic:
        raise ValueError(f"Missing required fields from JSON: {missing_basic}")

    if not (('revenue' in df_rows.columns) or ('sale_amount' in df_rows.columns)):
        raise ValueError("JSON source must include 'revenue' or 'sale_amount'.")

    for col in ['revenue', 'sale_amount']:
        if col in df_rows.columns:
            df_rows[col] = pd.to_numeric(df_rows[col], errors='coerce')

    return df_rows

def normalize_csv(csv_path: str) -> pd.DataFrame:
    df_raw = pd.read_csv(csv_path)
    if df_raw.empty:
        return pd.DataFrame(columns=['offer_id', 'order_date', 'coupon_code', 'revenue', 'sale_amount', 'geo'])

    df_raw.columns = [str(c).strip() for c in df_raw.columns]
    canonical_columns = [_canonical_csv_column(col) or col for col in df_raw.columns]
    df = df_raw.copy()
    df.columns = canonical_columns

    if df.columns.duplicated().any():
        df = df.T.groupby(level=0).first().T

    df = df.loc[:, [c for c in df.columns if not str(c).startswith('Unnamed')]]

    for optional in ['revenue', 'sale_amount', 'offer_id', 'geo']:
        if optional not in df.columns:
            df[optional] = pd.NA

    required_basic = {'order_date', 'coupon_code'}
    missing_basic = [c for c in required_basic if c not in df.columns]
    if missing_basic:
        raise ValueError(f"CSV source missing required columns after normalization: {missing_basic}")

    return df.reset_index(drop=True)

def load_source(resource: str) -> pd.DataFrame:
    if resource.lower().startswith(('http://', 'https://')):
        payload = fetch_json_resource(resource, timeout=60)
        return normalize_json_to_df(payload)

    if resource.lower().endswith('.json'):
        payload = fetch_json_resource(resource, timeout=60)
        return normalize_json_to_df(payload)

    if not os.path.isabs(resource):
        candidate = os.path.join(input_dir, resource)
        if os.path.exists(candidate):
            resource = candidate
        elif os.path.exists(os.path.join(script_dir, resource)):
            resource = os.path.join(script_dir, resource)
        elif os.path.exists(resource):
            resource = os.path.abspath(resource)
        else:
            resource = candidate

    if not os.path.exists(resource):
        raise FileNotFoundError(f"Al Dakheel data source not found: {resource}")

    return normalize_csv(resource)

def parse_route(route_val: str):
    """
    Parse a route like 'KSA-KSA', 'KSA > KSA', 'SAUDI ARABIA to UAE'.
    Return (origin, dest) uppercase or (None, None).
    """
    if pd.isna(route_val):
        return None, None
    s = str(route_val).strip()
    if not s:
        return None, None
    s_norm = re.sub(r"\s*(?:-|–|—|>|to|\/|→)\s*", "-", s, flags=re.IGNORECASE)
    parts = [p.strip().upper() for p in s_norm.split("-") if p.strip()]
    if len(parts) >= 2:
        return parts[0], parts[1]
    return None, None

def col_by_letter(df: pd.DataFrame, letter: str) -> str:
    """Return actual column name by Excel letter (A=0, B=1, ...)."""
    idx = ord(letter.upper()) - ord('A')
    if idx < 0 or idx >= len(df.columns):
        raise IndexError(f"Column letter {letter} out of range for columns: {list(df.columns)}")
    return df.columns[idx]

def find_coupon_column(df: pd.DataFrame) -> str:
    """Try common coupon column names; return '' if none found."""
    candidates = ["Coupon Code", "Promo Code", "Coupon", "Code", "Voucher", "Voucher Code"]
    low = {c.lower().strip(): c for c in df.columns}
    for name in candidates:
        col = low.get(name.lower().strip())
        if col:
            return col
    return ""  # handle as missing

def normalize_brand(x) -> str:
    if pd.isna(x):
        return ""
    return str(x).replace("\u00A0", " ").strip().lower()

def build_master_affiliate_map(xlsx_path: str, offer_to_sheet_map: dict) -> pd.DataFrame:
    frames = []
    for offer_id, sheet in offer_to_sheet_map.items():
        try:
            frames.append(load_affiliate_mapping_for_offer(xlsx_path, sheet, offer_id))
        except Exception as e:
            print(f"Warning: skipped sheet '{sheet}' for offer {offer_id}: {e}")
    if not frames:
        return pd.DataFrame(columns=['offer','code_norm','affiliate_ID','pct_fraction'])
    return pd.concat(frames, ignore_index=True)

def resolve_days_back(value: str | None) -> Optional[int]:
    if value is None:
        return DEFAULT_DAYS_BACK
    value = value.strip()
    if not value:
        return None
    try:
        parsed = int(value)
    except ValueError:
        return DEFAULT_DAYS_BACK
    return None if parsed < 0 else parsed

