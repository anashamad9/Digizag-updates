import requests
from bs4 import BeautifulSoup
import json
import time
import random
import re
import pandas as pd
import ast
import os

HEADERS_GET = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "ar",
}

HEADERS_POST = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "ar",
    "Content-Type": "application/json",
    "X-Requested-With": "XMLHttpRequest",
    "Accept": "application/json, text/javascript, */*; q=0.01"
}

OFFER_ID = 1365

day = input('DAY: ')
month = input('MONTH: ')
year = input('YEAR: ')

SHEET_NAME = "Dar Alamirat"
AFFILIATE_XLSX = "Offers Coupons.xlsx"
DEFAULT_PCT_IF_MISSING = 0.0
DEFAULT_AFF_ID_IF_MISSING = '1'

OUTPUT_CSV = f"Dar-AlAmirat_{month}_{day}_{year}.csv"
REDUNDANCY_CSV = "Dar-AlAmirat"
INPUT_CSV = F"Dar-AlAmirat_{month}_{day}_{year}.csv"

script_dir = os.path.dirname(os.path.abspath(__file__))
input_dir  = os.path.join(script_dir, '..', 'input data')
output_dir = os.path.join(script_dir, '..', 'output data')
os.makedirs(output_dir, exist_ok=True)

affiliate_xlsx_path = os.path.join(input_dir, AFFILIATE_XLSX)
# redundancy_csv_path = os.path.join(input_dir, REDUNDANCY_CSV)
output_file = os.path.join(output_dir, OUTPUT_CSV)
redun_file = os.path.join(input_dir, INPUT_CSV)

def normalize_coupon(x: str) -> str:
    """Uppercase, trim, and take the first token if multiple codes separated by ; , or whitespace."""
    if pd.isna(x):
        return ""
    s = str(x).strip().upper()
    parts = re.split(r"[;,\s]+", s)
    return parts[0] if parts else s

def find_latest_csv_by_prefix(directory: str, prefix: str) -> str:
    """
    Return the path to the most recently modified CSV whose *base name* starts with `prefix`.
    Matches e.g. 'DigiZag Dashboard_Commission Dashboard_Table.csv' or '... (3).csv'
    """
    prefix_norm = prefix.lower().strip()
    candidates = []
    for f in os.listdir(directory):
        if not f.lower().endswith(".csv"):
            continue
        base = os.path.splitext(f)[0].lower().strip()
        if base.startswith(prefix_norm):
            candidates.append(os.path.join(directory, f))
    if not candidates:
        avail = [f for f in os.listdir(directory) if f.lower().endswith(".csv")]
        raise FileNotFoundError(
            f"No CSV starting with '{prefix}' in {directory}. Available CSVs: {avail}"
        )
    return max(candidates, key=os.path.getmtime)

def load_affiliate_mapping_from_xlsx(xlsx_path: str, sheet_name: str) -> pd.DataFrame:
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
        'affiliate_ID': df_sheet[aff_col].fillna('1').astype(str).str.strip(),
        'type_norm': type_norm,
        'pct_new': pd.to_numeric(pct_new, errors='coerce').fillna(DEFAULT_PCT_IF_MISSING),
        'pct_old': pd.to_numeric(pct_old, errors='coerce').fillna(DEFAULT_PCT_IF_MISSING),
        'fixed_new': pd.to_numeric(fixed_new, errors='coerce'),
        'fixed_old': pd.to_numeric(fixed_old, errors='coerce'),
        'geo': df_sheet['Geo'],
        'URL': df_sheet['link']
    }).dropna(subset=['code_norm'])

    return out.drop_duplicates(subset=['code_norm'], keep='last')

# Extract CSRF token from HTML
def extract_csrf_token(html):
    soup = BeautifulSoup(html, "html.parser")

    meta = soup.find("meta", {"name": "csrf-token"})
    if meta:
        return meta["content"]

    inp = soup.find("input", {"name": "_token"})
    if inp:
        return inp.get("value")

    for script in soup.find_all("script"):
        if "csrfToken" in str(script):
            return str(script).split("csrfToken\":\"")[1].split("\"")[0]

    return None


# Request wrapper with retry for 429
def safe_request(method, url, session, **kwargs):
    for attempt in range(5):
        response = session.request(method, url, **kwargs)

        # If rate-limited
        if response.status_code == 429:
            wait = 1.5 + random.random() * 1.5
            print(f"[429] Rate limited. Retrying in {wait:.2f}s...")
            time.sleep(wait)
            continue

        return response

    return None  # Too many failures


# Main function
def fetch_coupon_data(url):
    session = requests.Session()   # NEW session per URL → safest
    
    # Step 1 — Get HTML to extract CSRF token
    resp = safe_request("GET", url, session, headers=HEADERS_GET)
    if not resp or resp.status_code != 200:
        return {"url": url, "error": "GET failed"}

    token = extract_csrf_token(resp.text)
    if not token:
        return {"url": url, "error": "Token missing"}

    payload = {"_token": token, "filter-opt": "all"}

    # Step 2 — POST to retrieve the real content
    resp2 = safe_request(
        "POST",
        url,
        session,
        headers=HEADERS_POST,
        data=json.dumps(payload)
    )

    if not resp2:
        return {"url": url, "error": "POST failed"}
    if resp2.status_code != 200:
        return {"url": url, "error": f"POST returned {resp2.status_code}"}

    return {
        "url": url,
        "content": resp2.text   #Raw HTML Data
    }


# Process a list of URLs safely, sequentially
def process_urls(urls):
    results = []
    for i, url in enumerate(urls, start=1):
        print(f"[{i}/{len(urls)}] Processing: {url}")
        result = fetch_coupon_data(url)
        results.append(result)

        # polite delay (prevents rate limits)
        time.sleep(0.7 + random.random() * 0.4)

    return results


# Example usage
# urls = """https://daralamirat.com.sa/ar/affiliate-coupon-eacc88cf-bce7-4976-bd8b-89bbfd0e1dd9
# https://daralamirat.com.sa/ar/affiliate-coupon-e128c403-fb21-4948-b227-f4f6457bdc70
# https://daralamirat.com.sa/ar/affiliate-coupon-c889288b-9ac0-46c8-9f9f-8fcd9f566e5c
# https://daralamirat.com.sa/ar/affiliate-coupon-5e761ff8-ab3f-4350-8798-8c2d7295ae6d
# https://daralamirat.com.sa/ar/affiliate-coupon-6935cb6f-afcf-4c36-8e1b-084a1d40cd75
# https://daralamirat.com.sa/ar/affiliate-coupon-cee93e46-54a6-4a72-b1b3-407ba43e3ad9
# https://daralamirat.com.sa/ar/affiliate-coupon-12b2675f-e268-4b5a-974e-39be0eb6dfdc
# https://daralamirat.com.sa/ar/affiliate-coupon-bc045833-0e9c-4bec-9a35-269e8ec44122
# https://daralamirat.com.sa/ar/affiliate-coupon-f96db8d5-03de-44e7-bc2e-76c83a272636
# https://daralamirat.com.sa/ar/affiliate-coupon-fdd2e31f-21ba-4d38-bd44-0355b4c6952f
# https://daralamirat.com.sa/ar/affiliate-coupon-dd50bd23-f316-4e55-afef-03eb070a8103
# https://daralamirat.com.sa/ar/affiliate-coupon-fb098f97-4356-45cf-912b-0101d759d02d
# https://daralamirat.com.sa/ar/affiliate-coupon-02e1d2cf-e374-4f7a-8d2d-ddece60c82d0
# https://daralamirat.com.sa/ar/affiliate-coupon-0eaa3602-34e5-499e-8fbf-b9dfab8761ed
# https://daralamirat.com.sa/ar/affiliate-coupon-e33b1b5a-04d6-48db-8f42-586c9d4c1edd
# https://daralamirat.com.sa/ar/affiliate-coupon-bb3f9dc0-4fc9-4522-9d5b-38bbdbf2c67e
# https://daralamirat.com.sa/ar/affiliate-coupon-868f56a4-ecdf-4018-a65d-49f4c7c824a9
# https://daralamirat.com.sa/ar/affiliate-coupon-67596522-f180-49f3-92b8-4722c35de3c2
# https://daralamirat.com.sa/ar/affiliate-coupon-5ce46983-5e27-4e93-9db7-70bc6bcf089a
# https://daralamirat.com.sa/ar/affiliate-coupon-f73395ee-1b6d-481c-9171-570b1487a7d7
# https://daralamirat.com.sa/ar/affiliate-coupon-8b2a7b25-b4b0-470c-9357-6073912ac833
# https://daralamirat.com.sa/ar/affiliate-coupon-2173ebcb-6cc0-4936-a495-9f16905db21b
# https://daralamirat.com.sa/ar/affiliate-coupon-47be7a12-30b0-475d-90d4-cb908e7247d5
# https://daralamirat.com.sa/ar/affiliate-coupon-2076fef1-bb71-420a-b1ad-6865511393cc
# https://daralamirat.com.sa/ar/affiliate-coupon-b22d56ef-6d88-41e8-a8a9-f771474d3306
# https://daralamirat.com.sa/ar/affiliate-coupon-c9233292-72c4-4d94-b779-2c7d2519317d
# https://daralamirat.com.sa/ar/affiliate-coupon-7eccf7ef-90f8-47df-a6b8-06c6fef06171
# https://daralamirat.com.sa/ar/affiliate-coupon-76889a50-0563-4af7-b97f-1bbe789fc862
# https://daralamirat.com.sa/ar/affiliate-coupon-2aa4e707-566e-45f1-9e9b-bf4e86e1e23a
# https://daralamirat.com.sa/ar/affiliate-coupon-d79828b5-a020-4f1d-a163-a238156dfd46
# https://daralamirat.com.sa/ar/affiliate-coupon-6b831c51-cb99-477a-9897-6d31fd4e9471
# https://daralamirat.com.sa/ar/affiliate-coupon-f5d2ba1d-7476-436d-973d-dd0319d40cca
# https://daralamirat.com.sa/ar/affiliate-coupon-1edc403b-479e-4a9c-9258-25895186d1e8
# https://daralamirat.com.sa/ar/affiliate-coupon-d6dc839f-f7a0-465a-b4cd-c51c18a3c33a
# https://daralamirat.com.sa/ar/affiliate-coupon-017bd058-4491-4317-a86f-59b296b44451
# https://daralamirat.com.sa/ar/affiliate-coupon-711217da-057a-4067-9469-2135e276bb43
# https://daralamirat.com.sa/ar/affiliate-coupon-04bb6c01-690f-4233-8db2-306bd4dc4bf1
# https://daralamirat.com.sa/ar/affiliate-coupon-7ff5dd3b-e294-422f-8ff3-8546ed663452
# https://daralamirat.com.sa/ar/affiliate-coupon-8c0333ff-755a-4c77-bb04-a6819c5f6b1c
# https://daralamirat.com.sa/ar/affiliate-coupon-f1439fb4-f9f9-4a8b-b4fe-13e9db6df270
# https://daralamirat.com.sa/ar/affiliate-coupon-bf5e5888-9dbb-4d26-bae1-2120154951d2
# https://daralamirat.com.sa/ar/affiliate-coupon-1355b181-6814-4069-b849-648b5bbc2e8f
# https://daralamirat.com.sa/ar/affiliate-coupon-b69ddcc1-ec56-4f24-b169-6119ac750bb3
# https://daralamirat.com.sa/ar/affiliate-coupon-40f1c4b8-12d1-4195-adcb-89f2d1b53902
# https://daralamirat.com.sa/ar/affiliate-coupon-be55af0c-7ed6-479e-984f-098feadd0cd6
# https://daralamirat.com.sa/ar/affiliate-coupon-08d25468-2df6-4d37-81d2-759a632a1d46
# https://daralamirat.com.sa/ar/affiliate-coupon-bea92bb0-30a0-4333-be17-2c60ec688c4a
# https://daralamirat.com.sa/ar/affiliate-coupon-e5958914-644a-4b6d-8aae-fee358b71958
# https://daralamirat.com.sa/ar/affiliate-coupon-aef18565-c074-41f7-902b-8be941be1a6b
# https://daralamirat.com.sa/ar/affiliate-coupon-4a49b819-f0db-4af1-9d34-9370f27f4c9c
# https://daralamirat.com.sa/ar/affiliate-coupon-3924f8cc-27dc-4df9-a1f4-29b6313309e0
# https://daralamirat.com.sa/ar/affiliate-coupon-06d99c30-acb9-4e14-98c7-f1f59682a401
# https://daralamirat.com.sa/ar/affiliate-coupon-946232c5-2d5e-4a20-8af3-5bd455a566a3
# https://daralamirat.com.sa/ar/affiliate-coupon-4a87a1a0-cc44-4f0d-b3d3-283fe8cbdf8d
# https://daralamirat.com.sa/ar/affiliate-coupon-db73bb23-6c6f-4d48-91cd-bd6de2f49b23
# https://daralamirat.com.sa/ar/affiliate-coupon-0914cb1f-26d5-461f-a94c-dc869a9d6402
# https://daralamirat.com.sa/ar/affiliate-coupon-1db9a0e1-6cdc-4bf6-9364-4e1fb9635124
# https://daralamirat.com.sa/ar/affiliate-coupon-18a7f490-fabc-4131-95ef-34531360dab1
# https://daralamirat.com.sa/ar/affiliate-coupon-2a843aee-2c9b-41c2-b1cd-66e960d037ef
# https://daralamirat.com.sa/ar/affiliate-coupon-2dd2fefa-c030-4049-a355-39f5be13d4db
# https://daralamirat.com.sa/ar/affiliate-coupon-84c6f789-4651-46af-9959-41e3e517ed6f
# https://daralamirat.com.sa/ar/affiliate-coupon-e450c9de-3484-421c-b209-98b96ae45c40
# https://daralamirat.com.sa/ar/affiliate-coupon-dcb2bf3c-bae1-40ab-8f80-13d416829dd0
# https://daralamirat.com.sa/ar/affiliate-coupon-14a0c6d7-dd12-4c48-8ddf-3fcb1090300a
# https://daralamirat.com.sa/ar/affiliate-coupon-946d7cba-c9e3-45c1-8fd5-0bb04de144e4
# https://daralamirat.com.sa/ar/affiliate-coupon-500ee31d-e6bb-4747-b61c-4b85e4a0ad7e
# https://daralamirat.com.sa/ar/affiliate-coupon-66ab0b2a-e05e-4ebe-9ace-35e1f787b16e
# https://daralamirat.com.sa/ar/affiliate-coupon-747b3d9a-7af7-4e1f-a1f3-fc5d75823d37
# https://daralamirat.com.sa/ar/affiliate-coupon-a3fd7e98-9efd-4eea-9d5f-ff47a3e1d0c4
# https://daralamirat.com.sa/ar/affiliate-coupon-237c66d8-3954-48a1-8934-deed6f975c8e
# https://daralamirat.com.sa/ar/affiliate-coupon-ec39bbfb-8034-421d-a8f6-802a093742ae
# https://daralamirat.com.sa/ar/affiliate-coupon-74503326-eab2-45a4-9c1a-51f066effa26
# https://daralamirat.com.sa/ar/affiliate-coupon-3b3491fd-2dac-4195-9b7d-6795f17d7b5b
# https://daralamirat.com.sa/ar/affiliate-coupon-53f683f4-4f21-4c53-973e-2674af5e9ec3
# https://daralamirat.com.sa/ar/affiliate-coupon-b3709377-e310-4fb7-8390-ee5c98d072fa
# https://daralamirat.com.sa/ar/affiliate-coupon-1e899963-5345-46a4-a104-fa06b9253fe6
# https://daralamirat.com.sa/ar/affiliate-coupon-c49975cd-0e60-4878-8d75-486563e716fa
# https://daralamirat.com.sa/ar/affiliate-coupon-063cf43b-7bbe-4334-9ee7-b0a8a9008886
# https://daralamirat.com.sa/ar/affiliate-coupon-41cf3abd-405d-41b7-9b45-4808286b6a35
# https://daralamirat.com.sa/ar/affiliate-coupon-904fd7d1-de83-480b-ba6c-b6e6e95ee725
# https://daralamirat.com.sa/ar/affiliate-coupon-67859ce2-53dc-4fc6-a6dc-afe7be17a9c6
# https://daralamirat.com.sa/ar/affiliate-coupon-0a34d067-ba96-48c9-86af-5a14151d0eb3
# https://daralamirat.com.sa/ar/affiliate-coupon-83f70715-1228-4255-a8d4-5a46c09a290e
# https://daralamirat.com.sa/ar/affiliate-coupon-3c4cc60b-38a9-4d2d-93c6-3635f2691a57
# https://daralamirat.com.sa/ar/affiliate-coupon-cf67f831-0f54-4678-b837-888ba32b2804
# https://daralamirat.com.sa/ar/affiliate-coupon-2dcdf22f-78e4-42b1-97a3-17b2f3e7e0aa
# https://daralamirat.com.sa/ar/affiliate-coupon-fdbe1df1-bb61-4216-bbc3-72130b76b7a2
# https://daralamirat.com.sa/ar/affiliate-coupon-39c8affe-db3a-4c47-9144-f9f2a9baa9cf
# https://daralamirat.com.sa/ar/affiliate-coupon-38ae3e6a-3606-4e95-b5d8-c104adb2e87b
# https://daralamirat.com.sa/ar/affiliate-coupon-ec1e9ef3-91c1-4e66-8676-20eb8d5cd2f2
# https://daralamirat.com.sa/ar/affiliate-coupon-a876006a-0547-42b2-afe3-33ae43cd511e
# https://daralamirat.com.sa/ar/affiliate-coupon-bef6a3fb-1b72-44ed-814b-08e1136c7551
# https://daralamirat.com.sa/ar/affiliate-coupon-995dd4dc-ea81-4386-a992-5bc5d4ce1fe9
# https://daralamirat.com.sa/ar/affiliate-coupon-16148e3d-267a-4ff4-a89b-6d9f97931081
# https://daralamirat.com.sa/ar/affiliate-coupon-255a9367-220e-4b87-9e9c-0d96398d5a73
# https://daralamirat.com.sa/ar/affiliate-coupon-9c7121e3-3f67-4e41-8d50-0464ebc997ed
# https://daralamirat.com.sa/ar/affiliate-coupon-29de8ad5-e604-4d00-94ce-46c7861b969a
# https://daralamirat.com.sa/ar/affiliate-coupon-f7ca7b57-ea1d-4f6c-b9a4-eb2a4cc7148b
# https://daralamirat.com.sa/ar/affiliate-coupon-39bfc5d8-a474-49fd-95d1-e9fb5e04e4cb
# https://daralamirat.com.sa/ar/affiliate-coupon-1a4fbb48-2d7b-48b3-9e9d-3ea207038484
# https://daralamirat.com.sa/ar/affiliate-coupon-950e64c3-6bb9-4f02-b772-0b2e82b07627
# https://daralamirat.com.sa/ar/affiliate-coupon-72aa2100-48c9-4913-8484-df66ff0a43c9
# https://daralamirat.com.sa/ar/affiliate-coupon-04fc2a64-9cfa-4ac8-92f6-8241538064a1
# https://daralamirat.com.sa/ar/affiliate-coupon-66c0bbe4-76f5-436d-99fc-7c6d4d063f10
# https://daralamirat.com.sa/ar/affiliate-coupon-671a1c14-ffc9-494d-88f0-ba4d16eeeb9c
# https://daralamirat.com.sa/ar/affiliate-coupon-34f5eefc-abea-4fda-b040-3b7d567a3148
# https://daralamirat.com.sa/ar/affiliate-coupon-0d80803d-39ca-4b66-b490-16222aea312b
# https://daralamirat.com.sa/ar/affiliate-coupon-1aa0ae0e-ba9b-4d11-9935-a7e7ad8204dd
# https://daralamirat.com.sa/ar/affiliate-coupon-f926799a-11de-4436-b16b-65272f3d273a
# https://daralamirat.com.sa/ar/affiliate-coupon-47f1914e-73f7-42c1-9fbd-5cff7375e6e1
# https://daralamirat.com.sa/ar/affiliate-coupon-ab0d6bd5-73df-4c3b-a18b-7a4c052d0924
# https://daralamirat.com.sa/ar/affiliate-coupon-34f23416-58c9-4749-882a-d89758433a68
# https://daralamirat.com.sa/ar/affiliate-coupon-a417eb90-f682-460e-9dda-1c29c940b1f3
# https://daralamirat.com.sa/ar/affiliate-coupon-6883d212-e05b-4c65-b9da-031cdd0a1e89
# https://daralamirat.com.sa/ar/affiliate-coupon-dbfc01b9-f0bd-4d6f-a987-4a5827b70cc7
# https://daralamirat.com.sa/ar/affiliate-coupon-29f61f7b-95c4-4858-a515-d16bbc3d08ff
# https://daralamirat.com.sa/ar/affiliate-coupon-d026d214-da70-4418-8df2-1d79d2ae3256
# https://daralamirat.com.sa/ar/affiliate-coupon-85a924a3-7919-4609-b3db-f5e0c675502b
# https://daralamirat.com.sa/ar/affiliate-coupon-9b9e8309-2e32-41b6-8e1e-c87924a4f5f0
# https://daralamirat.com.sa/ar/affiliate-coupon-951b6bdd-dad0-4c20-83b1-9616648c5e3b
# https://daralamirat.com.sa/ar/affiliate-coupon-d71d80e2-aac8-4dfc-96c0-1f7c03e26409
# https://daralamirat.com.sa/ar/affiliate-coupon-83e3aa54-58b7-4202-b26b-ec42ce6152d3
# https://daralamirat.com.sa/ar/affiliate-coupon-46a833c0-62ac-45c0-bf93-a45328b95ab6"""

# urls = urls.split('\n')

aff_sheet = load_affiliate_mapping_from_xlsx(affiliate_xlsx_path, SHEET_NAME)

urls = aff_sheet['URL'].to_list()

redundancy_df = pd.read_csv(find_latest_csv_by_prefix(input_dir, REDUNDANCY_CSV))

redundancy_df = pd.DataFrame({
    'Code': redundancy_df['Code'].apply(str),
    'Sale Amount': redundancy_df['Sale Amount'],
    'Revenue': redundancy_df['Revenue'],
    'ID': redundancy_df['ID'].apply(str).apply(lambda x: x.replace('.0', ''))
})

data = process_urls(urls)

for item in data:
    print(item["url"], "->", "OK" if "content" in item else item["error"])

def get_data(url, content) -> pd.DataFrame:
    values_raw = re.findall(r"<span[^>]*>\s*([0-9]+,*[0-9]*(?:\.[0-9]+)?)", ast.literal_eval(content))

    if values_raw:
        sales = []
        ids = []
        revs = []

        for i in range(0,len(values_raw),2):
            ids.append(values_raw[i])
            revs.append(float(values_raw[i+1].replace(',','')))
            
        sales = list(map(lambda x: x * 50, revs))

        return pd.DataFrame({
        'Sale Amount': sales,
        'Revenue': revs,
        'Order ID': ids,
        'URL': url
        })

refined_data = list(map(lambda item: get_data(item['url'], item['content']), data))

refined = pd.DataFrame({})

for d_frame in refined_data:
    refined = pd.concat([refined, d_frame], axis=0)

refined.reset_index(inplace=True, drop=True)

del_row = redundancy_df.loc[:,'ID']

del_row = del_row.apply(str)
del_row = del_row.apply(str.strip)

refined['Order ID'] = refined['Order ID'].apply(str)

refined = refined[~refined['Order ID'].isin(del_row)]

refined.reset_index(inplace=True, drop=True)

# aff_sheet = load_affiliate_mapping_from_xlsx(affiliate_xlsx_path, SHEET_NAME)

refined = refined.merge(aff_sheet, "left", "URL")

# refined['Revenue'] = refined['Revenue'] / 3.75
# refined['Sale Amount'] = refined['Sale Amount'] / 3.75

final_df = pd.DataFrame({
    'offer': OFFER_ID,
    'affiliate_id': refined['affiliate_ID'],
    'date': pd.to_datetime(f'{month}/{day}/{year}'),
    'status': 'pending',
    'payout': (refined['Revenue'] * refined['pct_new'])/3.75,
    'revenue': refined['Revenue']/3.75,
    'sale amount': refined['Sale Amount']/3.75,
    'coupon': refined['code_norm'],
    'geo': refined['geo']
})

print(final_df)

final_df.to_csv(output_file, index = False)

redundancy_df = redundancy_df.iloc[:,0:4]

refined.columns = ['Sale Amount', 'Revenue', 'ID', 'URL', 'Code',
       'affiliate_ID', 'type_norm', 'pct_new', 'pct_old', 'fixed_new',
       'fixed_old', 'geo']

redundancy_df = pd.concat([redundancy_df, refined[['Code', 'Sale Amount', 'Revenue', 'ID']]], axis = 0)

# print(redundancy_df)

redundancy_df.to_csv(redun_file, index=False)

# print(aff_sheet)