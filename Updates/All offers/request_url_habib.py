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

day = input('DAY: ')
month = input('MONTH: ')
year = input('YEAR: ')

SHEET_NAME = "ALHABIB Bedding"
AFFILIATE_XLSX = "Offers Coupons.xlsx"
DEFAULT_PCT_IF_MISSING = 0.0

OUTPUT_CSV = f"habib_{month}_{day}_{year}.csv"
REDUNDANCY_CSV = "Al-Habib"

script_dir = os.path.dirname(os.path.abspath(__file__))
input_dir  = os.path.join(script_dir, '..', 'input data')
output_dir = os.path.join(script_dir, '..', 'output data')
os.makedirs(output_dir, exist_ok=True)

affiliate_xlsx_path = os.path.join(input_dir, AFFILIATE_XLSX)
# redundancy_csv_path = os.path.join(input_dir, REDUNDANCY_CSV)
output_file = os.path.join(output_dir, OUTPUT_CSV)

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
        'affiliate_ID': df_sheet[aff_col].fillna('').astype(str).str.strip(),
        'type_norm': type_norm,
        'pct_new': pd.to_numeric(pct_new, errors='coerce').fillna(DEFAULT_PCT_IF_MISSING),
        'pct_old': pd.to_numeric(pct_old, errors='coerce').fillna(DEFAULT_PCT_IF_MISSING),
        'fixed_new': pd.to_numeric(fixed_new, errors='coerce'),
        'fixed_old': pd.to_numeric(fixed_old, errors='coerce'),
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
        "content": resp2.text   # Real data (HTML fragment)
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
urls = """https://alhabibshop.com/ar/affiliate-coupon-5e6472e7-d34c-4815-9865-12b6f29e40ed
https://alhabibshop.com/ar/affiliate-coupon-80516241-f2cd-4874-994f-dcc91a7ecbed
https://alhabibshop.com/ar/affiliate-coupon-5434c105-5ec0-40fe-8e22-50986fbf9d21
https://alhabibshop.com/ar/affiliate-coupon-1d65d3c4-16fa-493d-aff6-791a0b55a9fa
https://alhabibshop.com/ar/affiliate-coupon-ac928244-2173-4029-a121-c0545763e9ff
https://alhabibshop.com/ar/affiliate-coupon-5ba98561-b5b0-468b-b0b5-2719bb29040e
https://alhabibshop.com/ar/affiliate-coupon-f5976552-5592-4bfd-9a44-4738111ecede
https://alhabibshop.com/ar/affiliate-coupon-fcfa4ca6-17e0-4955-b565-e8e393b29c56
https://alhabibshop.com/ar/affiliate-coupon-516fa621-2473-430c-bcfa-5d10e27b6e7b
https://alhabibshop.com/ar/affiliate-coupon-c7469b80-2983-4fc3-9fdb-f86638763a78
https://alhabibshop.com/ar/affiliate-coupon-1c341cd0-ad03-443c-b0e3-f40f4d39802c
https://alhabibshop.com/ar/affiliate-coupon-0c83adf0-b465-4c5e-813c-6ed963ecefbc
https://alhabibshop.com/ar/affiliate-coupon-8cc8c571-196f-493c-b42f-022f40748b79
https://alhabibshop.com/ar/affiliate-coupon-4abea0b8-fd7f-4945-bca9-f746ec3f498a
https://alhabibshop.com/ar/affiliate-coupon-cdf76216-cb49-49a2-82bd-ae46e4417677
https://alhabibshop.com/ar/affiliate-coupon-68494034-108f-4096-a4a0-333ac1f14eee
https://alhabibshop.com/ar/affiliate-coupon-40ff7b17-7d02-4718-9d35-e0bd64f3f956
https://alhabibshop.com/ar/affiliate-coupon-87989acb-49ae-42d6-9682-1cf5482325db
https://alhabibshop.com/ar/affiliate-coupon-aee7cf4e-c875-4361-ac85-e20fd9e37c6e
https://alhabibshop.com/ar/affiliate-coupon-b8e21b0a-13cc-4940-bbb2-9a2c74fd7d50
https://alhabibshop.com/ar/affiliate-coupon-0d3dbf79-18b1-433b-9774-3fbf037b8828
https://alhabibshop.com/ar/affiliate-coupon-3ee8ab62-9d4a-40f5-8a85-b91c15dfe771
https://alhabibshop.com/ar/affiliate-coupon-88079dc0-430d-421a-96a3-556ce8da67fd
https://alhabibshop.com/ar/affiliate-coupon-4e374fcd-7715-481f-ac6f-c0bd88f28ea1
https://alhabibshop.com/ar/affiliate-coupon-df94c687-d3c3-4019-912e-c542078b6feb
https://alhabibshop.com/ar/affiliate-coupon-e9665581-e11f-48f8-8ddd-432ebb4052c2
https://alhabibshop.com/ar/affiliate-coupon-74509cda-0464-43bf-94c4-44adf118c593
https://alhabibshop.com/ar/affiliate-coupon-5c9a588d-6168-48ff-9c5e-0e3b485b5d7c
https://alhabibshop.com/ar/affiliate-coupon-f556ca1a-49df-44ee-a14b-00872e08dccc
https://alhabibshop.com/ar/affiliate-coupon-8061de69-464f-446b-a3f7-84213c57710a
https://alhabibshop.com/ar/affiliate-coupon-59754428-0255-4110-94ff-d516de52c3d9
https://alhabibshop.com/ar/affiliate-coupon-9fc434ca-6bdf-43ba-8966-45fa1f38a486
https://alhabibshop.com/ar/affiliate-coupon-8d1d934e-69fa-4bc1-a85d-8515b5c13441
https://alhabibshop.com/ar/affiliate-coupon-259446db-50b5-457b-894b-72a84cddcd4f
https://alhabibshop.com/ar/affiliate-coupon-4899071e-9911-4667-96c3-466f62b56b59
https://alhabibshop.com/ar/affiliate-coupon-60c54efb-04e9-4bfa-b397-ef96685b4627
https://alhabibshop.com/ar/affiliate-coupon-e99fd31b-68f6-42b6-af7b-93004c66f46e
https://alhabibshop.com/ar/affiliate-coupon-0fca0f20-c921-411b-84a3-dbe2d8c5d506
https://alhabibshop.com/ar/affiliate-coupon-1276b546-53a1-46b3-8dd5-21ec39342110
https://alhabibshop.com/ar/affiliate-coupon-c4042808-a4ca-472d-8b09-670dca3d6081
https://alhabibshop.com/ar/affiliate-coupon-01c62398-845a-42ae-a76b-58b803e8dbd0
https://alhabibshop.com/ar/affiliate-coupon-533e3f32-200a-4d2e-8749-add928b3bb07
https://alhabibshop.com/ar/affiliate-coupon-3e08e789-e550-4f7e-a26f-8c475a00cdcf
https://alhabibshop.com/ar/affiliate-coupon-9fa24a34-57b0-436e-9731-c103db79b9a9
https://alhabibshop.com/ar/affiliate-coupon-6eee8095-0924-4012-899f-acc666f743b7
https://alhabibshop.com/ar/affiliate-coupon-55650b73-cdf6-43bc-942e-5135d85bc0a6
https://alhabibshop.com/ar/affiliate-coupon-a2fa99b8-54ab-4e28-92c7-0a33f1fa563a
https://alhabibshop.com/ar/affiliate-coupon-308495eb-0276-4f75-b425-272a3d5cb602
https://alhabibshop.com/ar/affiliate-coupon-e75cf2e1-736e-48c6-82b5-1a586754dacf
https://alhabibshop.com/ar/affiliate-coupon-162b84a2-2a6d-4bbc-8b98-6db00067326c
https://alhabibshop.com/ar/affiliate-coupon-87b9e3f9-74f0-4020-aaf8-15557c08e101
https://alhabibshop.com/ar/affiliate-coupon-c447ceb3-2a47-4ded-9a81-e569e99f4c08
https://alhabibshop.com/ar/affiliate-coupon-b8645b49-8b6b-479c-b34c-7207e8da0b23
https://alhabibshop.com/ar/affiliate-coupon-291fe293-2b7c-4a17-90da-7419d6d7a73f
https://alhabibshop.com/ar/affiliate-coupon-8756a022-63cd-4acf-ae52-3d2d8a0439b1
https://alhabibshop.com/ar/affiliate-coupon-50bdeeca-2434-4f8c-8565-0d78d9a28a92
https://alhabibshop.com/ar/affiliate-coupon-60bede18-b3a3-40e0-bf9c-1a1a733dea9b
https://alhabibshop.com/ar/affiliate-coupon-721793bf-581d-4672-bf29-5bef2d3954a2
https://alhabibshop.com/ar/affiliate-coupon-ceda804e-08ab-4310-8657-09c26c76169e
https://alhabibshop.com/ar/affiliate-coupon-be7e9bc3-215e-48f1-a5bc-eef6ce5c5e14
https://alhabibshop.com/ar/affiliate-coupon-5f82801b-9018-49f7-8f89-e55c9b22c333
https://alhabibshop.com/ar/affiliate-coupon-98a16337-d11e-4c93-8435-46281724b28a
https://alhabibshop.com/ar/affiliate-coupon-c0b87164-0654-4700-abd6-aa200d141cf3
https://alhabibshop.com/ar/affiliate-coupon-3cb2e21f-be0f-4c7f-bdbe-fd801048133d
https://alhabibshop.com/ar/affiliate-coupon-9787f8e9-8e48-496f-902e-6d1e0905655b
https://alhabibshop.com/ar/affiliate-coupon-cf65fb54-7d90-4023-bf19-07d99f454e9d
https://alhabibshop.com/ar/affiliate-coupon-677985d5-4c47-4d67-af57-e10f2b600939
https://alhabibshop.com/ar/affiliate-coupon-bdb9a13e-8b68-438f-8a50-19a24bd244d0
https://alhabibshop.com/ar/affiliate-coupon-eb0573d5-5a2b-484c-b905-0c87e656043d
https://alhabibshop.com/ar/affiliate-coupon-ae0e598c-36ba-4e0e-a196-f8d13d9004fc
https://alhabibshop.com/ar/affiliate-coupon-5f08e411-3bab-49b4-91b0-8b693e5275e3
https://alhabibshop.com/ar/affiliate-coupon-4abcd7f1-c96e-49c0-9d3d-9957e761d71b
https://alhabibshop.com/ar/affiliate-coupon-2b2cfff0-720c-438d-91d2-612d05469a9a
https://alhabibshop.com/ar/affiliate-coupon-587f87fb-fa8d-495f-8caa-a94ce0d2cf88
https://alhabibshop.com/ar/affiliate-coupon-5ae6eb7f-99f5-42c3-a425-16880c1248ba
https://alhabibshop.com/ar/affiliate-coupon-8caef0d6-066d-4ceb-9be7-fc4f1c089fae
https://alhabibshop.com/ar/affiliate-coupon-70b9c6f2-707b-4ccf-8845-75ad950b6c22
https://alhabibshop.com/ar/affiliate-coupon-c985daef-84b0-46a0-9fa8-f3058e90d953
https://alhabibshop.com/ar/affiliate-coupon-4167fc5a-bf8e-40a9-8c58-52a111b7f18b
https://alhabibshop.com/ar/affiliate-coupon-e028d79b-21ff-405c-88eb-65d05779aba4
https://alhabibshop.com/ar/affiliate-coupon-fbf096a8-0bdd-478f-9004-9a15272135be
https://alhabibshop.com/ar/affiliate-coupon-59552dcc-9e88-4f97-a037-0fdc756588df
https://alhabibshop.com/ar/affiliate-coupon-3957c22c-8484-4291-8f58-cb7239637ff2
https://alhabibshop.com/ar/affiliate-coupon-29d5488f-4ed7-4d70-a2b8-a1d9414b5399
https://alhabibshop.com/ar/affiliate-coupon-bec57abc-90f6-492c-a224-22bc4a14ced5
https://alhabibshop.com/ar/affiliate-coupon-d4743fca-a990-43dd-b6b3-a0aa64f2fcc6
https://alhabibshop.com/ar/affiliate-coupon-d3a8d916-14cf-4ae4-af6f-9b8cab4fdb2f
https://alhabibshop.com/ar/affiliate-coupon-6c901abf-af88-4508-a98d-a2b6a36361a9
https://alhabibshop.com/ar/affiliate-coupon-ac953491-221a-48c2-8aaf-909d3356380a
https://alhabibshop.com/ar/affiliate-coupon-f4fb641d-65c1-4df4-93f6-e09b930ad247
https://alhabibshop.com/ar/affiliate-coupon-7edc99b5-9468-433b-b06b-a4b85af34063
https://alhabibshop.com/ar/affiliate-coupon-9d89ad08-1f1e-4d20-bb5d-7d8cab2affd6
https://alhabibshop.com/ar/affiliate-coupon-11446227-7a18-496f-abc9-02421b652968
https://alhabibshop.com/ar/affiliate-coupon-2d429687-7a88-47d4-8a7f-73a583ce63fd
https://alhabibshop.com/ar/affiliate-coupon-5608deb7-8e3d-451f-aebc-6017e57b1d53
https://alhabibshop.com/ar/affiliate-coupon-a82a3a72-3da2-4795-bfec-1dc19e3762b9
https://alhabibshop.com/ar/affiliate-coupon-5c7c56f4-328e-4498-880f-89df0d7e3de0
https://alhabibshop.com/ar/affiliate-coupon-838f5b4e-de46-434d-b7ae-b036897aaad0
https://alhabibshop.com/ar/affiliate-coupon-1b7a501e-1d89-4786-b835-47c73d698295
https://alhabibshop.com/ar/affiliate-coupon-8b07936f-548a-4fd0-a608-d01ddafab94c
https://alhabibshop.com/ar/affiliate-coupon-b3a30fa4-729c-4f96-9058-a49c25d806ec
https://alhabibshop.com/ar/affiliate-coupon-4313f2bf-bf34-4c1e-8772-5d8671145fe5
https://alhabibshop.com/ar/affiliate-coupon-725a1f8f-b23a-49ac-a7e0-33cb4e40f234
https://alhabibshop.com/ar/affiliate-coupon-aa89ae0f-88aa-4a64-98a7-4a0657061051
https://alhabibshop.com/ar/affiliate-coupon-c2b40f89-33ab-4f4b-86d1-8f7e4d014cb3
https://alhabibshop.com/ar/affiliate-coupon-752549e2-bf44-4ec7-ab8c-27838dddcfbb
https://alhabibshop.com/ar/affiliate-coupon-bd8e9c33-fbab-4b03-b8d4-022a77972017
https://alhabibshop.com/ar/affiliate-coupon-99163285-ec9c-4111-a4e1-5bfe47f86818
https://alhabibshop.com/ar/affiliate-coupon-7f3acc0e-78ba-439e-ab24-6a6090263fe7
https://alhabibshop.com/ar/affiliate-coupon-515c937f-e22b-4d2d-986f-2dfd06cf0ad7
https://alhabibshop.com/ar/affiliate-coupon-540d77ec-a9d9-4275-b441-4933c8776339
https://alhabibshop.com/ar/affiliate-coupon-038e7b66-5be6-4736-860c-6b7227f2a8a8
https://alhabibshop.com/ar/affiliate-coupon-e15f55dd-a8a1-434c-a6cb-65b524d410b5
https://alhabibshop.com/ar/affiliate-coupon-bc1043bc-5063-4624-a109-6135668fc19d
https://alhabibshop.com/ar/affiliate-coupon-e813e2f8-42d5-40dd-8a6e-c77a8eaf2d78
https://alhabibshop.com/ar/affiliate-coupon-a4a86f2f-30b6-4681-9f41-cca01eb1c046
https://alhabibshop.com/ar/affiliate-coupon-c91c77e4-54be-440e-bf07-840d0818cc29
https://alhabibshop.com/ar/affiliate-coupon-f0f9ed6e-e82f-451b-b163-3502dcafa934
https://alhabibshop.com/ar/affiliate-coupon-63d33d3a-2b05-438b-912e-132031ed75b1
https://alhabibshop.com/ar/affiliate-coupon-6bc2cd1f-8a4c-4a89-a94b-1e1600371190
https://alhabibshop.com/ar/affiliate-coupon-4e6c34c3-08c6-4897-8706-39a11f3b2217
https://alhabibshop.com/ar/affiliate-coupon-460f7d37-3278-4327-93b3-2e130165bf4a
https://alhabibshop.com/ar/affiliate-coupon-0fc9762f-189b-410c-a592-a11d6f820fdb"""

urls = urls.split('\n')

aff_sheet = load_affiliate_mapping_from_xlsx(affiliate_xlsx_path, SHEET_NAME)

redundancy_df = pd.read_csv(find_latest_csv_by_prefix(input_dir, REDUNDANCY_CSV))

data = process_urls(urls)

for item in data:
    print(item["url"], "->", "OK" if "content" in item else item["error"])

def get_data(url, content) -> pd.DataFrame:
    values_raw = re.findall(r"<span[^>]*>\s*([0-9]+,*[0-9]*(?:\.[0-9]+)?)", ast.literal_eval(content))

    if values_raw:
        sales = []
        ids = []
        revs = []

        for i in range(0,len(values_raw),3):
            ids.append(values_raw[i])
            sales.append(float(values_raw[i+2].replace(',','')))
            
        revs = list(map(lambda x: x * 0.05, sales))

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

del_row = redundancy_df.loc[:,'Order ID']

del_row = del_row.apply(str)
del_row = del_row.apply(str.strip)

refined['Order ID'] = refined['Order ID'].apply(str)

refined = refined[~refined['Order ID'].isin(del_row)]

refined.reset_index(inplace=True, drop=True)

refined.to_csv(output_file)