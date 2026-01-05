import requests
from bs4 import BeautifulSoup
import json
import time
import random
import re
import pandas as pd
import ast
import os
from playwright.sync_api import sync_playwright

OFFER_ID = 1363

day = input('DAY: ')
month = input('MONTH: ')
year = input('YEAR: ')

SHEET_NAME = "ALHABIB Bedding"
AFFILIATE_XLSX = "Offers Coupons.xlsx"
DEFAULT_PCT_IF_MISSING = 0.0
DEFAULT_AFF_ID_IF_MISSING = '1'

OUTPUT_CSV = f"habib_{month}_{day}_{year} Only.csv"
REDUNDANCY_CSV = f"habib"
INPUT_NEW = f"habib_{month}_{day}_{year}"

script_dir = os.path.dirname(os.path.abspath(__file__))
input_dir  = os.path.join(script_dir, '..', 'input data')
output_dir = os.path.join(script_dir, '..', 'output data')
os.makedirs(output_dir, exist_ok=True)

affiliate_xlsx_path = os.path.join(input_dir, AFFILIATE_XLSX)
redundancy_csv_path = os.path.join(input_dir, REDUNDANCY_CSV)
output_file = os.path.join(output_dir, OUTPUT_CSV)
redun_file = os.path.join(input_dir, INPUT_NEW)

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
        'URL': df_sheet['link'],
        'type': df_sheet['type']
    }).dropna(subset=['code_norm'])

    return out.drop_duplicates(subset=['code_norm'], keep='last')


aff_sheet = load_affiliate_mapping_from_xlsx(affiliate_xlsx_path, SHEET_NAME)

urls = aff_sheet['URL'].to_list()

results = []

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True, slow_mo=50)
    context = browser.new_context(locale="ar-SA")
    page = context.new_page()

    for i, url in enumerate(urls, start=1):
        print(f"[{i}/{len(urls)}] Visiting {url}")

        try:
            # 👇 REGISTER RESPONSE LISTENER FIRST
            with page.expect_response(
                lambda r: (
                    r.request.method == "POST"
                    and "affiliate-coupon" in r.url
                    and r.request.resource_type == "xhr"
                    and r.status == 200
                ),
                timeout=15000
            ) as resp_info:
                
                page.goto(url, wait_until="domcontentloaded")

            response = resp_info.value
            data = response.text()

            results.append({
                "url": url,
                "data": data
            })
            print("  ✔ XHR captured")

        except Exception as e:
            results.append({
                "url": url,
                "error": str(e)
            })
            print("  ✖ Failed")

        # polite cooldown
        time.sleep(6)

    browser.close()


