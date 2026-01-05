import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import re

OFFER_ID = 1360
DEFAULT_GEO = 'no-geo'
STATUS_DEFAULT = "pending"
DEFAULT_PCT_IF_MISSING = 0.0
FALLBACK_AFFILIATE_ID = "1"
CURRENCY_DIVISOR = 3.67
DAYS_BACK = 10

REPORT_PREFIX = "zzzzzzz"
REPORT_SHEET = "Export1"
AFFILIATE_XLSX = "Offers Coupons.xlsx"
AFFILIATE_SHEET = "SquatWolf-CPS"
OUTPUT_CSV = "squatwolf.csv"

script_dir = os.path.dirname(os.path.abspath(__file__))
input_dir = os.path.join(script_dir, '..', 'input data')
output_dir = os.path.join(script_dir, '..', 'output data')
os.makedirs(output_dir, exist_ok=True)

affiliate_xlsx_path = os.path.join(input_dir, AFFILIATE_XLSX)
output_file = os.path.join(output_dir, OUTPUT_CSV)

months = {
    'January': '1',
    'February': '2',
    'March': '3',
    'April': '4',
    'May': '5',
    'June': '6',
    'July': '7',
    'August': '8',
    'September': '9',
    'October': '10',
    'November': '11',
    'December': '12'
}

countries = {
    'United Kingdom': 'uk',
    'Australia': 'aus',
    'Saudi Arabia': 'ksa',
    'Kuwait': 'kwt',
    'United Arab Emirates': 'uae',
    'Netherlands': 'nl',
    'France': 'fr',
    'Germany': 'ger',
    'Qatar': 'qtr'
}

def _norm_name(s: str) -> str:
    """Lowercase + collapse spaces for robust comparisons."""
    return re.sub(r"\s+", " ", str(s).strip()).lower()

def normalize_coupon(x: str) -> str:
    """Uppercase, trim, first token if multiple codes separated by ; , or whitespace (handles NBSP)."""
    if pd.isna(x):
        return ""
    s = str(x).replace("\u00A0", " ").strip().upper()  # NBSP -> space
    parts = re.split(r"[;,\s]+", s)
    return parts[0] if parts else s

def load_affiliate_mapping_from_xlsx(xlsx_path: str, sheet_name: str) -> pd.DataFrame:
    """Return mapping with columns code_norm, affiliate_ID, type_norm, pct_new, pct_old, fixed_new, fixed_old."""
    df_sheet = pd.read_excel(xlsx_path, sheet_name=sheet_name, dtype=str)
    
    coupon = df_sheet['Code']
    id = df_sheet['Id']
    payout_perc = df_sheet['new customer payout']
    types = df_sheet['type']

    return pd.DataFrame({
        'code_norm': coupon.apply(normalize_coupon).apply(str),
        'affiliate_id': id.fillna(FALLBACK_AFFILIATE_ID),
        'payout_perc': payout_perc.apply(float),
        'types': types.apply(str)
    }).dropna()


def find_latest_csv_by_prefix(directory: str, prefix: str) -> str:
    """
    Find the newest CSV whose base filename starts with `prefix`
    (case/space-insensitive). Falls back to modified time.
    """
    prefix_n = _norm_name(prefix)
    candidates = []
    for fname in os.listdir(directory):
        if not fname.lower().endswith(".xlsx"):
            continue
        base = os.path.splitext(fname)[0]
        if _norm_name(base).startswith(prefix_n):
            candidates.append(os.path.join(directory, fname))
    if not candidates:
        avail = [f for f in os.listdir(directory) if f.lower().endswith(".xslx")]
        raise FileNotFoundError(
            f"No xlsx file starting with '{prefix}' in: {directory}\nAvailable Excels: {avail}"
        )
    return max(candidates, key=os.path.getmtime)

df = find_latest_csv_by_prefix(input_dir, REPORT_PREFIX)
df = pd.read_excel(df)

# print(df.columns)

df = df.iloc[:,0:5]
# print(df)

actual_cols = ['Order Date', 'Coupon Code', 'Country Name', 'Sales', 'CustomerType']
df_actual = pd.DataFrame(columns=actual_cols)

flag = False

for i in range(len(df)):
    if list(df.iloc[i, :]) == actual_cols:
        df.columns = actual_cols 
        flag = True
        continue

    if flag:
        if df.iloc[i, :].isna().all():
            break
        df_actual = pd.concat([df_actual, df.iloc[[i], :]], axis=0)

df_actual.reset_index(drop=True, inplace=True)

date = df_actual['Order Date'].apply(lambda x: str(x).split(" "))

day = date.apply(lambda x: x[1])
month = date.apply(lambda x: x[2]).apply(lambda x: months[x])
year = date.apply(lambda x: x[3])

date = pd.DataFrame({
    'Day': day,
    'Month': month,
    'Year': year
})

date['Date'] = pd.Series( list(map(lambda x: str(x), range(date.__len__()))) )

i = 0

for row in date.iterrows():
    # print(row)
    day = row[1]['Day']
    month = row[1]['Month']
    year = row[1]['Year']

    date.loc[i,'Date'] = f"{month}/{day}/{year}"
    i+=1

df_actual['Order Date'] = date['Date']
del date

# print(df_actual)

aff_sheet = load_affiliate_mapping_from_xlsx(affiliate_xlsx_path, AFFILIATE_SHEET)

df_actual = pd.merge(df_actual, aff_sheet, "left", left_on="Coupon Code", right_on="code_norm")

df_actual['Country Name'] = df_actual['Country Name'].apply(lambda x: countries[x])

sales_sum = np.sum(df_actual['Sales'])
df_actual['Sales'] = df_actual['Sales'] / CURRENCY_DIVISOR

def choose_revenue_rate(boundary: float) -> float:
    if boundary <= 114_000:
        return 0.1
    elif boundary > 114_000 and boundary <= 160_000:
        return 0.12
    else:
        return 0.14

print(f"Revenue (AED): {sales_sum}\nChoosing rate of {choose_revenue_rate(sales_sum)*100}%")

df_actual['Revenue'] = df_actual['Sales'] * choose_revenue_rate(sales_sum)

df_actual['Payout'] = pd.Series(range(df_actual.__len__())).apply(float)

df_actual.loc[df_actual['types'] == 'revenue', 'Payout'] = (df_actual['Revenue'].apply(float) * df_actual['payout_perc'].apply(float))
df_actual.loc[df_actual['types'] == 'sale', 'Payout'] = (df_actual['Sales'].apply(float) * df_actual['payout_perc'].apply(float))

df_final = pd.DataFrame({
    'offer': OFFER_ID,
    'affiliate_id': df_actual['affiliate_id'],
    'date': df_actual['Order Date'],
    'status': 'pending',
    'payout': df_actual['Payout'],
    'revenue': df_actual['Revenue'],
    'sales amount': df_actual['Sales'],
    'coupon': df_actual['code_norm'],
    'geo': df_actual['Country Name'] 
})


df_final.loc[df_final['affiliate_id'] == '1', 'payout'] = 0.0

df_final.to_csv(output_file, index=False)