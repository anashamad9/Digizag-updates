import pandas as pd
from datetime import datetime, timedelta
import os

# Parameters
days_back = 1
end_date = datetime.now().date() + timedelta(days=1)  # 2025-07-14 to include 2025-07-13
start_date = end_date - timedelta(days=days_back + 1)  # 2025-07-09 for days_back = 3
today = datetime.now().date()  # 2025-07-13

print(f"Current date: {today}, Start date (days_back={days_back}): {start_date}")

# Define directory paths relative to the script location
script_dir = os.path.dirname(os.path.abspath(__file__))
input_dir = os.path.join(script_dir, '..', 'input data')
output_dir = os.path.join(script_dir, '..', 'output data')

# Ensure output directory exists
os.makedirs(output_dir, exist_ok=True)

# Read the Excel file from the input data folder
input_file = os.path.join(input_dir, 'sales (6).xlsx')
df = pd.read_excel(input_file)

# Filter for Noon data
df = df[df['Advertiser'] == 'Noon']

# Convert Order Date to datetime and filter for the last 'days_back' days including today
df['Order Date'] = pd.to_datetime(df['Order Date'])
df = df[(df['Order Date'].dt.date >= start_date) & (df['Order Date'].dt.date < end_date)]
df['Order Date'] = df['Order Date'].dt.strftime('%m-%d-%Y')

# Define geo mapping
geo_mapping = {'SA': 'ksa', 'AE': 'uae', 'BH': 'bhr', 'KW': 'kwt'}

# Expand rows for FTU and RTU
ftu_df = df.loc[df.index.repeat(df['FTU Orders'].fillna(0).astype(int))].reset_index(drop=True)
ftu_df = ftu_df[ftu_df['FTU Orders'] > 0]
ftu_df['sale_amount'] = ftu_df['FTU Order Values'] / ftu_df['FTU Orders'] / 3.67
ftu_df['revenue'] = 4.08

rtu_df = df.loc[df.index.repeat(df['RTU Orders'].fillna(0).astype(int))].reset_index(drop=True)
rtu_df = rtu_df[rtu_df['RTU Orders'] > 0]
rtu_df['sale_amount'] = rtu_df['RTU Order Value'] / rtu_df['RTU Orders'] / 3.67
rtu_df['revenue'] = 2.72

# Combine FTU and RTU data
output_df = pd.concat([ftu_df, rtu_df], ignore_index=True)

# Create output dataframe with required columns
output_df = pd.DataFrame({
    'offer': 1166,
    'date': output_df['Order Date'],
    'revenue': output_df['revenue'].round(2),
    'sale_amount': output_df['sale_amount'].round(2),
    'coupon_code': output_df['Coupon Code'],
    'geo': output_df['Country'].map(geo_mapping)
})

# Save to CSV in the output data folder
output_file = os.path.join(output_dir, 'noon.csv')
output_df.to_csv(output_file, index=False)

print(f"Processed {len(output_df)} rows for date range {start_date} to {today}")