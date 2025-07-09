import pandas as pd
from datetime import datetime, timedelta

# Parameters (adjust days_back as needed, e.g., 3 for previous 3 days)
days_back = 4
end_date = datetime.now().date()
start_date = end_date - timedelta(days=days_back)
today = datetime.now().date()

# Read the CSV files
df_ftu = pd.read_csv('Updates/Input data/FTU.csv')
df_rtu = pd.read_csv('Updates/Input data/RTU.csv')

# Convert Period to MM-DD-YYYY format and filter data for the last 'days_back' days, excluding the current day
for df in [df_ftu, df_rtu]:
    df['Period'] = pd.to_datetime(df['Period'], format='%d %b %Y')
    df = df[df['Period'].dt.date < today] # Exclude current day
    df['Period'] = df['Period'].dt.strftime('%m-%d-%Y')

# Expand rows based on Number of Uses and calculate per-order values
df_ftu_expanded = df_ftu.loc[df_ftu.index.repeat(df_ftu['Number of Uses'])].reset_index(drop=True)
df_ftu_expanded['sale_amount'] = df_ftu_expanded['Sales Total Amount (USD)'] / df_ftu_expanded['Number of Uses']
df_ftu_expanded['revenue'] = df_ftu_expanded.apply(
    lambda row: row['sale_amount'] * 0.10 if any(code in str(row['Coupon Code']) for code in ['fxn', 'cnl', 'ost', 'qqq', 'tnmw', 'fuj', 'df5', 'DD', 'VV', 'ck]']) else row['sale_amount'] * 0.16, 
    axis=1
)

df_rtu_expanded = df_rtu.loc[df_rtu.index.repeat(df_rtu['Number of Uses'])].reset_index(drop=True)
df_rtu_expanded['sale_amount'] = df_rtu_expanded['Sales Total Amount (USD)'] / df_rtu_expanded['Number of Uses']
df_rtu_expanded['revenue'] = df_rtu_expanded.apply(
    lambda row: row['sale_amount'] * 0.10 if any(code in str(row['Coupon Code']) for code in ['fxn', 'cnl', 'ost', 'qqq', 'tnmw', 'fuj', 'df5', 'DD', 'VV', 'ck]']) else row['sale_amount'] * 0.05, 
    axis=1
)

# Create output dataframes with required columns
output_ftu = pd.DataFrame({
    'offer': 910,
    'date': df_ftu_expanded['Period'],
    'revenue': df_ftu_expanded['revenue'].round(2),
    'sale_amount': df_ftu_expanded['sale_amount'].round(2),
    'coupon_code': df_ftu_expanded['Coupon Code'],
    'geo': 'no-geo'
})

output_rtu = pd.DataFrame({
    'offer': 910,
    'date': df_rtu_expanded['Period'],
    'revenue': df_rtu_expanded['revenue'].round(2),
    'sale_amount': df_rtu_expanded['sale_amount'].round(2),
    'coupon_code': df_rtu_expanded['Coupon Code'],
    'geo': 'no-geo'
})

# Combine FTU and RTU data
output_df = pd.concat([output_ftu, output_rtu], ignore_index=True)

# Save to CSV
output_df.to_csv('voga.csv', index=False)