import pandas as pd
from datetime import datetime, timedelta
import os

# Parameters (adjust days_back as needed, e.g., 3 for previous 3 days)
days_back = 4
end_date = datetime.now().date()
start_date = end_date - timedelta(days=days_back)
today = datetime.now().date()

print(f"Current date: {today}, Start date (days_back={days_back}): {start_date}")

# Define directory paths relative to the script location
script_dir = os.path.dirname(os.path.abspath(__file__))
input_dir = os.path.join(script_dir, '..', 'input data')
output_dir = os.path.join(script_dir, '..', 'output data')

# Ensure output directory exists
os.makedirs(output_dir, exist_ok=True)

# Read the CSV files from the input data folder
df_ftu = pd.read_csv(os.path.join(input_dir, 'FTU 2.csv'))
df_rtu = pd.read_csv(os.path.join(input_dir, 'RTU 2.csv'))

# Convert Period to MM-DD-YYYY format and filter data for the last 'days_back' days, excluding the current day
for df in [df_ftu, df_rtu]:
    df['Period'] = pd.to_datetime(df['Period'], format='%d %b %Y')
    df = df[df['Period'].dt.date < today]  # Exclude current day
    df['Period'] = df['Period'].dt.strftime('%m-%d-%Y')

# Expand rows based on Number of Uses and calculate per-order values
df_ftu_expanded = df_ftu.loc[df_ftu.index.repeat(df_ftu['Number of Uses'])].reset_index(drop=True)
df_ftu_expanded['sale_amount'] = df_ftu_expanded['Sales Total Amount (USD)'] / df_ftu_expanded['Number of Uses']
df_ftu_expanded['revenue'] = df_ftu_expanded['sale_amount'] * 0.16

df_rtu_expanded = df_rtu.loc[df_rtu.index.repeat(df_rtu['Number of Uses'])].reset_index(drop=True)
df_rtu_expanded['sale_amount'] = df_rtu_expanded['Sales Total Amount (USD)'] / df_rtu_expanded['Number of Uses']
df_rtu_expanded['revenue'] = df_rtu_expanded['sale_amount'] * 0.05

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

# Save to CSV in the output data folder
output_file = os.path.join(output_dir, 'voga.csv')
output_df.to_csv(output_file, index=False)

print(f"Number of records processed: {len(output_df)}")
print(f"Date range processed: {output_df['date'].min() if not output_df.empty else 'N/A'} to {output_df['date'].max() if not output_df.empty else 'N/A'}")