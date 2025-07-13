import pandas as pd
from datetime import datetime, timedelta
import os

# Parameters
days_back = 1
end_date = datetime.now().date() + timedelta(days=1)  # 2025-07-14 to include 2025-07-13
start_date = end_date - timedelta(days=days_back + 1)  # 2025-07-11 for days_back = 1
today = datetime.now().date()  # 2025-07-13

print(f"Current date: {today}, Start date (days_back={days_back}): {start_date}")

# Define directory paths relative to the script location
script_dir = os.path.dirname(os.path.abspath(__file__))
input_dir = os.path.join(script_dir, '..', 'input data')
output_dir = os.path.join(script_dir, '..', 'output data')

# Ensure output directory exists
os.makedirs(output_dir, exist_ok=True)

# Read the CSV file from the input data folder
input_file = os.path.join(input_dir, 'EG DigiZag Coupon Dashboard_Affiliate Summary_Table.csv')
df = pd.read_csv(input_file)

# Convert egy_date to MM-DD-YYYY format and filter data for the last 'days_back' days including today
df['egy_date'] = pd.to_datetime(df['egy_date'], format='%b %d, %Y')
df = df[(df['egy_date'].dt.date >= start_date) & (df['egy_date'].dt.date < end_date)]
df['egy_date'] = df['egy_date'].dt.strftime('%m-%d-%Y')

# Define revenue tiers
def get_revenue_per_order(tier):
    if '3.33 - 9.169' in tier:
        return 0.5
    elif '9.17 - 16.499' in tier:
        return 1.0
    elif '16.5 - 24.819' in tier:
        return 1.3
    elif '24.82 - 36.959' in tier:
        return 2.2
    elif '36.96 - 53.409' in tier:
        return 3.25
    elif '53.41 - 89.169' in tier:
        return 4.25
    elif 'Above 89.17' in tier:
        return 6.5
    return 0.0

# Expand rows based on Orders and calculate per-order values
df_expanded = df.loc[df.index.repeat(df['Orders'])].reset_index(drop=True)
df_expanded['sale_amount'] = df_expanded['GMV_USD'] / df_expanded['Orders']
df_expanded['revenue'] = df_expanded['gmv_tag_usd'].apply(get_revenue_per_order)

# Create output dataframe with required columns
output_df = pd.DataFrame({
    'offer': 1282,
    'date': df_expanded['egy_date'],
    'revenue': df_expanded['revenue'].round(2),
    'sale_amount': df_expanded['sale_amount'].round(2),
    'coupon_code': df_expanded['Coupon Code'],
    'geo': 'egy'
})

# Save to CSV in the output data folder
output_file = os.path.join(output_dir, 'noon_egypt.csv')
output_df.to_csv(output_file, index=False)

print(f"Processed {len(output_df)} rows for date range {start_date} to {today}")