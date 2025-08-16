import pandas as pd
from datetime import datetime, timedelta
import os

# Parameters
days_back = 4
end_date = datetime.now().date() + timedelta(days=1)  # 2025-08-05 to include 2025-08-04
start_date = end_date - timedelta(days=days_back + 1)  # 2025-08-03 for days_back = 1
today = datetime.now().date()  # 2025-08-05

print(f"Current date: {today}, Start date (days_back={days_back}): {start_date}")

# Define directory paths relative to the script location
script_dir = os.path.dirname(os.path.abspath(__file__))
input_dir = os.path.join(script_dir, '..', 'input data')
output_dir = os.path.join(script_dir, '..', 'output data')

# Ensure output directory exists
os.makedirs(output_dir, exist_ok=True)

# Read the CSV file from the input data folder
input_file = os.path.join(input_dir, 'EG DigiZag Coupon Dashboard_Affiliate Summary_Table (1).csv')
df = pd.read_csv(input_file)

# Convert egy_date to MM-DD-YYYY format and filter data for the last 'days_back' days including today
df['egy_date'] = pd.to_datetime(df['egy_date'], format='%b %d, %Y')
df = df[(df['egy_date'].dt.date >= start_date) & (df['egy_date'].dt.date < end_date)]
df['egy_date'] = df['egy_date'].dt.strftime('%m-%d-%Y')

# Define revenue tiers based on new structure
def get_revenue_per_order(tier):
    if '4.75 - 14.25' in tier:
        return 0.30
    elif '14.26 - 23.85' in tier:
        return 0.70
    elif '23.86 - 37.24' in tier:
        return 1.30
    elif '37.25 - 59.40' in tier:
        return 2.20
    elif '59.41 - 72.00' in tier:
        return 3.25
    elif '72.01 - 110.00' in tier:
        return 4.25
    elif 'Above 110.01' in tier:
        return 7.00
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