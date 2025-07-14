import pandas as pd
from datetime import datetime, timedelta
import os

# Parameters (adjust days_back as needed, e.g., 3 for previous 3 days)
days_back = 6
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

# Read the CSV file from the input data folder
input_file = os.path.join(input_dir, 'digizag_2025-07-14T07_55_27.152204Z.csv')
df = pd.read_csv(input_file)

# Parse Order Date to datetime
df['Order Date'] = pd.to_datetime(df['Order Date'])

# Filter for sales from the last 'days_back' days and non-cancelled orders, excluding the current day
df_filtered = df[(df['Order Date'].dt.date >= start_date) & (~df['Order Status'].str.contains('Cancelled', case=False, na=False)) & (df['Order Date'].dt.date < today)]

# Calculate sale amount and revenue based on currency
def convert_amount(row, column):
    currency = row['Currency']
    value = row[column]
    if pd.isna(value):
        return 0.0
    if currency == 'SAR':
        return value / 3.75
    elif currency == 'AED':
        return value / 3.67
    else:
        return value / 3.67  # Default to AED rate for unrecognized currencies

df_filtered['sale_amount'] = df_filtered.apply(lambda row: convert_amount(row, 'Sale Amount'), axis=1)
df_filtered['revenue'] = df_filtered.apply(lambda row: convert_amount(row, 'Payout'), axis=1)

# Map Geo to 3-letter lowercase codes
geo_mapping = {
    'UAE': 'uae',
    'KSA': 'ksa'
}
df_filtered['geo'] = df_filtered['Geo'].map(geo_mapping).fillna('no-geo')

# Create output dataframe with required columns
output_df = pd.DataFrame({
    'offer': 1279,
    'date': df_filtered['Order Date'].dt.strftime('%m-%d-%Y'),
    'revenue': df_filtered['revenue'].round(2),
    'sale_amount': df_filtered['sale_amount'].round(2),
    'coupon_code': df_filtered['Coupon Code'],
    'geo': df_filtered['geo']
})

# Save to CSV in the output data folder
output_file = os.path.join(output_dir, 'Cartlow.csv')
output_df.to_csv(output_file, index=False)

print(f"Number of records processed: {len(output_df)}")
print(f"Date range processed: {output_df['date'].min() if not output_df.empty else 'N/A'} to {output_df['date'].max() if not output_df.empty else 'N/A'}")