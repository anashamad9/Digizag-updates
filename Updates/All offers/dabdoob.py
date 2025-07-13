import pandas as pd
from datetime import datetime, timedelta
import os

# Parameters (adjust days_back as needed, e.g., 3 for previous 3 days)
days_back = 2
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

# Read the Excel file from the input data folder
input_file = os.path.join(input_dir, 'Orders_Coupons_Report_Digizag.xlsx')
df = pd.read_excel(input_file, sheet_name='Sheet1')

# Parse Order Date (Full date) to datetime
df['Order Date (Full date)'] = pd.to_datetime(df['Order Date (Full date)'], format='%d %b, %Y %H:%M:%S')

# Filter for sales from the last 'days_back' days and non-cancelled orders, excluding the current day
df_filtered = df[(df['Order Date (Full date)'].dt.date >= start_date) & (df['Status Of Order'] != 'Cancelled') & (df['Order Date (Full date)'].dt.date < today)]

# Calculate sale amount based on country
def calculate_sale_amount(row):
    country = row['Country']
    subtotal = row['Subtotal']
    if country == 'Saudi Arabia':
        return subtotal / 3.75
    elif country == 'UAE':
        return subtotal / 3.67
    elif country == 'Bahrain':
        return subtotal * 2.65
    elif country == 'Kuwait':
        return subtotal * 3.26
    else:
        return subtotal / 3.67  # Default to UAE rate for unrecognized countries

df_filtered['sale_amount'] = df_filtered.apply(calculate_sale_amount, axis=1)

# Calculate revenue (10% of sale amount)
df_filtered['revenue'] = df_filtered['sale_amount'] * 0.10

# Map Country to 3-letter lowercase geo codes
geo_mapping = {
    'Saudi Arabia': 'ksa',
    'UAE': 'uae',
    'Bahrain': 'bhr',
    'Kuwait': 'kwt'
}
df_filtered['geo'] = df_filtered['Country'].map(geo_mapping).fillna('no-geo')

# Create output dataframe with required columns
output_df = pd.DataFrame({
    'offer': 1329,
    'date': df_filtered['Order Date (Full date)'].dt.strftime('%m-%d-%Y'),
    'revenue': df_filtered['revenue'].round(2),
    'sale_amount': df_filtered['sale_amount'].round(2),
    'coupon_code': df_filtered['Coupon'],
    'geo': df_filtered['geo']
})

# Save to CSV in the output data folder
output_file = os.path.join(output_dir, 'dabdoub.csv')
output_df.to_csv(output_file, index=False)

print(f"Number of records processed: {len(output_df)}")
print(f"Date range processed: {output_df['date'].min() if not output_df.empty else 'N/A'} to {output_df['date'].max() if not output_df.empty else 'N/A'}")