import pandas as pd
from datetime import datetime, timedelta
import os
import re

# Parameters (adjust days_back as needed, e.g., 2 for previous 2 days including today)
days_back = 2
end_date = datetime.now().date() + timedelta(days=1)  # 2025-07-25 to include 2025-07-24
start_date = end_date - timedelta(days=days_back)     # 2025-07-23 for days_back = 2
today = datetime.now().date()                         # 2025-07-24

print(f"Current date: {today}, Start date (days_back={days_back}): {start_date}")

# Define directory paths relative to the script location
script_dir = os.path.dirname(os.path.abspath(__file__))
input_dir = os.path.join(script_dir, '..', 'input data')
output_dir = os.path.join(script_dir, '..', 'output data')

# Ensure output directory exists
os.makedirs(output_dir, exist_ok=True)

# Find the latest sales-DigiZag- file in the input directory
sales_digizag_files = [f for f in os.listdir(input_dir) if f.startswith('sales-DigiZag-') and f.endswith('.csv')]
if not sales_digizag_files:
    raise FileNotFoundError("No files starting with 'sales-DigiZag-' found in the input directory.")

# Extract and sort by start date using regex
def extract_start_date(filename):
    match = re.search(r'sales-DigiZag-(\d{4}-\d{2}-\d{2})__\d{4}-\d{2}-\d{2}.csv', filename)
    if match:
        return datetime.strptime(match.group(1), '%Y-%m-%d')
    return datetime.min  # Default to min date if no match

latest_file = max(sales_digizag_files, key=extract_start_date)
input_file = os.path.join(input_dir, latest_file)
print(f"Using input file: {latest_file}")

# Read the CSV file from the input data folder
df = pd.read_csv(input_file)

# Parse Puchase Date to datetime
df['Puchase Date'] = pd.to_datetime(df['Puchase Date'])

# Filter for sales from the last 'days_back' days including today with delivered or complete status
df_filtered = df[(df['Puchase Date'].dt.date >= start_date) & (df['Puchase Date'].dt.date < end_date)]

# Calculate sale amount (FINAL_TOTAL * 3.26)
df_filtered['sale_amount'] = df_filtered['FINAL_TOTAL'] * 3.26

# Calculate revenue (10% for new, 7% for existing customers)
df_filtered['revenue'] = df_filtered.apply(
    lambda row: row['sale_amount'] * 0.10 if row['Customer_Type'] == 'New' else row['sale_amount'] * 0.07,
    axis=1
)

# Map country to 3-letter lowercase geo codes
geo_mapping = {
    'Bahrain': 'bhr',
    'Saudi Arabia': 'ksa',
    'Kuwait': 'kwt',
    'United Arab Emirates': 'uae',
    'Oman': 'omn',
    'Qatar': 'qat',
    'Jordan': 'jor'
}
df_filtered['geo'] = df_filtered['Country'].map(geo_mapping).fillna('no-geo')

# Create output dataframe with required columns
output_df = pd.DataFrame({
    'offer': 1183,
    'date': df_filtered['Puchase Date'].dt.strftime('%m-%d-%Y'),
    'revenue': df_filtered['revenue'].round(2),
    'sale_amount': df_filtered['sale_amount'].round(2),
    'coupon_code': df_filtered['Coupon Code'],
    'geo': df_filtered['geo']
})

# Save to CSV in the output data folder
output_file = os.path.join(output_dir, 'RivaFashion.csv')
output_df.to_csv(output_file, index=False)

print(f"Processed {len(output_df)} rows for date range {start_date} to {today}")