import pandas as pd
from datetime import datetime, timedelta
import os
import re

# Parameters
days_back = 2
end_date = datetime.now().date()
start_date = end_date - timedelta(days=days_back)

print(f"Current date: {end_date}, Start date (days_back={days_back}): {start_date}")

# Define directory paths relative to the script location
script_dir = os.path.dirname(os.path.abspath(__file__))
input_dir = os.path.join(script_dir, '..', 'input data')
output_dir = os.path.join(script_dir, '..', 'output data')

# Ensure output directory exists
os.makedirs(output_dir, exist_ok=True)

# Find the latest Dr.Nutrition_DigiZag_Report_ file in the input directory
dr_nutrition_files = [f for f in os.listdir(input_dir) if f.startswith('Dr.Nutrition_DigiZag_Report_') and f.endswith('.xlsx')]
if not dr_nutrition_files:
    raise FileNotFoundError("No files starting with 'Dr.Nutrition_DigiZag_Report_' found in the input directory.")

# Extract and sort by timestamp using regex
def extract_timestamp(filename):
    match = re.search(r'Dr\.Nutrition_DigiZag_Report_\d{4}_\d{2}_\d{2}_\d{2}_\d{2}_\d{2}', filename)
    if match:
        return datetime.strptime(match.group(0).replace('Dr.Nutrition_DigiZag_Report_', '').replace('_', '-'), '%Y-%m-%d-%H-%M-%S')
    return datetime.min  # Default to min date if no match

latest_file = max(dr_nutrition_files, key=extract_timestamp)
input_file = os.path.join(input_dir, latest_file)
print(f"Using input file: {latest_file}")

# Load the Excel data from the input data folder
df = pd.read_excel(input_file, sheet_name='Worksheet')

# Convert 'Created Date' to datetime, keeping track of original values
df['Created Date'] = pd.to_datetime(df['Created Date'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
print(f"Total rows before filtering: {len(df)}")
print(f"Rows with invalid dates dropped: {len(df) - len(df.dropna(subset=['Created Date']))}")

# Filter for offer ID 1334 (assuming Campaign 'DigiZag' indicates this offer)
df_offer = df[df['Campaign'] == 'DigiZag']
print(f"Rows with DigiZag Campaign: {len(df_offer)}")

# Remove canceled orders
df_filtered = df_offer[df_offer['Status'] != 'canceled']
print(f"Rows after removing canceled orders: {len(df_filtered)}")

# Filter for date range
df_filtered = df_filtered[(df_filtered['Created Date'].dt.date >= start_date) & 
                         (df_filtered['Created Date'].dt.date < end_date)]
print(f"Rows after date filter (July 03, 2025): {len(df_filtered)}")

# Apply geo mapping and exclude Jordan
def map_geo(geo):
    geo = str(geo).strip() if pd.notnull(geo) else ''
    if geo == 'Saudi Arabia':
        return 'ksa'
    elif geo == 'Kuwait':
        return 'kwt'
    elif geo == 'Qatar':
        return 'qtr'
    elif geo == 'Jordan':
        return None  # Exclude Jordan
    elif geo == 'UAE':
        return 'uae'
    return geo  # Default to original if unmatched

# Create output dataframe
output_df = pd.DataFrame({
    'offer': 1334,
    'date': df_filtered['Created Date'].dt.strftime('%m-%d-%Y'),
    'revenue': df_filtered['commission'] / 3.67,
    'sale_amount': df_filtered['Selling Price'] / 3.67,
    'coupon_code': df_filtered['Code'],
    'geo': df_filtered['country'].apply(map_geo)
})

# Remove rows where geo is None (Jordan)
output_df = output_df.dropna(subset=['geo'])

# Save to CSV in the output data folder
output_file = os.path.join(output_dir, 'Dr Nu.csv')
output_df.to_csv(output_file, index=False)

print(f"Number of records processed: {len(output_df)}")
print(f"Date range processed: {output_df['date'].min() if not output_df.empty else 'N/A'} to {output_df['date'].max() if not output_df.empty else 'N/A'}")
if len(output_df) < len(df_filtered):
    print("Warning: Some rows were excluded during output creation.")