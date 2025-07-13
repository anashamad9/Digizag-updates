import pandas as pd
from datetime import datetime, timedelta
import os

# Parameters
days_back = 9
end_date = datetime.now().date()
start_date = end_date - timedelta(days=days_back)
today = datetime.now().date()

print(f"Current date: {end_date}, Start date (days_back={days_back}): {start_date}")

# Define directory paths relative to the script location
script_dir = os.path.dirname(os.path.abspath(__file__))
input_dir = os.path.join(script_dir, '..', 'input data')
output_dir = os.path.join(script_dir, '..', 'output data')

# Ensure output directory exists
os.makedirs(output_dir, exist_ok=True)

# Read the CSV file from the input data folder
input_file = os.path.join(input_dir, 'ConversionsExport_2025-07-09_2025-07-09.csv')
df = pd.read_csv(input_file)

# Convert 'date' to datetime, keeping track of original values and exclude the current day
df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
df.dropna(subset=['date'], inplace=True)
df = df[df['date'].dt.date < today]  # Exclude current day

print(f"Total rows before filtering: {len(df)}")
print(f"Rows with invalid dates dropped: {len(df) - len(df.dropna(subset=['date']))}")

# Initial filter for Eyewa Affiliates Program
df_eyewa = df[df['offer_name'] == 'Eyewa Affiliates Program']
print(f"Rows with Eyewa Affiliates Program: {len(df_eyewa)}")

# Filter for date range
df_filtered = df_eyewa[(df_eyewa['date'].dt.date >= start_date) &
                       (df_eyewa['date'].dt.date < end_date)]
print(f"Rows after date filter (June 27, 2025): {len(df_filtered)}")

# Calculate revenue based on adv1
def calculate_revenue(row):
    sale_amount = float(row['sale_amount']) if pd.notnull(row['sale_amount']) else 0.0
    adv1 = str(row['adv1']).strip() if pd.notnull(row['adv1']) else ''
    if adv1 == '3P':
        return sale_amount * 0.20
    elif adv1 == 'HB Frames':
        return sale_amount * 0.15
    elif adv1 == 'HB Lense':
        return sale_amount * 0.20
    return 0.0

df_filtered['revenue'] = df_filtered.apply(calculate_revenue, axis=1)

# Create output dataframe with required columns and geo mapping
def map_geo(geo):
    geo = str(geo).strip() if pd.notnull(geo) else ''
    if geo == 'Saudi Arabia':
        return 'ksa'
    elif geo == 'Kuwait':
        return 'kwt'
    elif geo == 'Qatar':
        return 'qtr'
    elif geo == 'ARE':
        return 'egy'
    elif geo == 'UAE':
        return 'uae'
    return geo  # Default to original if unmatched

output_df = pd.DataFrame({
    'offer': 1204,
    'date': df_filtered['date'].dt.strftime('%m-%d-%Y'),
    'revenue': df_filtered['revenue'].round(2),
    'sale_amount': df_filtered['sale_amount'].round(2),
    'coupon_code': df_filtered['coupon_code'],
    'geo': df_filtered['adv2'].apply(map_geo)
})

# Save to CSV in the output data folder
output_file = os.path.join(output_dir, 'eweya.csv')
output_df.to_csv(output_file, index=False)

print(f"Number of records processed: {len(output_df)}")
print(f"Date range processed: {output_df['date'].min()} to {output_df['date'].max()}")
if len(output_df) < len(df_filtered):
    print("Warning: Some rows were excluded during output creation.")