import pandas as pd
from datetime import datetime, timedelta
import os

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

# Load the CSV data from the input data folder
input_file = os.path.join(input_dir, 'DigiZag__Baytonia_ Orders Tracking Report_DigiZag_Table.csv')
df = pd.read_csv(input_file)

# Convert 'Date' to datetime, keeping track of original values
df['Date'] = pd.to_datetime(df['Date'], format='%b %d, %Y', errors='coerce')
print(f"Total rows before filtering: {len(df)}")
print(f"Rows with invalid dates dropped: {len(df) - len(df.dropna(subset=['Date']))}")

# Filter for 'No bidding' status
df_filtered = df[df['Bidding Status'] == 'No bidding']
print(f"Rows after removing bidding orders: {len(df_filtered)}")

# Filter for date range
df_filtered = df_filtered[(df_filtered['Date'].dt.date >= start_date) & 
                         (df_filtered['Date'].dt.date < end_date)]
print(f"Rows after date filter: {len(df_filtered)}")

# Calculate sale amount (Total / (3.75))
df_filtered['sale_amount'] = df_filtered['Total'] / (3.75)

# Calculate revenue based on Customer Type
df_filtered['revenue'] = df_filtered.apply(
    lambda row: row['sale_amount'] * 0.05 if row['Cutomer Type'] == 'New' else row['sale_amount'] * 0.02,
    axis=1
)

# Create output dataframe
output_df = pd.DataFrame({
    'offer': 1293,
    'date': df_filtered['Date'].dt.strftime('%m-%d-%Y'),
    'revenue': df_filtered['revenue'],
    'sale_amount': df_filtered['sale_amount'],
    'coupon_code': df_filtered['Coupon'],
    'geo': 'no-geo'
})

# Save to CSV in the output data folder
output_file = os.path.join(output_dir, 'baytonia.csv')
output_df.to_csv(output_file, index=False)

print(f"Number of records processed: {len(output_df)}")
print(f"Date range processed: {output_df['date'].min() if not output_df.empty else 'N/A'} to {output_df['date'].max() if not output_df.empty else 'N/A'}")
if len(output_df) < len(df_filtered):
    print("Warning: Some rows were excluded during output creation.")