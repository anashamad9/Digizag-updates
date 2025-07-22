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
input_file = os.path.join(input_dir, 'Dailymealz - DigiZag (commission report)_Detailed Code usages_Table.csv')
df = pd.read_csv(input_file)

# Convert 'Voucher_applied_date' to datetime with a flexible format
df['Voucher_applied_date'] = pd.to_datetime(df['Voucher_applied_date'], format='%b %d, %Y, %I:%M %p', errors='coerce')
print(f"Total rows before filtering: {len(df)}")
print(f"Rows with invalid dates dropped: {len(df) - len(df.dropna(subset=['Voucher_applied_date']))}")

# Filter out cancelled orders and apply date range
df_filtered = df[df['status'] == 'ACCEPTED']
df_filtered = df_filtered[(df_filtered['Voucher_applied_date'].dt.date >= start_date) & 
                         (df_filtered['Voucher_applied_date'].dt.date <= end_date)]
print(f"Rows after filtering cancelled and date range: {len(df_filtered)}")

# Calculate sale amount (total_price / 3.75)
df_filtered['sale_amount'] = df_filtered['total_price'] / 3.75

# Calculate revenue based on new_vs_old
df_filtered['revenue'] = df_filtered.apply(
    lambda row: 16 if row['new_vs_old'] == 'NEW USER' else 4,
    axis=1
)

# Create output dataframe
output_df = pd.DataFrame({
    'offer': 1324,
    'date': df_filtered['Voucher_applied_date'].dt.strftime('%m-%d-%Y'),
    'revenue': df_filtered['revenue'],
    'sale_amount': df_filtered['sale_amount'],
    'coupon_code': df_filtered['code'],
    'geo': 'no-geo'
})

# Save to CSV in the output data folder
output_file = os.path.join(output_dir, 'dailymealz.csv')
output_df.to_csv(output_file, index=False)

print(f"Number of records processed: {len(output_df)}")
print(f"Date range processed: {output_df['date'].min() if not output_df.empty else 'N/A'} to {output_df['date'].max() if not output_df.empty else 'N/A'}")
if len(output_df) < len(df_filtered):
    print("Warning: Some rows were excluded during output creation.")