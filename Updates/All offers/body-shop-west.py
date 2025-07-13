import pandas as pd
from datetime import datetime, timedelta
import os

# Parameters
days_back = 17
end_date = datetime.now().date()
start_date = end_date - timedelta(days=days_back)

print(f"Current date: {end_date}, Start date (days_back={days_back}): {start_date}")

# Define directory paths relative to the script location
script_dir = os.path.dirname(os.path.abspath(__file__))
input_dir = os.path.join(script_dir, '..', 'input data')
output_dir = os.path.join(script_dir, '..', 'output data')

# Ensure output directory exists
os.makedirs(output_dir, exist_ok=True)

# Load data from both files
ksa_df = pd.read_csv(os.path.join(input_dir, 'ksa.csv'))
uae_df = pd.read_csv(os.path.join(input_dir, 'uae.csv'))

# Combine dataframes
df = pd.concat([ksa_df, uae_df], ignore_index=True)

# Convert 'Purchase Date (Date)' column to datetime
df['Purchase Date (Date)'] = pd.to_datetime(df['Purchase Date (Date)'], format='%b %d, %Y', errors='coerce')
print(f"Total rows before filtering: {len(df)}")
print(f"Rows with invalid dates dropped: {len(df) - len(df.dropna(subset=['Purchase Date (Date)']))}")

# Filter by date range
df_filtered = df[(df['Purchase Date (Date)'].dt.date >= start_date) & 
                 (df['Purchase Date (Date)'].dt.date <= end_date)]
print(f"Rows after filtering date range: {len(df_filtered)}")

# Set geo based on filename origin
df_filtered.loc[df_filtered.index.isin(ksa_df.index), 'geo'] = 'ksa'
df_filtered.loc[df_filtered.index.isin(uae_df.index), 'geo'] = 'uae'

# Calculate sale amount (" Grand Total (Base) " / 3.67)
df_filtered['sale_amount'] = df_filtered[' Grand Total (Base) '] / 3.67

# Calculate revenue (15% of sale amount)
df_filtered['revenue'] = df_filtered['sale_amount'] * 0.15

# Create output dataframe
output_df = pd.DataFrame({
    'offer': 1182,
    'date': df_filtered['Purchase Date (Date)'].dt.strftime('%m-%d-%Y'),
    'coupon_code': df_filtered['Coupon Code'],
    'geo': df_filtered['geo'],
    'revenue': df_filtered['revenue'],
    'sale_amount': df_filtered['sale_amount']
})

# Save to CSV in the output data folder
output_file = os.path.join(output_dir, 'thebodyshop.csv')
output_df.to_csv(output_file, index=False)

print(f"Number of records processed: {len(output_df)}")
print(f"Date range processed: {output_df['date'].min() if not output_df.empty else 'N/A'} to {output_df['date'].max() if not output_df.empty else 'N/A'}")