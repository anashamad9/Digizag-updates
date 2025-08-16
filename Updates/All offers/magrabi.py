import pandas as pd
from datetime import datetime, timedelta
import os

# Parametersss
days_back = 140
end_date = datetime.now().date()
start_date = end_date - timedelta(days=days_back)

print(f"Current date: {end_date}, Start date (days_back={days_back}): {start_date}")

# Define directory paths relative to the script location
script_dir = os.path.dirname(os.path.abspath(__file__))
input_dir = os.path.join(script_dir, '..', 'input data')
output_dir = os.path.join(script_dir, '..', 'output data')

# Ensure output directory exists
os.makedirs(output_dir, exist_ok=True)

# Load the Excel data from the input data folder
input_file = os.path.join(input_dir, 'DigiZag_MAGRABi_Report (2).xlsx')
df = pd.read_excel(input_file, sheet_name='Sheet1')

# Convert 'date' column to datetime, handling mixed formats
def convert_date(date_val):
    if pd.isna(date_val):
        return pd.NaT
    try:
        # Try converting as Excel serial date
        return pd.to_datetime(date_val, origin='1899-12-30', unit='D')
    except ValueError:
        # Try converting as string date
        return pd.to_datetime(date_val, errors='coerce')

df['date'] = df['date'].apply(convert_date)
print(f"Total rows before filtering: {len(df)}")
print(f"Rows with invalid dates dropped: {len(df) - len(df.dropna(subset=['date']))}")

# Filter out cancelled orders and apply date range
df_filtered = df[df['status'] != 'Cancelled']
df_filtered = df_filtered[(df_filtered['date'].dt.date >= start_date) & 
                         (df_filtered['date'].dt.date <= end_date)]
print(f"Rows after filtering cancelled and date range: {len(df_filtered)}")

# Calculate sale amount (price / 3.75)
df_filtered['sale_amount'] = df_filtered['price (SAR)'] / 3.75

# Set revenue to fixed value 26.66
df_filtered['revenue'] = 26.66

# Create output dataframe
output_df = pd.DataFrame({
    'offer': 1291,
    'date': df_filtered['date'].dt.strftime('%m-%d-%Y'),
    'revenue': df_filtered['revenue'],
    'sale_amount': df_filtered['sale_amount'],
    'coupon_code': df_filtered['Coupon Code'],
    'geo': df_filtered['country']
})

# Save to CSV in the output data folder
output_file = os.path.join(output_dir, 'magrabi.csv')
output_df.to_csv(output_file, index=False)

print(f"Number of records processed: {len(output_df)}")
print(f"Date range processed: {output_df['date'].min() if not output_df.empty else 'N/A'} to {output_df['date'].max() if not output_df.empty else 'N/A'}")
if len(output_df) < len(df_filtered):
    print("Warning: Some rows were excluded during output creation.")