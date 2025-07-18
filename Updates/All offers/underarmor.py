import pandas as pd
from datetime import datetime, timedelta
import os

# Parameters
days_back = 7
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
input_file = os.path.join(input_dir, 'UA - Digizag Data Studio Report by GMG_Untitled Page_Table.csv')
df = pd.read_csv(input_file)

# Convert 'Order Date' column to datetime
df['Order Date'] = pd.to_datetime(df['Order Date'], format='%b %d, %Y', errors='coerce')
print(f"Total rows before filtering: {len(df)}")
print(f"Rows with invalid dates dropped: {len(df) - len(df.dropna(subset=['Order Date']))}")

# Filter by date range
df_filtered = df[(df['Order Date'].dt.date >= start_date) & 
                 (df['Order Date'].dt.date <= end_date)]
print(f"Rows after filtering date range: {len(df_filtered)}")

# Calculate sale amount (Net Sales (in AED) / 3.67)
df_filtered['sale_amount'] = df_filtered['Net Sales (in AED)'] / 3.67

# Calculate revenue (8% of sale amount)
df_filtered['revenue'] = df_filtered['sale_amount'] * 0.08

# Create output dataframe
output_df = pd.DataFrame({
    'offer': 1103,
    'date': df_filtered['Order Date'].dt.strftime('%m-%d-%Y'),
    'revenue': df_filtered['revenue'],
    'sale_amount': df_filtered['sale_amount'],
    'coupon_code': df_filtered['Coupon Code'],
    'geo': 'no-geo',
    
})

# Save to CSV in the output data folder
output_file = os.path.join(output_dir, 'underarmour.csv')
output_df.to_csv(output_file, index=False)

print(f"Number of records processed: {len(output_df)}")
print(f"Date range processed: {output_df['date'].min() if not output_df.empty else 'N/A'} to {output_df['date'].max() if not output_df.empty else 'N/A'}")