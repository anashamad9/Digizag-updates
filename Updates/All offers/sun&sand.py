import pandas as pd
from datetime import datetime, timedelta
import os

# Parameters
end_date = datetime.now().date()  # 01:26 PM +03, July 13, 2025
start_date = end_date - timedelta(days=1)

print(f"Current date: {end_date}, Start date (30 days back): {start_date}")

# Define directory paths relative to the script location
script_dir = os.path.dirname(os.path.abspath(__file__))
input_dir = os.path.join(script_dir, '..', 'input data')
output_dir = os.path.join(script_dir, '..', 'output data')

# Ensure output directory exists
os.makedirs(output_dir, exist_ok=True)

# Read the CSV file from the input data folder
input_file = os.path.join(input_dir, 'SSS- Digizag Daily Report_Untitled Page_Table.csv')
df = pd.read_csv(input_file)

# Ensure Date is in datetime format
df['Date'] = pd.to_datetime(df['Date'])

# Debugging: Check available dates
print(f"Unique dates in dataset: {df['Date'].dt.date.unique()}")

# Filter for sales from the last 30 days, excluding Returned and Cancelled status, and current day
df_filtered = df[
    (df['Date'].dt.date >= start_date) &
    (df['Date'].dt.date < end_date) &  # Exclude the current day
    (df['final_status'] != 'Returned') &
    (df['final_status'] != 'Cancelled')
]

# Debugging: Check filtered dates and row count
print(f"Filtered rows: {len(df_filtered)}")
print(f"Filtered dates: {df_filtered['Date'].dt.date.unique()}")

# Convert sale amount to USD (divide by 3.67)
df_filtered['sale_amount_usd'] = df_filtered['net_sales'] / 3.67

# Calculate revenue (6% flat rate)
df_filtered['revenue'] = df_filtered['sale_amount_usd'] * 0.06

# Sort by user_tag (New before Repeat)
df_filtered['user_tag_rank'] = df_filtered['user_tag'].map({'New': 0, 'Repeat': 1})
df_filtered = df_filtered.sort_values(by='user_tag_rank')

# Create output dataframe with required columns
output_df = pd.DataFrame({
    'offer': 1101,
    'date': df_filtered['Date'].dt.strftime('%m-%d-%Y'),
    'revenue': df_filtered['revenue'].round(2),
    'sale_amount': df_filtered['sale_amount_usd'].round(2),
    'coupon_code': df_filtered['coupon_code'],
    'geo': df_filtered['Store']
})

# Drop temporary sorting column
df_filtered = df_filtered.drop(columns=['user_tag_rank'])

# Save to CSV in the output data folder
output_file = os.path.join(output_dir, 'sun_sand.csv')
output_df.to_csv(output_file, index=False)

print(f"Number of records processed: {len(output_df)}")
print(f"Date range processed: {output_df['date'].min() if not output_df.empty else 'N/A'} to {output_df['date'].max() if not output_df.empty else 'N/A'}")