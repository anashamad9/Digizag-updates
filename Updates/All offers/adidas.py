import pandas as pd
from datetime import datetime, timedelta
import os

# Parameters (adjust days_back as needed, e.g., 18 for previous 18 days)
days_back = 5
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

# Read the CSV file from the input data folder, skipping initial report header rows
input_file = os.path.join(input_dir, 'Individual-Item-Report.csv')
df = pd.read_csv(input_file, skiprows=range(4))

# Convert 'Transaction Date' to datetime, coercing errors and dropping NaT values
df['Transaction Date'] = pd.to_datetime(df['Transaction Date'], format='%m/%d/%y', errors='coerce')
df.dropna(subset=['Transaction Date'], inplace=True)

# Filter for Adidas KSA data and date range, excluding the current day
df_filtered = df[(df['Advertiser Name'] == 'Adidas KSA') &
                 (df['Transaction Date'].dt.date >= start_date) &
                 (df['Transaction Date'].dt.date < today)]  # Exclude current day

# Split rows with # of Items > 1
split_rows = []
for index, row in df_filtered.iterrows():
    items = int(row['# of Items']) if pd.notnull(row['# of Items']) else 1
    sales_per_item = float(row['Sales']) / items if pd.notnull(row['Sales']) else 0.0
    for _ in range(items):
        split_rows.append({
            'Order ID': row['Order ID'],
            'Transaction Date': row['Transaction Date'],
            'Sales': sales_per_item,
            '# of Items': 1
        })

df_split = pd.DataFrame(split_rows)

# Calculate sale amount (Sales * 1.31)
df_split['sale_amount'] = df_split['Sales'] * 1.31

# Calculate revenue (7% of sale amount)
df_split['revenue'] = df_split['sale_amount'] * 0.07

# Create output dataframe with required columns
output_df = pd.DataFrame({
    'offer': 1283,
    'date': df_split['Transaction Date'].dt.strftime('%m-%d-%Y'),
    'revenue': df_split['revenue'].round(2),
    'sale_amount': df_split['sale_amount'].round(2),
    'coupon_code': df_split['Order ID'],
    'geo': 'ksa',
})

# Save to CSV in the output data folder
output_file = os.path.join(output_dir, 'adidas.csv')
output_df.to_csv(output_file, index=False)