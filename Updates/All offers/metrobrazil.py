import pandas as pd
from datetime import datetime, timedelta
import os

# Parameters (adjust days_back as needed, e.g., 3 for previous 3 days)
days_back = 4
end_date = datetime.now().date()
start_date = end_date - timedelta(days=days_back)
today = datetime.now().date()

print(f"Current date: {today}, Start date (days_back={days_back}): {start_date}")

# Define directory paths relative to the script location
script_dir = os.path.dirname(os.path.abspath(__file__))
input_dir = os.path.join(script_dir, '..', 'input data')
output_dir = os.path.join(script_dir, '..', 'output data')

# Ensure output directory exists
os.makedirs(output_dir, exist_ok=True)

# Read the Excel file from the input data folder
input_file = os.path.join(input_dir, 'DigiZag New 30-days.xlsx')
df = pd.read_excel(input_file, sheet_name='DigiZag')

# Parse Date to datetime
df['Date'] = pd.to_datetime(df['Date'])

# Filter for sales from the last 'days_back' days, excluding the current day
df_filtered = df[(df['Date'].dt.date >= start_date) & (df['Date'].dt.date < today)].copy()

# Split rows with Order Count > 1
split_rows = []
for index, row in df_filtered.iterrows():
    if row['Order Count'] > 1:
        net_sales_per_order = row['Net Sales'] / row['Order Count']
        for _ in range(int(row['Order Count'])):
            split_rows.append({
                'Discount Code': row['Discount Code'],
                'Order Name': row['Order Name'],
                'Date': row['Date'],
                'Order Count': 1,
                'Net Sales': net_sales_per_order
            })
    else:
        split_rows.append(row)

df_split = pd.DataFrame(split_rows)

# Calculate sale amount (Net Sales / 3.75)
df_split['sale_amount'] = df_split['Net Sales'] / 3.75

# Calculate revenue (10% of sale amount)
df_split['revenue'] = df_split['sale_amount'] * 0.10

# Create output dataframe with required columns
output_df = pd.DataFrame({
    'offer': 1277,
    'date': df_split['Date'].dt.strftime('%m-%d-%Y'),
    'revenue': df_split['revenue'].round(2),
    'sale_amount': df_split['sale_amount'].round(2),
    'coupon_code': df_split['Discount Code'],
    'geo': 'no-geo'
})

# Save to CSV in the output data folder
output_file = os.path.join(output_dir, 'Metro_brazil.csv')
output_df.to_csv(output_file, index=False)

print(f"Number of records processed: {len(output_df)}")
print(f"Date range processed: {output_df['date'].min() if not output_df.empty else 'N/A'} to {output_df['date'].max() if not output_df.empty else 'N/A'}")