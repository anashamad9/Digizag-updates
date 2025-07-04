import pandas as pd
from datetime import datetime, timedelta
import os

# Parameters
days_back = 190
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
input_file = os.path.join(input_dir, 'Digizag_Untitled Page_Table.csv')
df = pd.read_csv(input_file)

# Convert 'created_at' column to datetime
df['created_at'] = pd.to_datetime(df['created_at'], format='%b %d, %Y', errors='coerce')
print(f"Total rows before filtering: {len(df)}")
print(f"Rows with invalid dates dropped: {len(df) - len(df.dropna(subset=['created_at']))}")

# Filter by date range
df_filtered = df[(df['created_at'].dt.date >= start_date) & 
                 (df['created_at'].dt.date <= end_date)]
print(f"Rows after filtering date range: {len(df_filtered)}")

# Normalize coupon codes to uppercase
df_filtered['coupon_code'] = df_filtered['coupon_code'].str.upper()

# Split rows based on Record Count
rows = []
for _, row in df_filtered.iterrows():
    record_count = int(row['Record Count'])
    sale_amount = float(row['grand_total']) / 3.75  # Divide sale amount by 3.75
    revenue = sale_amount * 0.10  # 10% revenue
    for _ in range(record_count):
        rows.append({
            'offer': 1327,
            'date': row['created_at'].strftime('%m-%d-%Y'),
            'coupon_code': row['coupon_code'],
            'geo': 'ksa',
            'revenue': revenue / record_count,
            'sale_amount': sale_amount / record_count
        })

output_df = pd.DataFrame(rows)

# Save to CSV in the output data folder
output_file = os.path.join(output_dir, 'alokozay.csv')
output_df.to_csv(output_file, index=False)

print(f"Number of records processed: {len(output_df)}")
print(f"Date range processed: {output_df['date'].min() if not output_df.empty else 'N/A'} to {output_df['date'].max() if not output_df.empty else 'N/A'}")