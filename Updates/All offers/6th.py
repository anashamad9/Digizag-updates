import pandas as pd
from datetime import datetime, timedelta
import os

# Parameters
days_back = 2
end_date = datetime.now().date()
start_date = end_date - timedelta(days=days_back - 1)

print(f"Current date: {end_date}, Start date (days_back={days_back}): {start_date}")

# Define directory paths relative to the script location
script_dir = os.path.dirname(os.path.abspath(__file__))
input_dir = os.path.join(script_dir, '..', 'input data')
output_dir = os.path.join(script_dir, '..', 'output data')

# Ensure output directory exists
os.makedirs(output_dir, exist_ok=True)

# Load the Excel data from the input data folder
input_file = os.path.join(input_dir, 'DigiZag X 6thStreet Performance Tracker.xlsx')
df = pd.read_excel(input_file)

# Convert 'order_date' column to datetime
df['order_date'] = pd.to_datetime(df['order_date'])

# Filter for sales from the previous 3 days including today, excluding canceled orders
df_filtered = df[
    (df['order_date'].dt.date >= start_date) &
    (df['order_date'].dt.date <= end_date) &
    (df['status'] != 'canceled')
]

# Expand rows based on qty
expanded_rows = []
for _, row in df_filtered.iterrows():
    qty = int(row['qty'])  # Number of orders in the row
    sale_amount_per_order = row['gmv'] / qty  # Divide total gmv by qty
    for _ in range(qty):  # Duplicate row qty times
        expanded_rows.append({
            'order_date': row['order_date'],
            'country': row['country'],
            'user_type': row['user_type'],
            'sale_amount': sale_amount_per_order,
            'discount_code': row['discount_code']
        })

df_expanded = pd.DataFrame(expanded_rows)

# Convert sale amount to USD and calculate revenue
df_expanded['sale_amount_usd'] = df_expanded['sale_amount'] / 3.67
df_expanded['revenue'] = df_expanded.apply(
    lambda row: row['sale_amount_usd'] * 0.10 if row['user_type'] == 'FTU' else row['sale_amount_usd'] * 0.05,
    axis=1
)

# Sort by user type rank
df_expanded['user_type_rank'] = df_expanded['user_type'].map({'FTU': 0, 'Repeat': 1})
df_expanded = df_expanded.sort_values(by='user_type_rank')
df_expanded = df_expanded.drop(columns=['user_type_rank'])

# Prepare output dataframe
output_df = pd.DataFrame({
    'offer': 1325,
    'date': df_expanded['order_date'].dt.strftime('%m-%d-%Y'),
    'revenue': df_expanded['revenue'].round(2),
    'sale_amount': df_expanded['sale_amount_usd'].round(2),
    'coupon_code': df_expanded['discount_code'],
    'geo': df_expanded['country'],
})

# Save to CSV in the output data folder
output_file = os.path.join(output_dir, '6th.csv')
output_df.to_csv(output_file, index=False)

print(f"Number of records processed: {len(output_df)}")
print(f"Date range processed: {output_df['date'].min() if not output_df.empty else 'N/A'} to {output_df['date'].max() if not output_df.empty else 'N/A'}")