import pandas as pd
from datetime import datetime, timedelta
import os

# Parameters
days_back = 1
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

# Read the CSV file from the input data folder
input_file = os.path.join(input_dir, 'DigiZag Dashboard_Commission Dashboard_Table (1).csv')
df = pd.read_csv(input_file)

# Ensure Date_ordered is in datetime format
df['Date_ordered'] = pd.to_datetime(df['Date_ordered'], format='%b %d, %Y', errors='coerce')
df.dropna(subset=['Date_ordered'], inplace=True)

# Filter for sales from the last 'days_back' days, excluding the current day
df_filtered = df[df['Date_ordered'].dt.date < today]

# Expand rows based on # Orders New Customers and # Orders Repeat Customers
expanded_rows = []
for _, row in df_filtered.iterrows():
    new_orders = int(row['# Orders New Customers'] or 0)
    repeat_orders = int(row['# Orders Repeat Customers'] or 0)

    # Process new customer orders
    if new_orders > 0 and pd.notnull(row['New Cust Revenue']):
        sale_amount_per_new_order = row['New Cust Revenue'] / new_orders
        order_date = row['Date_ordered']  # Ensure datetime is preserved
        for _ in range(new_orders):
            expanded_rows.append({
                'order_date': order_date,
                'country': row['Country'],
                'user_type': 'New',
                'sale_amount': sale_amount_per_new_order,
                'coupon_code': row['follower_code'],
                'revenue': sale_amount_per_new_order * 0.08
            })

    # Process repeat customer orders
    if repeat_orders > 0 and pd.notnull(row['Repeat Cust Revenue']):
        sale_amount_per_repeat_order = row['Repeat Cust Revenue'] / repeat_orders
        order_date = row['Date_ordered']  # Ensure datetime is preserved
        for _ in range(repeat_orders):
            expanded_rows.append({
                'order_date': order_date,
                'country': row['Country'],
                'user_type': 'Repeat',
                'sale_amount': sale_amount_per_repeat_order,
                'coupon_code': row['follower_code'],
                'revenue': sale_amount_per_repeat_order * 0.03
            })

# Create new dataframe from expanded rows
df_expanded = pd.DataFrame(expanded_rows)
df_expanded['order_date'] = pd.to_datetime(df_expanded['order_date'])  # Ensure datetime type

# Sort by user_type (New before Repeat)
df_expanded['user_type_rank'] = df_expanded['user_type'].map({'New': 0, 'Repeat': 1})
df_expanded = df_expanded.sort_values(by='user_type_rank')

# Create output dataframe with required columns
output_df = pd.DataFrame({
    'offer': 1192,
    'date': df_expanded['order_date'].dt.strftime('%m-%d-%Y'),
    'revenue': df_expanded['revenue'].round(2),
    'sale_amount': df_expanded['sale_amount'].round(2),
    'coupon_code': df_expanded['coupon_code'],
    'geo': df_expanded['country']
})

# Drop temporary sorting column
df_expanded = df_expanded.drop(columns=['user_type_rank'])

# Save to CSV in the output data folder
output_file = os.path.join(output_dir, 'mumzworld.csv')
output_df.to_csv(output_file, index=False)

print(f"Number of records processed: {len(output_df)}")
print(f"Date range processed: {output_df['date'].min() if not output_df.empty else 'N/A'} to {output_df['date'].max() if not output_df.empty else 'N/A'}")