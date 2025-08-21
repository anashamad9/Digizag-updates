import pandas as pd
from datetime import datetime, timedelta
import os

# Parameters
yesterday = datetime.now().date() - timedelta(days=1)
today = datetime.now().date()

print(f"Current date: {today}, Yesterday: {yesterday}")

# Define directory paths relative to the script location
script_dir = os.path.dirname(os.path.abspath(__file__))
input_dir = os.path.join(script_dir, '..', 'input data')
output_dir = os.path.join(script_dir, '..', 'output data')

# Ensure output directory exists
os.makedirs(output_dir, exist_ok=True)

# Read the CSV file from the input data folder
input_file = os.path.join(input_dir, 'DigiZag Promo External Report_ Digizag External (4).csv')
df = pd.read_csv(input_file)

# Ensure date is in datetime format
df['date'] = pd.to_datetime(df['date'])

# Filter for sales from yesterday, excluding the current day
df_filtered = df[(df['date'].dt.date == yesterday) & (df['date'].dt.date < today)]

# Assign fixed revenue based on customer type
df_filtered['revenue'] = df_filtered['customer type'].map({'new': 10.0, 'returning': 0.5})

# Sort by customer type (new before returning)
df_filtered['customer_type_rank'] = df_filtered['customer type'].map({'new': 0, 'returning': 1})
df_filtered = df_filtered.sort_values(by='customer_type_rank')

# Create output dataframe with required columns
output_df = pd.DataFrame({
    'offer': 1186,
    'date': df_filtered['date'].dt.strftime('%m-%d-%Y'),
    'revenue': df_filtered['revenue'].round(2),
    'sale_amount': df_filtered['amount'].round(2),
    'coupon_code': df_filtered['coupon'],
    'geo': df_filtered['country']
})

# Drop temporary sorting column
df_filtered = df_filtered.drop(columns=['customer_type_rank'])

# Save to CSV in the output data folder
output_file = os.path.join(output_dir, 'toyou.csv')
output_df.to_csv(output_file, index=False)

print(f"Number of records processed: {len(output_df)}")
print(f"Date processed: {yesterday if not output_df.empty else 'N/A'}")