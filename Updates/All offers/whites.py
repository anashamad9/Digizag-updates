import pandas as pd
import requests
import os
import json

# Define directory paths relative to the script location
script_dir = os.path.dirname(os.path.abspath(__file__))
input_dir = os.path.join(script_dir, '..', 'input data')
output_dir = os.path.join(script_dir, '..', 'output data')

# Ensure output directory exists
os.makedirs(output_dir, exist_ok=True)

# Load the JSON data from the provided link
json_url = "https://www.whites.net/entity/track_coupons_digizag?get_coupon_data=1&start_date=2025-08-01&end_date=2025-08-31"
response = requests.get(json_url)
if response.status_code != 200:
    raise Exception(f"Failed to fetch data from {json_url}, status code: {response.status_code}")
data = response.json()
df = pd.json_normalize(data)

# Print available columns and inspect 'data' column structure
print("Available columns:", df.columns.tolist())
if 'data' in df.columns:
    print("Sample 'data' column content (first 2 rows):")
    print(df['data'].head(2).to_dict())
    # Attempt to normalize if 'data' contains a list or dict
    if df['data'].apply(lambda x: isinstance(x, (list, dict))).all():
        df = pd.json_normalize(df['data'].explode().dropna())
        print("Normalized columns after exploding 'data':", df.columns.tolist())
    else:
        raise ValueError("Unexpected 'data' column format. Please check the printed content.")

# Convert final_amount to float and calculate sale_amount and revenue
df['final_amount'] = pd.to_numeric(df['final_amount'], errors='coerce')
df['sale_amount'] = df['final_amount'] / 3.75
df['revenue'] = df['sale_amount'] * 0.10

# Prepare output dataframe with fixed geo as 'ksa'
output_df = pd.DataFrame({
    'offer': 1345,
    'date': pd.to_datetime(df['order_date']).dt.strftime('%m-%d-%Y'),
    'revenue': df['revenue'].round(2),
    'sale_amount': df['sale_amount'].round(2),
    'coupon_code': df['coupon_code'],
    'geo': 'ksa'
})

# Save to CSV in the output data folder
output_file = os.path.join(output_dir, 'whites2.csv')
output_df.to_csv(output_file, index=False)

print(f"Number of records processed: {len(output_df)}")
print(f"Date range processed: {output_df['date'].min() if not output_df.empty else 'N/A'} to {output_df['date'].max() if not output_df.empty else 'N/A'}")