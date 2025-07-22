import pandas as pd
import os
from datetime import datetime

# Define directory paths relative to the script location
script_dir = os.path.dirname(os.path.abspath(__file__))
input_dir = os.path.join(script_dir, '..', 'input data')
output_dir = os.path.join(script_dir, '..', 'output data')

# Ensure output directory exists
os.makedirs(output_dir, exist_ok=True)

# Load the input CSV file
input_file = os.path.join(input_dir, 'conversion_item_report_2025-07-19_13_18_43.csv')
df = pd.read_csv(input_file)

# Process the data, handling missing values
def calculate_payout(row):
    publisher_id = row.get('publisher_reference', 0)  # Use publisher_reference (column K)
    if pd.isna(publisher_id):
        publisher_id = 0
    revenue = row.get('item_publisher_commission', 0)
    if pd.isna(revenue):
        revenue = 0
    sale_value = row.get('item_value', 0)
    if pd.isna(sale_value):
        sale_value = 0
    if publisher_id == 12941:
        return revenue * 0.80
    return sale_value * 0.038

output_data = []
for _, row in df.iterrows():
    datetime_str = row.get('conversion_date_time')
    if pd.isna(datetime_str):
        continue  # Skip row if conversion_date_time is missing
    dt = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')
    formatted_date = dt.strftime('%m-%d-%Y')
    
    payout = calculate_payout(row)
    
    output_data.append({
        'offer id': 1276,
        'affiliate id': row.get('publisher_reference', ''),  # Match column K (publisher_reference)
        'datetime': formatted_date,
        'status': 'pending',
        'payout': payout,
        'revenue': row.get('item_publisher_commission', 0),  # Match column AC
        'sale amount': row.get('item_value', 0),
        'affiliate info': 'link',
        'geo': row.get('country', 'Unknown')  # Default to 'Unknown' if missing
    })

# Convert to DataFrame and save to CSV
output_df = pd.DataFrame(output_data)
output_file = os.path.join(output_dir, 'farfetch.csv')
output_df.to_csv(output_file, index=False)

print(f"Processed {len(output_df)} records. Check {output_file} for results.")