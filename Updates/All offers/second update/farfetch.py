import pandas as pd
import os
from datetime import datetime

AED_TO_USD_DIVISOR = 3.67

# Define directory paths relative to the script location
script_dir = os.path.dirname(os.path.abspath(__file__))
updates_dir = os.path.dirname(os.path.dirname(script_dir))
input_dir = os.path.join(updates_dir, 'Input data')
output_dir = os.path.join(updates_dir, 'output data')

# Ensure output directory exists
os.makedirs(output_dir, exist_ok=True)

# Load the input CSV file
<<<<<<< HEAD
input_file = os.path.join(input_dir, 'farfetch.csv')
=======
input_file = os.path.join(input_dir, 'farfetch99.csv')
>>>>>>> 0d89299 (D)
df = pd.read_csv(input_file)

def safe_number(value, default=0.0):
    """Return a float representation or fallback to default."""
    if pd.isna(value):
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def normalize_publisher_id(value):
    """Normalize publisher IDs to clean strings without decimal artifacts."""
    if pd.isna(value):
        return ''
    text = str(value).strip()
    if not text or text.lower() in {'nan', 'none'}:
        return ''
    try:
        return str(int(float(text)))
    except (TypeError, ValueError):
        return text


def remap_publisher_id(publisher_id):
    """Map legacy IDs to their current equivalents."""
    if publisher_id == '14796':
        return '2345'
    return publisher_id


def calculate_payout(publisher_id, revenue, sale_value):
    """
    Calculate payout based on publisher-specific rules.
    - IDs 2345 (and legacy 14796 remapped above) earn 90% of revenue
    - ID 12941 earns 80% of revenue
    - All others default to 3.8% of sale value
    """
    if publisher_id == '2345':
        return revenue * 0.90
    if publisher_id == '12941':
        return revenue * 0.80
    return sale_value * 0.038

output_data = []
for _, row in df.iterrows():
    datetime_str = row.get('conversion_date_time')
    if pd.isna(datetime_str):
        continue  # Skip row if conversion_date_time is missing
    dt = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')
    formatted_date = dt.strftime('%m-%d-%Y')

    publisher_id_raw = normalize_publisher_id(row.get('publisher_reference', ''))
    publisher_id_clean = remap_publisher_id(publisher_id_raw)
    revenue = safe_number(row.get('item_publisher_commission', 0)) / AED_TO_USD_DIVISOR
    sale_value = safe_number(row.get('item_value', 0)) / AED_TO_USD_DIVISOR
    payout = calculate_payout(publisher_id_clean, revenue, sale_value)
    coupon_value = '14796' if publisher_id_clean == '2345' else 'link'

    output_data.append({
        'offer': 1276,
        'affiliate_id': publisher_id_clean,
        'date': formatted_date,
        'status': 'pending',
        'payout': payout,
        'revenue': revenue,  # Match column AC
        'sale amount': sale_value,
        'coupon': coupon_value,
        'geo': row.get('country', 'Unknown')  # Default to 'Unknown' if missing
    })

# Convert to DataFrame and save to CSV
output_df = pd.DataFrame(output_data)
output_file = os.path.join(output_dir, 'farfetch.csv')
output_df.to_csv(output_file, index=False)

print(f"Processed {len(output_df)} records. Check {output_file} for results.")
