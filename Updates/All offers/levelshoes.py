import pandas as pd
from datetime import datetime, timedelta
import os

# Parameters (adjust days_back as needed, e.g., 3 for previous 3 days)
days_back = 2
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

# Read the CSV file from the input data folder
input_file = os.path.join(input_dir, 'DigiZag x Level Shoes_All Orders_Table (4).csv')
df = pd.read_csv(input_file)

# Parse Order Date to datetime
df['Order Date'] = pd.to_datetime(df['Order Date'])

# Filter for sales from the last 'days_back' days, excluding the current day
df_filtered = df[(df['Order Date'].dt.date >= start_date) & (df['Order Date'].dt.date < today)]

# Calculate sale amount (Gross Revenue / 3.67)
df_filtered['sale_amount'] = df_filtered['Gross Revenue'] / 3.67

# Calculate revenue based on customer type
df_filtered['revenue'] = df_filtered.apply(
    lambda row: row['sale_amount'] * 0.10 if row['Customer Type'] == 'NEW' else row['sale_amount'] * 0.05,
    axis=1
)

# Map Order Country to 3-letter lowercase geo codes
geo_mapping = {
    'Saudi Arabia': 'ksa',
    'United Arab Emirates': 'uae',
    'Kuwait': 'kwt',
    'Oman': 'omn',
    'Qatar': 'qtr',
    'Bahrain': 'bhr',
    'Jordan': 'jor'
}
df_filtered['geo'] = df_filtered['Order Country'].map(geo_mapping).fillna('no-geo')

# Sort by Customer Type (NEW before Returning)
df_filtered['customer_type_rank'] = df_filtered['Customer Type'].map({'NEW': 0, 'Returning': 1})
df_filtered = df_filtered.sort_values(by='customer_type_rank')

# Create output dataframe with required columns
output_df = pd.DataFrame({
    'offer': 1159,
    'date': df_filtered['Order Date'].dt.strftime('%m-%d-%Y'),
    'revenue': df_filtered['revenue'].round(2),
    'sale_amount': df_filtered['sale_amount'].round(2),
    'coupon_code': df_filtered['Coupon Code'],
    'geo': df_filtered['geo']
})

# Drop temporary sorting column
df_filtered = df_filtered.drop(columns=['customer_type_rank'])

# Save to CSV in the output data folder
output_file = os.path.join(output_dir, 'levelshoes.csv')
output_df.to_csv(output_file, index=False)

print(f"Number of records processed: {len(output_df)}")
print(f"Date range processed: {output_df['date'].min() if not output_df.empty else 'N/A'} to {output_df['date'].max() if not output_df.empty else 'N/A'}")