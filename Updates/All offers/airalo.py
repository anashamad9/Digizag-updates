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

# Read the Excel file from the input data folder
input_file = os.path.join(input_dir, '8682-AdvancedActionListi (6).xlsx')
df = pd.read_excel(input_file)

# Parse Action Date to datetime
df['Action Date'] = pd.to_datetime(df['Action Date'])

# Filter for sales from the last 'days_back' days, excluding the current day
df_filtered = df[(df['Action Date'].dt.date >= start_date) & (df['Action Date'].dt.date < today)]

# Calculate sale amount (Sale Amount / 3.67)
df_filtered['sale_amount'] = df_filtered['Sale Amount'] / 3.67

# Calculate revenue (10% of sale amount)
df_filtered['revenue'] = df_filtered['sale_amount'] * 0.10

# Create output dataframe with required columns
output_df = pd.DataFrame({
    'offer': 1256,
    'date': df_filtered['Action Date'].dt.strftime('%m-%d-%Y'),
    'revenue': df_filtered['revenue'].round(2),
    'sale_amount': df_filtered['sale_amount'].round(2),
    'coupon_code': df_filtered['Promo Code'],
    'geo': 'no-geo'
})

# Save to CSV in the output data folder
output_file = os.path.join(output_dir, 'airalo.csv')
output_df.to_csv(output_file, index=False)

print(f"Number of records processed: {len(output_df)}")
print(f"Date range processed: {output_df['date'].min() if not output_df.empty else 'N/A'} to {output_df['date'].max() if not output_df.empty else 'N/A'}")