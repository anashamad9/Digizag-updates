import pandas as pd
from datetime import datetime, timedelta
import os

# Parameters
days_back = 7
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
input_file = os.path.join(input_dir, 'Social Affiliate - digizag_Untitled Page_Pivot table (1).csv')
df = pd.read_csv(input_file)

# Convert 'Date' column to datetime
df['Date'] = pd.to_datetime(df['Date'], format='%b %d, %Y', errors='coerce')
print(f"Total rows before filtering: {len(df)}")
print(f"Rows with invalid dates dropped: {len(df) - len(df.dropna(subset=['Date']))}")

# Filter by date range
df_filtered = df[(df['Date'].dt.date >= start_date) & 
                 (df['Date'].dt.date <= end_date)]
print(f"Rows after filtering date range: {len(df_filtered)}")

# Map brand to offer ID
brand_to_offer = {'vs': 1208, 'pk': 1250, 'nb': 1161, 'mc': 1146, 'hm': 1132, 
                  'fl': 1160, 'bbw': 1130, 'aeo': 1133, 'pb': 1176, 'wes': 1131}
df_filtered['offer'] = df_filtered['Brand'].map(brand_to_offer)

# Remove rows where Affiliate Revenue is 0
df_filtered = df_filtered[df_filtered['Affiliate Cost'] != 0]

# Create output dataframe
output_df = pd.DataFrame({
    'offer': df_filtered['offer'],
    'date': df_filtered['Date'].dt.strftime('%m-%d-%Y'),
    'coupon_code': df_filtered['Coupon Code'],
    'geo': df_filtered['Market'],
    'revenue': df_filtered['Affiliate Cost'],
    'sale_amount': df_filtered['Affiliate Revenue']  # Assuming Affiliate Orders represents sale amount contextually
})

# Save to CSV in the output data folder
output_file = os.path.join(output_dir, 'shaye3.csv')
output_df.to_csv(output_file, index=False)

print(f"Number of records processed: {len(output_df)}")
print(f"Date range processed: {output_df['date'].min() if not output_df.empty else 'N/A'} to {output_df['date'].max() if not output_df.empty else 'N/A'}")