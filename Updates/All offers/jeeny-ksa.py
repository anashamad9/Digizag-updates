import pandas as pd
from datetime import datetime, timedelta
import os

# Read the CSV file from the input data folder
input_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'input data', 'ksa-digizag-report-2025-07-11.csv')
df = pd.read_csv(input_file)

# Convert Date to MM-DD-YYYY format and exclude the current day
today = datetime.now().date()
df['Date'] = pd.to_datetime(df['Date'], format='%d-%b-%Y')
df = df[df['Date'].dt.date < today]  # Exclude current day
df['Date'] = df['Date'].dt.strftime('%m-%d-%Y')

# Expand rows based on Usage
df_expanded = df.loc[df.index.repeat(df['Usage'])].reset_index(drop=True)

# Create output dataframe with required columns
output_df = pd.DataFrame({
    'offer': 1260,
    'date': df_expanded['Date'],
    'revenue': 2,
    'sale_amount': 0,
    'coupon_code': df_expanded['coupon'],
    'geo': 'ksa'
})

# Define directory paths relative to the script location
output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'output data')
os.makedirs(output_dir, exist_ok=True)

# Save to CSV in the output data folder
output_file = os.path.join(output_dir, 'jeeny_ksa.csv')
output_df.to_csv(output_file, index=False)

print(f"Number of records processed: {len(output_df)}")
print(f"Date range processed: {output_df['date'].min() if not output_df.empty else 'N/A'} to {output_df['date'].max() if not output_df.empty else 'N/A'}")