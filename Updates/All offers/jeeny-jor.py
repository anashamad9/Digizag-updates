import pandas as pd
from datetime import datetime, timedelta
import os
import re

# Parameters
days_back = 5
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

# Find the latest jor-digizag-report- file in the input directory
jor_digizag_files = [f for f in os.listdir(input_dir) if f.startswith('jor-digizag-report-') and f.endswith('.csv')]
if not jor_digizag_files:
    raise FileNotFoundError("No files starting with 'jor-digizag-report-' found in the input directory.")

# Extract and sort by date using regex
def extract_date(filename):
    match = re.search(r'jor-digizag-report-(\d{4}-\d{2}-\d{2})', filename)
    if match:
        return datetime.strptime(match.group(1), '%Y-%m-%d')
    return datetime.min  # Default to min date if no match

latest_file = max(jor_digizag_files, key=extract_date)
input_file = os.path.join(input_dir, latest_file)
print(f"Using input file: {latest_file}")

# Read the CSV file from the input data folder
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
    'offer': 1261,
    'date': df_expanded['Date'],
    'revenue': 1,
    'sale_amount': 0,
    'coupon_code': df_expanded['coupon'],
    'geo': 'jor'
})

# Save to CSV in the output data folder
output_file = os.path.join(output_dir, 'jeeny_jor.csv')
output_df.to_csv(output_file, index=False)

print(f"Number of records processed: {len(output_df)}")
print(f"Date range processed: {output_df['date'].min() if not output_df.empty else 'N/A'} to {output_df['date'].max() if not output_df.empty else 'N/A'}")