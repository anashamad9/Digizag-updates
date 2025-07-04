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

# Load the Excel file from the input data folder
input_file = os.path.join(input_dir, 'Degi Zag - Daily Sales Report .xlsx')
xls = pd.ExcelFile(input_file)

# Dictionary to map sheet names to offer IDs
offer_ids = {'Rasees': 1163, 'Beluar': 1253, 'Waya': 1254}

# Process each sheet separately
for sheet_name in xls.sheet_names:
    if sheet_name in offer_ids:
        # Read the sheet, skipping the header row
        df = pd.read_excel(input_file, sheet_name=sheet_name, header=0)
        
        # Convert 'Created_at' column to datetime, handling both string and Excel serial dates
        def convert_date(x):
            try:
                return pd.to_datetime(x, errors='coerce')
            except (ValueError, TypeError):
                return pd.to_datetime(x, unit='D', origin='1899-12-30', errors='coerce')
        
        df['Created_at'] = df['Created_at'].apply(convert_date)
        
        print(f"Processing sheet: {sheet_name}, Total rows: {len(df)}")
        print(f"Rows with invalid dates dropped: {len(df) - len(df.dropna(subset=['Created_at']))}")

        # Filter by date range
        df_filtered = df[(df['Created_at'].dt.date >= start_date) & 
                         (df['Created_at'].dt.date <= end_date)]
        print(f"Rows after filtering date range for {sheet_name}: {len(df_filtered)}")

        # Process each row
        all_rows = []
        for _, row in df_filtered.iterrows():
            sale_amount = float(row['Amount']) / 3.75
            revenue = sale_amount * 0.13
            all_rows.append({
                'offer': offer_ids[sheet_name],
                'date': row['Created_at'].strftime('%m-%d-%Y'),
                'coupon_code': row['Coupon'],
                'geo': 'ksa',
                'revenue': revenue,
                'sale_amount': sale_amount
            })

        output_df = pd.DataFrame(all_rows)

        # Save to individual CSV files for each offer
        output_file = os.path.join(output_dir, f'{sheet_name.lower()}_offer.csv')
        output_df.to_csv(output_file, index=False)

        print(f"Number of records processed for {sheet_name}: {len(output_df)}")
        print(f"Date range processed for {sheet_name}: {output_df['date'].min() if not output_df.empty else 'N/A'} to {output_df['date'].max() if not output_df.empty else 'N/A'}")