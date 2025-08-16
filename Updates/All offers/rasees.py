import pandas as pd
from datetime import datetime, timedelta

# Parameters
days_back = 2
end_date = datetime(2025, 7, 30).date()  # Today's date
start_date = end_date - timedelta(days=days_back - 1)

# Define input file path
input_file = "Degi Zag - Daily Sales Report  (1).xlsx"  # Adjust this path if the file is in a different location

# Load the Rasees sheet from the Excel file
df = pd.read_excel(input_file, sheet_name='Rasees')

# Clean and convert 'Created_at' column
def excel_to_datetime(excel_date):
    try:
        if isinstance(excel_date, (int, float)):
            return pd.to_datetime('1899-12-30') + pd.to_timedelta(excel_date, 'D')
        return pd.to_datetime(excel_date, format='%Y-%m-%d %I:%M %p', errors='coerce')
    except ValueError:
        return pd.NaT

df['Created_at'] = df['Created_at'].apply(excel_to_datetime)

# Filter for date range and drop invalid dates
df = df.dropna(subset=['Created_at'])
df['Created_at'] = pd.to_datetime(df['Created_at']).dt.date
df = df[(df['Created_at'] >= start_date) & (df['Created_at'] <= end_date)]

# Calculate sale amount and revenue
df['sale_amount'] = df['Amount'] / 3.75
df['revenue'] = df['sale_amount'] * 0.05

# Set geo and format date
df['geo'] = 'ksa'
df['date'] = pd.to_datetime(df['Created_at']).strftime('%m/%d/%Y')

# Create output dataframe
output_df = pd.DataFrame({
    'offer': 'rasees',
    'date': df['date'],
    'revenue': df['revenue'].round(2),
    'sale_amount': df['sale_amount'].round(2),
    'coupon_code': df['Coupon'],
    'geo': df['geo']
})

# Save to CSV
output_df.to_csv('rasees_output.csv', index=False)

print(f"Processed {len(output_df)} records for dates {start_date} to {end_date}")