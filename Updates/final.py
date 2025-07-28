import pandas as pd
import os
from datetime import datetime, timedelta

# Define the folder path
folder_path = "/Users/digizagoperation/Desktop/Digizag/Updates/Output Data"

# Read the last update dates
update_df = pd.read_csv(os.path.join(folder_path, "Admin view_Performance Overview_Table.csv"))
update_dict = dict(zip(update_df["Offer id"], update_df["Last Update"]))

# Set the current date and previous day
current_date = datetime.now()
prev_day = current_date - timedelta(days=1)

# List to hold all dataframes
all_dataframes = []

# Iterate through all files in the folder
for filename in os.listdir(folder_path):
    if filename.endswith(".csv"):
        file_path = os.path.join(folder_path, filename)
        df = pd.read_csv(file_path)
        # Check for case-insensitive column name 'date'
        date_col = next((col for col in df.columns if col.lower() == 'date'), None)
        if date_col:
            df[date_col] = pd.to_datetime(df[date_col], format='%m-%d-%Y')
            # Filter rows based on the day after the last update date and max date
            df_filtered = df[df.apply(lambda row: (
                (pd.to_datetime(update_dict.get(row['offer'], '01-01-2025'), format='%b %d, %Y') + pd.Timedelta(days=1) <= row[date_col] <= prev_day)
                or (row['offer'] in [1183, 1282, 910, 1166, 1189] and pd.to_datetime(update_dict.get(row['offer'], '01-01-2025'), format='%b %d, %Y') <= row[date_col] <= current_date)
            ), axis=1)]
            # Map geo column values
            geo_col = next((col for col in df_filtered.columns if col.lower() == 'geo'), 'geo')
            df_filtered[geo_col] = df_filtered[geo_col].replace({
                'AE': 'uae',
                'sa': 'ksa',
                'SA': 'ksa',
                'SAU': 'ksa',
                'bah': 'bhr',
                'RoGCC': 'no-geo',
                'KW': 'kwt',
                'OM': 'omn',
                'Oman': 'omn',
                'qat': 'qtr',
                'QA': 'qtr',
                'Bahrain': 'bhr',
                'kuwait': 'kwt',
            }).fillna('null')
            all_dataframes.append(df_filtered)

# Concatenate all dataframes
combined_df = pd.concat(all_dataframes, ignore_index=True)

# Convert date column to mm/dd/yyyy format before saving
if 'date' in combined_df.columns:
    combined_df['date'] = pd.to_datetime(combined_df['date']).dt.strftime('%m/%d/%Y')

# Save to a new CSV file
combined_df.to_csv("final.csv", index=False)