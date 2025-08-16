import pandas as pd
import os
from datetime import datetime, timedelta

# Define the folder path
folder_path = "/Users/digizagoperation/Desktop/Digizag/Updates/Output Data"

# Read the last update dates
update_df = pd.read_csv(os.path.join(folder_path, "Admin view_Performance Overview_Table (29).csv"))
print("update_df columns:", update_df.columns.tolist())
print("update_df sample:", update_df[['Offer id', 'Last Update']].head())
update_dict = dict(zip(update_df["Offer id"], update_df["Last Update"]))
print("update_dict:", update_dict)

# Set the current date and previous day
current_date = datetime.now()
prev_day = current_date - timedelta(days=1)
print(f"Current date: {current_date}, Previous day: {prev_day}")

# List to hold all dataframes
all_dataframes = []

# Iterate through all files in the folder
for filename in os.listdir(folder_path):
    if filename.endswith(".csv"):
        file_path = os.path.join(folder_path, filename)
        print(f"Processing file: {filename}")
        df = pd.read_csv(file_path)
        print(f"Columns in {filename}: {df.columns.tolist()}")
        
        # Check for case-insensitive column name 'date'
        date_col = next((col for col in df.columns if col.lower() == 'date'), None)
        if date_col:
            print(f"Date column found: {date_col}")
            # Parse dates with mixed format and handle errors
            df[date_col] = pd.to_datetime(df[date_col], format='mixed', errors='coerce')
            print(f"Sample dates in {filename}: {df[date_col].head().tolist()}")
            
            # Ensure 'offer' column exists
            if 'offer' not in df.columns:
                print(f"Warning: 'offer' column not found in {filename}")
                continue

            # Filter rows
            df_filtered = df[df.apply(lambda row: (
                (pd.to_datetime(update_dict.get(row['offer'], '01-01-2025'), format='%b %d, %Y', errors='coerce') + pd.Timedelta(days=1) <= row[date_col] <= prev_day)
                or (row['offer'] in [1183, 1282, 910, 1166, 1189] and pd.to_datetime(update_dict.get(row['offer'], '01-01-2025'), format='%b %d, %Y', errors='coerce') <= row[date_col] <= current_date)
            ), axis=1)].copy()
            
            print(f"Rows in df_filtered for {filename}: {len(df_filtered)}")
            print(f"Offer IDs in df_filtered: {df_filtered['offer'].unique().tolist()}")
            
            # Map geo column values
            geo_col = next((col for col in df_filtered.columns if col.lower() == 'geo'), 'geo')
            if geo_col in df_filtered.columns:
                df_filtered.loc[:, geo_col] = df_filtered[geo_col].replace({
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
            
            if not df_filtered.empty:
                all_dataframes.append(df_filtered)

# Concatenate all dataframes
if all_dataframes:
    combined_df = pd.concat(all_dataframes, ignore_index=True)
    print(f"Total rows in combined_df: {len(combined_df)}")
    print(f"Offer IDs in combined_df: {combined_df['offer'].unique().tolist()}")
    
    # Convert date column to mm/dd/yyyy format before saving
    if 'date' in combined_df.columns:
        combined_df['date'] = pd.to_datetime(combined_df['date']).dt.strftime('%m/%d/%Y')
    
    # Save to a new CSV file
    combined_df.to_csv(os.path.join(folder_path, "finaaaaaaaaaaaaaaaaal.csv"), index=False)
    print("Output saved to final.csv")
else:
    print("No dataframes to concatenate. Check filtering conditions or input data.")