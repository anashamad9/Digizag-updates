import pandas as pd
import os

# Define the folder path
folder_path = "/Users/digizagoperation/Desktop/Digizag/Updates/Output Data"

# Read the last update dates
update_df = pd.read_csv(os.path.join(folder_path, "Admin view_Offer last Update_Table.csv"))
update_dict = dict(zip(update_df["Offer ID"], update_df["Last Update"]))

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
            # Filter rows based on the day after the last update date
            df_filtered = df[df.apply(lambda row: pd.to_datetime(update_dict.get(row['offer'], '01-01-2025'), format='%b %d, %Y') + pd.Timedelta(days=1) <= row[date_col] or row['offer'] in [1183, 1282, 910, 1166, 1189], axis=1)]
            all_dataframes.append(df_filtered)

# Concatenate all dataframes
combined_df = pd.concat(all_dataframes, ignore_index=True)

# Save to a new CSV file
combined_df.to_csv("combined_output.csv", index=False)