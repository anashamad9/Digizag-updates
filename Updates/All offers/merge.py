import csv
import os
from datetime import datetime

# Combine all CSV files from Output Data folder
output_folder = "Updates/Output Data"
combined_data = []
header = None

for filename in os.listdir(output_folder):
    if filename.endswith(".csv"):
        file_path = os.path.join(output_folder, filename)
        with open(file_path, 'r', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            if not header:
                header = reader.fieldnames
            for row in reader:
                combined_data.append(row)

# Write combined data to a new CSV file
output_file = "combined_output.csv"
with open(output_file, 'w', newline='') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=header)
    writer.writeheader()
    writer.writerows(combined_data)

# Read admin view offer last update
admin_file = "Updates/Admin view_Offer last Update_Table (1).csv"
last_updates = {}
with open(admin_file, 'r', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        offer_id = row['Offer ID']
        last_update = datetime.strptime(row['Last Update'], '%b %d, %Y')
        last_updates[offer_id] = last_update

# Filter combined data based on last update dates
filtered_data = []
with open(output_file, 'r', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        offer_id = row['offer']
        row_date = datetime.strptime(row['date'], '%m-%d-%Y')
        if offer_id in last_updates and row_date > last_updates[offer_id]:
            filtered_data.append(row)

# Write filtered data to a new CSV file
filtered_output_file = "filtered_output.csv"
with open(filtered_output_file, 'w', newline='') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=header)
    writer.writeheader()
    writer.writerows(filtered_data)