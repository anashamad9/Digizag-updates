import pandas as pd
from datetime import datetime, timedelta
import os
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import time
import shutil

# Parameters
days_back = 2
end_date = datetime.now().date()
start_date = end_date - timedelta(days=days_back - 1)

print(f"Current date: {end_date}, Start date (days_back={days_back}): {start_date}")

# Define directory paths relative to the script location
script_dir = os.path.dirname(os.path.abspath(__file__))
input_dir = os.path.join(script_dir, '..', 'input data')
output_dir = os.path.join(script_dir, '..', 'output data')

# Ensure output directory exists
os.makedirs(output_dir, exist_ok=True)
os.makedirs(input_dir, exist_ok=True)

# Set up Selenium WebDriver
chrome_options = Options()
chrome_options.add_argument("--headless")  # Run in background
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

try:
    # Navigate to the Google Sheet
    sheet_url = "https://docs.google.com/spreadsheets/d/1dECQVaN77nlgvSei8pPyU59Qfd9oMGILmfnK7AQI3fc/edit?gid=0#gid=0"
    driver.get(sheet_url)

    # Wait for the page to load
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "body")))

    # Simulate login if required (replace with your credentials)
    email = "your_email@gmail.com"  # Replace with your Google email
    password = "your_password"      # Replace with your Google password
    try:
        email_field = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "identifierId")))
        email_field.send_keys(email)
        driver.find_element(By.ID, "identifierNext").click()
        time.sleep(2)
        password_field = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.NAME, "password")))
        password_field.send_keys(password)
        driver.find_element(By.ID, "passwordNext").click()
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "body")))  # Wait for login
    except:
        print("Login might not be required or page structure changed.")

    # Navigate to download
    driver.get(sheet_url)  # Reload to ensure logged-in state
    WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//div[contains(text(), 'File')]"))).click()
    WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//div[contains(text(), 'Download')]"))).click()
    WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//div[contains(text(), 'Microsoft Excel (.xlsx)')]"))).click()

    # Wait for download to complete
    time.sleep(10)  # Adjust based on download speed

    # Move the downloaded file (assuming default Downloads folder)
    downloaded_file = f"/Users/digizagoperation/Downloads/DigiZag X 6thStreet Performance Tracker.xlsx"
    output_excel = os.path.join(input_dir, f'DigiZag_X_6thStreet_Performance_Tracker_{end_date}.xlsx')
    if os.path.exists(downloaded_file):
        shutil.move(downloaded_file, output_excel)
    else:
        raise FileNotFoundError(f"Downloaded file not found at {downloaded_file}. Check download path or permissions.")

except Exception as e:
    print(f"Error during Selenium automation: {e}")
    driver.quit()
    raise

driver.quit()

# Load the saved Excel file
df = pd.read_excel(output_excel)

# Convert 'order_date' column to datetime
df['order_date'] = pd.to_datetime(df['order_date'])

# Filter for sales from the previous 3 days including today, excluding canceled orders
df_filtered = df[
    (df['order_date'].dt.date >= start_date) &
    (df['order_date'].dt.date <= end_date) &
    (df['status'] != 'canceled')
]

# Expand rows based on qty
expanded_rows = []
for _, row in df_filtered.iterrows():
    qty = int(row['qty'])  # Number of orders in the row
    sale_amount_per_order = row['gmv'] / qty  # Divide total gmv by qty
    for _ in range(qty):  # Duplicate row qty times
        expanded_rows.append({
            'order_date': row['order_date'],
            'country': row['country'],
            'user_type': row['user_type'],
            'sale_amount': sale_amount_per_order,
            'discount_code': row['discount_code']
        })

df_expanded = pd.DataFrame(expanded_rows)

# Convert sale amount to USD and calculate revenue
df_expanded['sale_amount_usd'] = df_expanded['sale_amount'] / 3.67
df_expanded['revenue'] = df_expanded.apply(
    lambda row: row['sale_amount_usd'] * 0.10 if row['user_type'] == 'FTU' else row['sale_amount_usd'] * 0.05,
    axis=1
)

# Sort by user_type rank
df_expanded['user_type_rank'] = df_expanded['user_type'].map({'FTU': 0, 'Repeat': 1})
df_expanded = df_expanded.sort_values(by='user_type_rank')
df_expanded = df_expanded.drop(columns=['user_type_rank'])

# Prepare output dataframe
output_df = pd.DataFrame({
    'offer': 1325,
    'date': df_expanded['order_date'].dt.strftime('%m-%d-%Y'),
    'revenue': df_expanded['revenue'].round(2),
    'sale_amount': df_expanded['sale_amount_usd'].round(2),
    'coupon_code': df_expanded['discount_code'],
    'geo': df_expanded['country'],
})

# Save to CSV in the output data folder
output_file = os.path.join(output_dir, '6th.csv')
output_df.to_csv(output_file, index=False)

print(f"Number of records processed: {len(output_df)}")
print(f"Date range processed: {output_df['date'].min() if not output_df.empty else 'N/A'} to {output_df['date'].max() if not output_df.empty else 'N/A'}")