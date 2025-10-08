import pandas as pd
import os
from datetime import datetime, timedelta

# ====== CONFIG ======
folder_path = "/Users/digizagoperation/Desktop/Digizag/Updates/Output Data"
ADMIN_CSV   = "Partnership Teams View_Performance Overview_Table (14).csv"
FINAL_CSV   = "finaaaaaaaaaaaaaaaaal.csv"
FINAL_XLSX  = "finaaaaaaaaaaaaaaaaal.xlsx"   # NEW: styled Excel with red alerts

# Offers allowed to include up to current_date (exceptions from your old logic)
EXCEPTION_OFFERS = {1183, 1282, 910, 1166, 1189}

# ====== LOAD LAST UPDATE MAP ======
admin_path = os.path.join(folder_path, ADMIN_CSV)
update_df = pd.read_csv(admin_path)
update_dict = dict(zip(update_df["Offer id"], update_df["Last Update"]))

# ====== DATES ======
current_date = datetime.now()
prev_day = current_date - timedelta(days=1)

# ====== HELPERS ======
def to_datetime_mixed(s):
    """Robust mixed-format datetime parser -> datetime (NaT on failure)."""
    return pd.to_datetime(s, format='mixed', errors='coerce')

def coalesce_cols(df, candidates, new_name):
    """If any column in candidates exists, rename first match to new_name."""
    for c in candidates:
        if c in df.columns:
            if c != new_name:
                df.rename(columns={c: new_name}, inplace=True)
            return True
    return False

def standardize_columns(df):
    """
    Map old variants to the new unified schema without dropping extra columns.
    Returns df with at least:
    offer, affiliate_id, date, status, payout, revenue, sale amount, coupon, geo
    """
    # Normalize column names (strip and keep case)
    df.columns = [c.strip() for c in df.columns]

    # Coalesce likely variants
    coalesce_cols(df, ['coupon', 'coupon_code', 'Coupon', 'Coupon Code'], 'coupon')
    coalesce_cols(df, ['sale amount', 'sale_amount', 'Sale Amount', 'sale_amount_usd'], 'sale amount')
    coalesce_cols(df, ['affiliate_id', 'affiliate_ID', 'Affiliate_ID', 'Affiliate Id'], 'affiliate_id')
    coalesce_cols(df, ['status', 'Status'], 'status')
    coalesce_cols(df, ['payout', 'Payout'], 'payout')
    coalesce_cols(df, ['revenue', 'Revenue'], 'revenue')
    coalesce_cols(df, ['geo', 'Geo', 'country'], 'geo')
    coalesce_cols(df, ['offer', 'Offer', 'Offer id', 'Offer ID'], 'offer')
    coalesce_cols(df, ['date', 'Date'], 'date')

    # Ensure required columns exist; fill sensible defaults if missing
    if 'affiliate_id' not in df.columns:
        df['affiliate_id'] = '1'  # fallback
    if 'status' not in df.columns:
        df['status'] = 'pending'
    if 'payout' not in df.columns:
        df['payout'] = 0.0
    if 'revenue' not in df.columns:
        df['revenue'] = 0.0
    if 'sale amount' not in df.columns:
        df['sale amount'] = 0.0
    if 'coupon' not in df.columns:
        df['coupon'] = ''
    if 'geo' not in df.columns:
        df['geo'] = 'no-geo'

    # Types
    if 'offer' in df.columns:
        df['offer'] = pd.to_numeric(df['offer'], errors='coerce').astype('Int64')
    df['payout']      = pd.to_numeric(df['payout'], errors='coerce').fillna(0.0)
    df['revenue']     = pd.to_numeric(df['revenue'], errors='coerce').fillna(0.0)
    df['sale amount'] = pd.to_numeric(df['sale amount'], errors='coerce').fillna(0.0)

    return df

def should_keep_row(row_date, offer_id):
    """
    Apply date filtering based on Last Update.
    - Default: keep rows where (last_update + 1 day) <= row_date <= prev_day
    - Exceptions: offers in EXCEPTION_OFFERS use <= current_date instead of <= prev_day.
    """
    last_update_raw = update_dict.get(offer_id, '01-01-2025')
    last_update_dt = pd.to_datetime(last_update_raw, format='%b %d, %Y', errors='coerce')

    if pd.isna(row_date) or pd.isna(last_update_dt) or offer_id is None:
        return False

    if offer_id in EXCEPTION_OFFERS:
        return (row_date >= (last_update_dt)) and (row_date <= current_date)
    else:
        return (row_date >= (last_update_dt + pd.Timedelta(days=1))) and (row_date <= prev_day)

# ====== GATHER FILES ======
all_dataframes = []
skip_files = {ADMIN_CSV, FINAL_CSV, FINAL_XLSX}

for filename in os.listdir(folder_path):
    if not filename.endswith(".csv"):
        continue
    if filename in skip_files:
        continue
    file_path = os.path.join(folder_path, filename)

    df = pd.read_csv(file_path)
    df = standardize_columns(df)

    # Require at minimum these columns
    required = ['offer', 'date']
    if not all(c in df.columns for c in required):
        continue

    # Parse the date column robustly
    df['date'] = to_datetime_mixed(df['date'])

    # Filter by last update logic
    df_filtered = df[df.apply(
        lambda r: should_keep_row(r['date'], int(r['offer']) if pd.notna(r['offer']) else None),
        axis=1
    )].copy()

    if df_filtered.empty:
        continue

    # Geo normalization
    df_filtered['geo'] = df_filtered['geo'].replace({
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

    # Keep only unified columns in order
    unified_cols = ['offer', 'affiliate_id', 'date', 'status', 'payout', 'revenue', 'sale amount', 'coupon', 'geo']
    df_filtered = df_filtered[unified_cols]

    all_dataframes.append(df_filtered)

# ====== CONCAT, ALERTS & SAVE ======
if all_dataframes:
    combined_df = pd.concat(all_dataframes, ignore_index=True)

    # detection alert: likely-unmatched payout if affiliate_id == "1" OR payout == 0 while there is value to pay
    aff_is_one = combined_df['affiliate_id'].astype(str).str.strip().eq("1")
    zero_payout_with_value = (combined_df['payout'].fillna(0) == 0) & (
        (combined_df['revenue'].fillna(0) > 0) | (combined_df['sale amount'].fillna(0) > 0)
    )
    combined_df['alert'] = (aff_is_one | zero_payout_with_value).map({True: 'UNMATCHED', False: ''})

    # Dates to mm/dd/YYYY
    combined_df['date'] = pd.to_datetime(combined_df['date'], errors='coerce').dt.strftime('%m/%d/%Y')

    # Save CSV (plain – no colors in CSV)
    out_csv = os.path.join(folder_path, FINAL_CSV)
    combined_df.to_csv(out_csv, index=False)

    # Save XLSX with red highlighting for alerts
    out_xlsx = os.path.join(folder_path, FINAL_XLSX)
    with pd.ExcelWriter(out_xlsx, engine="xlsxwriter") as writer:
        combined_df.to_excel(writer, index=False, sheet_name="final")
        ws = writer.sheets["final"]
        wb = writer.book

        # Find alert column index (1-based in Excel)
        alert_col_idx = combined_df.columns.get_loc('alert')  # 0-based
        n_rows, n_cols = combined_df.shape

        red_fmt = wb.add_format({
            'bg_color': '#FFC7CE',  # light red fill
            'font_color': '#9C0006'
        })

        # Apply conditional format: color entire row when alert == 'UNMATCHED'
        # Build Excel range A2:?? for data rows (exclude header)
        start_row = 1
        end_row = n_rows
        start_col_letter = 'A'
        end_col_letter = chr(ord('A') + n_cols - 1)

        # Condition on the alert cell in each row
        # Example formula for row r: =$J2="UNMATCHED" (adjust letter)
        alert_col_letter = chr(ord('A') + alert_col_idx)
        ws.conditional_format(
            f"{start_col_letter}{start_row+1}:{end_col_letter}{end_row+1}",
            {
                'type': 'formula',
                'criteria': f'=${alert_col_letter}{start_row+1}="UNMATCHED"',
                'format': red_fmt
            }
        )

    print(f"CSV saved -> {FINAL_CSV}")
    print(f"XLSX saved with red alerts -> {FINAL_XLSX}")
else:
    print("No dataframes to concatenate. Check filtering conditions or input data.")
