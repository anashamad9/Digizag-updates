import os
import pandas as pd

# =======================
# CONFIG
# =======================
WORKBOOK_NAME = "Offers Coupons.xlsx"
OUTPUT_NAME = "finaaaaaaaaaaaaaaaaal_coupon_mismatch.csv"


def _to_numeric_payout(value):
    """Return float payout value (percent or fixed) or pd.NA when blank/non-numeric."""
    if pd.isna(value):
        return pd.NA
    s = str(value).strip()
    if not s:
        return pd.NA
    s = s.replace('%', '')
    try:
        return float(s)
    except ValueError:
        return pd.NA


def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    input_dir = os.path.join(script_dir, 'Input data')
    output_dir = os.path.join(script_dir, 'Output Data')
    os.makedirs(output_dir, exist_ok=True)

    workbook_path = os.path.join(input_dir, WORKBOOK_NAME)
    if not os.path.exists(workbook_path):
        raise FileNotFoundError(f"Workbook not found: {workbook_path}")

    xl = pd.ExcelFile(workbook_path)
    mismatch_rows = []

    for sheet in xl.sheet_names:
        df = xl.parse(sheet)
        if df.empty:
            continue

        cols_lookup = {str(c).strip().lower(): c for c in df.columns}

        code_col = cols_lookup.get('code') or cols_lookup.get('coupon')
        new_col = cols_lookup.get('new customer payout')
        old_col = cols_lookup.get('old customer payout')

        if not (code_col and new_col and old_col):
            # Skip sheets that do not follow the payout structure
            continue

        working = df[[code_col, new_col, old_col]].copy()
        working.columns = ['code', 'new_customer_raw', 'old_customer_raw']

        working['code'] = working['code'].astype(str).str.strip()
        working['new_customer_val'] = working['new_customer_raw'].apply(_to_numeric_payout)
        working['old_customer_val'] = working['old_customer_raw'].apply(_to_numeric_payout)

        def is_mismatch(row):
            new_val = row['new_customer_val']
            old_val = row['old_customer_val']

            if pd.isna(new_val) and pd.isna(old_val):
                return False
            if pd.isna(new_val) != pd.isna(old_val):
                return True
            return abs(new_val - old_val) > 1e-6

        flagged = working[working.apply(is_mismatch, axis=1)].copy()
        if flagged.empty:
            continue

        flagged['sheet'] = sheet
        flagged['difference'] = flagged.apply(
            lambda r: (r['new_customer_val'] - r['old_customer_val'])
            if pd.notna(r['new_customer_val']) and pd.notna(r['old_customer_val'])
            else pd.NA,
            axis=1
        )
        flagged['flag'] = 'DIFFERENT'

        mismatch_rows.append(flagged[
            ['sheet', 'code', 'new_customer_raw', 'old_customer_raw',
             'new_customer_val', 'old_customer_val', 'difference', 'flag']
        ])

    if mismatch_rows:
        result = pd.concat(mismatch_rows, ignore_index=True)
        out_path = os.path.join(output_dir, OUTPUT_NAME)
        result.to_csv(out_path, index=False)
        print(f"Saved coupon mismatch report: {out_path}")
        print(f"Total mismatched rows: {len(result)} across {result['sheet'].nunique()} sheets")
    else:
        print("No mismatched payouts detected across sheets.")


if __name__ == "__main__":
    main()

