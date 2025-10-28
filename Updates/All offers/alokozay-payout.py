#!/usr/bin/env python3
"""Generate Alokozay payout CSV from aggregated coupon totals."""
from __future__ import annotations

import os
import re
from datetime import datetime, timedelta
from typing import Optional

import numpy as np
import pandas as pd


OFFER_ID = 1327
GEO = "ksa"
REVENUE_RATE = 0.10
CURRENCY_DIVISOR = 3.75
DEFAULT_DAYS_BACK = 42
STATUS_DEFAULT = "pending"
DEFAULT_AFFILIATE_ID = "1"
INPUT_BASE_NAME = "Digizag_Untitled Page_Table"
INPUT_SUFFIX = ".csv"
COUPON_WORKBOOK = "Offers Coupons.xlsx"
COUPON_SHEET = "AloKozay"
OUTPUT_FILENAME = "alokozay.csv"
DATE_FORMAT = "%m-%d-%Y"

ENV_SOURCE = os.getenv("ALOKOZAY_SOURCE")
ENV_OUTPUT = os.getenv("ALOKOZAY_OUTPUT")
ENV_DAYS_BACK = os.getenv("ALOKOZAY_DAYS_BACK")

TYPE_ALIAS_MAP = {
    "revenue": "revenue",
    "rev": "revenue",
    "revenueshare": "revenue",
    "revshare": "revenue",
    "sales": "sale",
    "sale": "sale",
    "gmv": "sale",
    "order": "sale",
    "orders": "sale",
    "fixed": "fixed",
    "flat": "fixed",
    "amount": "fixed",
}


def resolve_days_back(value: str | None) -> Optional[int]:
    if value is None:
        return DEFAULT_DAYS_BACK
    value = value.strip()
    if not value:
        return None
    try:
        parsed = int(value)
    except ValueError:
        return DEFAULT_DAYS_BACK
    return None if parsed < 0 else parsed


def normalize_coupon(code: object) -> str:
    if pd.isna(code):
        return ""
    text = str(code).strip().upper()
    parts = re.split(r"[;,\s]+", text)
    return parts[0] if parts else text



def normalize_type_label(value: object) -> str:
    if pd.isna(value):
        return "revenue"
    text = str(value).strip().lower()
    if not text:
        return "revenue"
    key = re.sub(r"[^a-z]+", "", text)
    return TYPE_ALIAS_MAP.get(key, "revenue")


def parse_numeric_series(series: pd.Series) -> pd.Series:
    cleaned = (
        series.astype(str)
        .str.replace('%', '', regex=False)
        .str.replace(',', '', regex=False)
        .str.strip()
    )
    cleaned = cleaned.replace({'': np.nan, 'nan': np.nan, 'none': np.nan, '-': np.nan})
    return pd.to_numeric(cleaned, errors='coerce')


def normalize_affiliate_id(value: object) -> str:
    if pd.isna(value):
        return DEFAULT_AFFILIATE_ID
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none"}:
        return DEFAULT_AFFILIATE_ID
    if re.fullmatch(r"\d+(\.0+)?", text):
        try:
            return str(int(float(text)))
        except ValueError:
            pass
    return text


def determine_source_path(input_dir: str, override: str | None) -> str:
    if override:
        override_path = override
        if not os.path.isabs(override_path):
            override_path = os.path.join(input_dir, override_path)
        if not os.path.exists(override_path):
            raise FileNotFoundError(f"Override source file not found: {override}")
        return override_path

    exact = os.path.join(input_dir, f"{INPUT_BASE_NAME}{INPUT_SUFFIX}")
    if os.path.exists(exact):
        return exact

    pattern = re.compile(
        rf"^{re.escape(INPUT_BASE_NAME)}(?:\s*\(\d+\))?{re.escape(INPUT_SUFFIX)}$"
    )
    candidates = [
        name for name in os.listdir(input_dir) if pattern.match(name)
    ]
    if not candidates:
        raise FileNotFoundError(
            f"No input file found matching '{INPUT_BASE_NAME}*.csv' in {input_dir}"
        )
    latest = max(candidates, key=lambda name: os.path.getmtime(os.path.join(input_dir, name)))
    return os.path.join(input_dir, latest)


def load_source_dataframe(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    df['created_at'] = pd.to_datetime(df['created_at'], format='%b %d, %Y', errors='coerce')
    df['Record Count'] = pd.to_numeric(df['Record Count'], errors='coerce').fillna(0).astype(int)
    df['grand_total'] = pd.to_numeric(df['grand_total'], errors='coerce')
    df['coupon_code'] = df['coupon_code'].astype(str).str.strip().str.upper()

    df = df.dropna(subset=['created_at', 'grand_total'])
    df = df[df['Record Count'] > 0]
    return df


def expand_orders(df: pd.DataFrame) -> pd.DataFrame:
    sale_total_converted = df['grand_total'] / CURRENCY_DIVISOR
    sale_each = sale_total_converted / df['Record Count']
    revenue_each = sale_each * REVENUE_RATE

    repeat_counts = df['Record Count'].to_numpy()
    expanded = df.loc[df.index.repeat(repeat_counts)].copy()
    expanded['date'] = expanded['created_at'].dt.strftime(DATE_FORMAT)
    expanded['sale_amount'] = np.repeat(sale_each.to_numpy(), repeat_counts)
    expanded['revenue'] = np.repeat(revenue_each.to_numpy(), repeat_counts)
    return expanded[['date', 'coupon_code', 'sale_amount', 'revenue']]


def load_coupon_affiliates(path: str, sheet_name: str) -> pd.DataFrame:
    columns = ['coupon_norm', 'affiliate_id', 'type_norm', 'pct_value', 'fixed_value']
    if not os.path.exists(path):
        return pd.DataFrame(columns=columns)

    df_sheet = pd.read_excel(path, sheet_name=sheet_name, dtype=str)
    if df_sheet.empty:
        return pd.DataFrame(columns=columns)

    def find_column(candidates: list[str]) -> Optional[str]:
        normalized = {str(col).strip().lower(): col for col in df_sheet.columns}
        for cand in candidates:
            label = cand.strip().lower()
            if label in normalized:
                return normalized[label]
        for col in df_sheet.columns:
            cleaned = re.sub(r"[^a-z0-9]+", "", str(col).lower())
            if cleaned in {re.sub(r"[^a-z0-9]+", "", c.lower()) for c in candidates}:
                return col
        return None

    code_col = find_column(['code', 'coupon code', 'coupon'])
    id_col = find_column(['id', 'affiliate id', 'affiliate_id'])
    type_col = find_column(['type', 'payout type'])
    payout_col = find_column(['payout', 'payout value', 'value', 'rate'])

    if not code_col:
        return pd.DataFrame(columns=columns)

    base = df_sheet[[code_col]].copy()
    base['coupon_norm'] = base[code_col].apply(normalize_coupon)
    base['affiliate_id'] = (
        df_sheet[id_col].fillna("").astype(str).str.strip() if id_col else ""
    )
    base['type_norm'] = (
        df_sheet[type_col].apply(normalize_type_label) if type_col else "revenue"
    )

    if payout_col:
        payout_numeric = parse_numeric_series(df_sheet[payout_col])
    else:
        payout_numeric = pd.Series(np.nan, index=df_sheet.index, dtype=float)

    new_col = find_column(['new customer payout', 'new payout', 'new customer'])
    old_col = find_column(['old customer payout', 'old payout', 'old customer'])
    payout_new = parse_numeric_series(df_sheet[new_col]) if new_col else pd.Series(np.nan, index=df_sheet.index, dtype=float)
    payout_old = parse_numeric_series(df_sheet[old_col]) if old_col else pd.Series(np.nan, index=df_sheet.index, dtype=float)

    combined = payout_numeric
    combined = combined.combine_first(payout_new)
    combined = combined.combine_first(payout_old)

    pct_mask = base['type_norm'].isin(['revenue', 'sale'])
    pct_values = combined.where(pct_mask)
    pct_values = pct_values.apply(lambda v: (v / 100.0) if pd.notna(v) and v > 1 else v)
    fixed_values = combined.where(base['type_norm'].eq('fixed'), np.nan)

    mapping = base.assign(
        pct_value=pct_values,
        fixed_value=fixed_values,
    )

    mapping = mapping[mapping['coupon_norm'].str.len() > 0]
    return mapping[['coupon_norm', 'affiliate_id', 'type_norm', 'pct_value', 'fixed_value']].drop_duplicates(
        subset='coupon_norm',
        keep='last',
    )


def main() -> None:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    input_dir = os.path.join(script_dir, '..', 'input data')
    output_dir = os.path.join(script_dir, '..', 'output data')
    os.makedirs(output_dir, exist_ok=True)

    source_path = determine_source_path(input_dir, ENV_SOURCE)
    df = load_source_dataframe(source_path)

    days_back = resolve_days_back(ENV_DAYS_BACK)
    if days_back is not None:
        cutoff_date = datetime.now().date() - timedelta(days=days_back)
        df = df[df['created_at'].dt.date >= cutoff_date]

    if df.empty:
        print("No qualifying rows after filtering; nothing to write.")
        return

    expanded = expand_orders(df).reset_index(drop=True)
    expanded['offer'] = OFFER_ID

    coupons_path = os.path.join(input_dir, COUPON_WORKBOOK)
    coupon_mapping = load_coupon_affiliates(coupons_path, COUPON_SHEET)
    if not coupon_mapping.empty:
        expanded['coupon_norm'] = expanded['coupon_code'].astype(str).str.strip().str.upper()
        expanded = expanded.merge(coupon_mapping, on='coupon_norm', how='left')
        expanded.drop(columns=['coupon_norm'], inplace=True)
    if 'affiliate_id' not in expanded.columns:
        expanded['affiliate_id'] = DEFAULT_AFFILIATE_ID
    expanded['affiliate_id'] = expanded['affiliate_id'].apply(normalize_affiliate_id)
    if 'type_norm' not in expanded.columns:
        expanded['type_norm'] = 'revenue'
    expanded['type_norm'] = expanded['type_norm'].apply(normalize_type_label)
    if 'pct_value' not in expanded.columns:
        expanded['pct_value'] = np.nan
    expanded['pct_value'] = pd.to_numeric(expanded['pct_value'], errors='coerce')
    if 'fixed_value' not in expanded.columns:
        expanded['fixed_value'] = np.nan
    expanded['fixed_value'] = pd.to_numeric(expanded['fixed_value'], errors='coerce')
    expanded['status'] = STATUS_DEFAULT
    expanded['geo'] = GEO
    expanded['sale_amount'] = expanded['sale_amount'].round(2)
    expanded['revenue'] = expanded['revenue'].round(2)
    payout_series = expanded['revenue'].copy()
    mask_revenue = expanded['type_norm'].eq('revenue')
    mask_sale = expanded['type_norm'].eq('sale')
    mask_fixed = expanded['type_norm'].eq('fixed')
    payout_series.loc[mask_revenue] = (
        expanded.loc[mask_revenue, 'revenue']
        * expanded.loc[mask_revenue, 'pct_value'].fillna(REVENUE_RATE)
    )
    payout_series.loc[mask_sale] = (
        expanded.loc[mask_sale, 'sale_amount']
        * expanded.loc[mask_sale, 'pct_value'].fillna(REVENUE_RATE)
    )
    payout_series.loc[mask_fixed] = expanded.loc[mask_fixed, 'fixed_value'].fillna(
        expanded.loc[mask_fixed, 'revenue']
    )
    expanded['payout'] = payout_series.round(2)

    output_df = expanded[
        [
            'offer',
            'affiliate_id',
            'date',
            'status',
            'payout',
            'revenue',
            'sale_amount',
            'coupon_code',
            'geo',
        ]
    ].rename(columns={'sale_amount': 'sale amount', 'coupon_code': 'coupon'})

    output_name = ENV_OUTPUT or OUTPUT_FILENAME
    output_path = os.path.join(output_dir, output_name)
    output_df.to_csv(output_path, index=False)

    print(
        "Generated {rows} rows from {records} aggregated entries. Date range: {start} to {end}.".format(
            rows=len(output_df),
            records=len(df),
            start=output_df['date'].min(),
            end=output_df['date'].max(),
        )
    )


if __name__ == "__main__":
    main()
