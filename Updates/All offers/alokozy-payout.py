#!/usr/bin/env python3
"""Generate Alokozy payout CSV from aggregated coupon totals."""
from __future__ import annotations

import os
from datetime import datetime, timedelta

import numpy as np
import pandas as pd


OFFER_ID = 1327
GEO = "ksa"
REVENUE_RATE = 0.10          # 10% of sale amount
CURRENCY_DIVISOR = 3.75       # Convert SAR totals into target currency
DEFAULT_DAYS_BACK = 190       # Limit optional historical window
STATUS_DEFAULT = "pending"
DEFAULT_AFFILIATE_ID = "1"
INPUT_RESOURCE = "Digizag_Untitled Page_Table (1).csv"
OUTPUT_FILENAME = "alokozy.csv"
DATE_FORMAT = "%m-%d-%Y"

ENV_SOURCE = os.getenv("ALOKOZY_SOURCE")
ENV_OUTPUT = os.getenv("ALOKOZY_OUTPUT")
ENV_DAYS_BACK = os.getenv("ALOKOZY_DAYS_BACK")


def resolve_days_back(value: str | None) -> int | None:
    if value is None:
        return DEFAULT_DAYS_BACK
    value = value.strip()
    if not value:
        return None
    try:
        parsed = int(value)
    except ValueError:
        return DEFAULT_DAYS_BACK
    if parsed < 0:
        return None
    return parsed


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


def main() -> None:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    input_dir = os.path.join(script_dir, '..', 'input data')
    output_dir = os.path.join(script_dir, '..', 'output data')
    os.makedirs(output_dir, exist_ok=True)

    source_name = ENV_SOURCE or INPUT_RESOURCE
    output_name = ENV_OUTPUT or OUTPUT_FILENAME

    source_path = os.path.join(input_dir, source_name)
    if not os.path.exists(source_path):
        raise FileNotFoundError(f"Source file not found: {source_path}")

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
    expanded['affiliate_id'] = DEFAULT_AFFILIATE_ID
    expanded['status'] = STATUS_DEFAULT
    expanded['geo'] = GEO
    expanded['sale_amount'] = expanded['sale_amount'].round(2)
    expanded['revenue'] = expanded['revenue'].round(2)
    expanded['payout'] = expanded['revenue']

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
