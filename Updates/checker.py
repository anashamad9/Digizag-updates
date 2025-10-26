import math
import os
import re
from typing import Optional

import numpy as np
import pandas as pd

# ========= CONFIG =========
# Affiliate (partner) ID to inspect; set to "all" to include every ID.
TARGET_AFFILIATE_ID = "all"

# Month to analyze (1-12). Use "all" to include every month.
TARGET_MONTH = 10

# Optional: restrict to a specific year. Set to None to include any year.
TARGET_YEAR: Optional[int] = None

# Allowed deviation (as a fraction) when comparing ratio-based payouts.
RATIO_TOLERANCE = 0.01  # 1%

# Absolute tolerance when comparing fixed payouts.
FIXED_PAYOUT_TOLERANCE = 0.5  # currency units

# Filenames
CHECKER_FILENAME = "checker.csv"
OUTPUT_ORDERS_FILENAME = "checker_results.csv"
OUTPUT_SUMMARY_FILENAME = "checker_results_summary.csv"


def as_float(value) -> float:
    """Best-effort conversion to float; returns NaN on failure."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return float("nan")


def format_method_label(raw: str) -> str:
    """Normalize aggregated method strings."""
    if not raw:
        return ""
    methods = [m for m in raw.split("|") if m]
    if not methods:
        return ""
    if len(methods) == 1:
        return methods[0]
    return "mixed (" + ", ".join(methods) + ")"


def extract_affiliate_id(partner_value: str) -> str:
    """Pull the leading numeric identifier from the Partner column."""
    if pd.isna(partner_value):
        return ""
    text = str(partner_value).strip()
    match = re.match(r"(\d+)", text)
    return match.group(1) if match else text


def load_checker_dataframe(path: str) -> pd.DataFrame:
    """Load the checker CSV and normalize essential columns."""
    if not os.path.exists(path):
        raise FileNotFoundError(f"Checker file not found: {path}")

    df = pd.read_csv(path)
    cleaned_cols = []
    for col in df.columns:
        stripped = str(col).strip()
        cleaned_cols.append(stripped if stripped else "record_id")
    df.columns = cleaned_cols
    if " " in df.columns:
        df.rename(columns={" ": "record_id"}, inplace=True)
    df["affiliate_id"] = df["Partner"].apply(extract_affiliate_id)
    df["date"] = pd.to_datetime(df["Date"], errors="coerce")

    for col in ["Payout", "Revenue", "Sale Amount"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    has_revenue = df["Revenue"] != 0
    df.loc[has_revenue, "ratio"] = df.loc[has_revenue, "Payout"] / df.loc[has_revenue, "Revenue"]
    df.loc[~has_revenue, "ratio"] = pd.NA
    return df


def build_ratio_baseline(df: pd.DataFrame) -> pd.DataFrame:
    """Compute typical payout behaviour per offer+code for revenue, sale, and fixed payouts."""
    records = []
    if df.empty:
        return pd.DataFrame(
            columns=[
                "Offer Name",
                "Code",
                "expected_ratio_revenue",
                "expected_ratio_sale",
                "expected_payout_fixed",
            ]
        )

    grouped = df.groupby(["Offer Name", "Code"], dropna=False)
    for (offer, code), group in grouped:
        revenue_rows = group[group["Revenue"] > 0]
        sale_rows = group[group["Sale Amount"] > 0]

        expected_ratio_revenue = (
            (revenue_rows["Payout"] / revenue_rows["Revenue"]).median()
            if not revenue_rows.empty
            else float("nan")
        )
        expected_ratio_sale = (
            (sale_rows["Payout"] / sale_rows["Sale Amount"]).median()
            if not sale_rows.empty
            else float("nan")
        )
        expected_payout_fixed = group["Payout"].median() if not group.empty else float("nan")

        records.append(
            {
                "Offer Name": offer,
                "Code": code,
                "expected_ratio_revenue": expected_ratio_revenue,
                "expected_ratio_sale": expected_ratio_sale,
                "expected_payout_fixed": expected_payout_fixed,
            }
        )

    return pd.DataFrame.from_records(records)


def filter_scope(df: pd.DataFrame) -> pd.DataFrame:
    """Filter records by affiliate id, month, and optional year."""
    mask = pd.Series(True, index=df.index)

    target_affiliate = str(TARGET_AFFILIATE_ID).strip().lower()
    if target_affiliate != "all":
        mask &= df["affiliate_id"].astype(str) == str(TARGET_AFFILIATE_ID)

    target_month = TARGET_MONTH
    if isinstance(target_month, str) and target_month.strip().lower() == "all":
        target_month = None
    if target_month is not None:
        mask &= df["date"].dt.month == int(target_month)

    if TARGET_YEAR is not None:
        mask &= df["date"].dt.year == int(TARGET_YEAR)
    scoped = df[mask].copy()
    return scoped


def summarize_codes(order_details: pd.DataFrame) -> pd.DataFrame:
    """Aggregate order-level records back to per-code totals for a quick overview."""
    if order_details.empty:
        return pd.DataFrame(
            columns=[
                "id",
                "offer name",
                "code",
                "payout",
                "revenue",
                "sale amount",
                "expected ratio revenue",
                "expected ratio sale",
                "expected payout fixed",
                "match method",
                "matched or not",
            ]
        )

    grouped = order_details.groupby(["id", "offer name", "code"], dropna=False)
    summary = grouped.agg(
        payout=("payout", "sum"),
        revenue=("revenue", "sum"),
        sale_amount=("sale amount", "sum"),
        expected_ratio_revenue=("expected ratio revenue", "first"),
        expected_ratio_sale=("expected ratio sale", "first"),
        expected_payout_fixed=("expected payout fixed", "first"),
        all_matched=("matched or not", lambda col: all(val == "matched" for val in col)),
        method_concat=("match method", lambda col: "|".join(sorted({m for m in col if m and m != "none"}))),
    ).reset_index()

    summary.rename(
        columns={
            "sale_amount": "sale amount",
            "expected_ratio_revenue": "expected ratio revenue",
            "expected_ratio_sale": "expected ratio sale",
            "expected_payout_fixed": "expected payout fixed",
        },
        inplace=True,
    )

    summary["matched or not"] = summary.pop("all_matched").map({True: "matched", False: "not matched"})
    summary["match method"] = summary.pop("method_concat").apply(format_method_label)

    summary = summary[
        [
            "id",
            "offer name",
            "code",
            "payout",
            "revenue",
            "sale amount",
            "expected ratio revenue",
            "expected ratio sale",
            "expected payout fixed",
            "match method",
            "matched or not",
        ]
    ]
    summary.sort_values(["offer name", "code"], inplace=True)
    summary.reset_index(drop=True, inplace=True)
    return summary


def summarize_orders(scoped: pd.DataFrame, baseline: pd.DataFrame) -> pd.DataFrame:
    """Return per-order payout validation with payout method recognition."""
    if scoped.empty:
        return pd.DataFrame(
            columns=[
                "id",
                "offer name",
                "code",
                "date",
                "order id",
                "partner",
                "geo",
                "payout",
                "revenue",
                "sale amount",
                "expected ratio revenue",
                "actual ratio revenue",
                "expected ratio sale",
                "actual ratio sale",
                "expected payout fixed",
                "match method",
                "matched or not",
            ]
        )

    detail = scoped.copy()
    detail = detail.merge(
        baseline,
        on=["Offer Name", "Code"],
        how="left",
        validate="many_to_one",
    )

    detail["actual_ratio_revenue"] = np.where(
        detail["Revenue"] > 0,
        detail["Payout"] / detail["Revenue"],
        np.nan,
    )
    detail["actual_ratio_revenue"] = detail["actual_ratio_revenue"].replace([np.inf, -np.inf], np.nan)

    detail["actual_ratio_sale"] = np.where(
        detail["Sale Amount"] > 0,
        detail["Payout"] / detail["Sale Amount"],
        np.nan,
    )
    detail["actual_ratio_sale"] = detail["actual_ratio_sale"].replace([np.inf, -np.inf], np.nan)

    def evaluate_match(row):
        payout = as_float(row["Payout"])
        revenue = as_float(row["Revenue"])
        sale_amount = as_float(row["Sale Amount"])

        actual_ratio_revenue = as_float(row["actual_ratio_revenue"])
        expected_ratio_revenue = as_float(row["expected_ratio_revenue"])

        actual_ratio_sale = as_float(row["actual_ratio_sale"])
        expected_ratio_sale = as_float(row["expected_ratio_sale"])

        expected_fixed = as_float(row["expected_payout_fixed"])

        if (
            not math.isnan(actual_ratio_revenue)
            and not math.isnan(expected_ratio_revenue)
            and math.isclose(
                actual_ratio_revenue,
                expected_ratio_revenue,
                rel_tol=0.0,
                abs_tol=RATIO_TOLERANCE + 1e-9,
            )
        ):
            return "matched", "revenue"

        if (
            not math.isnan(actual_ratio_sale)
            and not math.isnan(expected_ratio_sale)
            and math.isclose(
                actual_ratio_sale,
                expected_ratio_sale,
                rel_tol=0.0,
                abs_tol=RATIO_TOLERANCE + 1e-9,
            )
        ):
            return "matched", "sale"

        if (
            not math.isnan(expected_fixed)
            and math.isclose(
                payout,
                expected_fixed,
                rel_tol=0.0,
                abs_tol=FIXED_PAYOUT_TOLERANCE,
            )
        ):
            return "matched", "fixed"

        if payout == 0 and revenue == 0 and sale_amount == 0:
            return "matched", "zero"

        return "not matched", "none"

    evaluated = detail.apply(lambda row: evaluate_match(row), axis=1)
    detail["matched or not"] = evaluated.map(lambda x: x[0])
    detail["match method"] = evaluated.map(lambda x: x[1])

    result = detail.rename(
        columns={
            "affiliate_id": "id",
            "Offer Name": "offer name",
            "Code": "code",
            "Partner": "partner",
            "Lower_Geo": "geo",
            "Payout": "payout",
            "Revenue": "revenue",
            "Sale Amount": "sale amount",
        }
    )

    result["order id"] = result.get("record_id")
    result["date"] = pd.to_datetime(result["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    result["expected ratio revenue"] = detail["expected_ratio_revenue"]
    result["actual ratio revenue"] = detail["actual_ratio_revenue"]
    result["expected ratio sale"] = detail["expected_ratio_sale"]
    result["actual ratio sale"] = detail["actual_ratio_sale"]
    result["expected payout fixed"] = detail["expected_payout_fixed"]

    ordered_cols = [
        "id",
        "offer name",
        "code",
        "date",
        "order id",
        "partner",
        "geo",
        "payout",
        "revenue",
        "sale amount",
        "expected ratio revenue",
        "actual ratio revenue",
        "expected ratio sale",
        "actual ratio sale",
        "expected payout fixed",
        "match method",
        "matched or not",
    ]
    result = result[ordered_cols].sort_values(["offer name", "date", "order id"]).reset_index(drop=True)
    return result


def main() -> None:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    checker_path = os.path.join(script_dir, "output data", CHECKER_FILENAME)
    orders_output_path = os.path.join(script_dir, "output data", OUTPUT_ORDERS_FILENAME)
    summary_output_path = os.path.join(script_dir, "output data", OUTPUT_SUMMARY_FILENAME)

    df = load_checker_dataframe(checker_path)
    baseline = build_ratio_baseline(df)
    scoped = filter_scope(df)
    order_details = summarize_orders(scoped, baseline)
    summary = summarize_codes(order_details)

    affiliate_label = (
        "all affiliates"
        if str(TARGET_AFFILIATE_ID).strip().lower() == "all"
        else f"affiliate {TARGET_AFFILIATE_ID}"
    )
    month_label = (
        "all months"
        if isinstance(TARGET_MONTH, str) and TARGET_MONTH.strip().lower() == "all"
        else f"month {TARGET_MONTH}"
    )

    if order_details.empty:
        print(
            f"No records found for {affiliate_label} "
            f"in {month_label}"
            + ("" if TARGET_YEAR is None else f" of {TARGET_YEAR}")
        )
    order_details.to_csv(orders_output_path, index=False)
    summary.to_csv(summary_output_path, index=False)
    if not summary.empty:
        print(summary.to_string(index=False))
    print(f"Saved order-level results to {orders_output_path}")
    print(f"Saved per-code summary to {summary_output_path}")


if __name__ == "__main__":
    main()
