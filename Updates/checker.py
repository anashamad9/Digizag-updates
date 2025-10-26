import math
import os
import re
from typing import Optional

import pandas as pd

# ========= CONFIG =========
# Affiliate (partner) ID to inspect; editable by user.
TARGET_AFFILIATE_ID = "14465"

# Numeric month to analyze (1-12). Example: 10 for October.
TARGET_MONTH = 10

# Optional: restrict to a specific year. Set to None to include any year.
TARGET_YEAR: Optional[int] = None

# Allowed deviation (as a fraction) when comparing actual ratio vs baseline.
RATIO_TOLERANCE = 0.01  # 1%

# Filenames
CHECKER_FILENAME = "checker.csv"
OUTPUT_ORDERS_FILENAME = "checker_results.csv"
OUTPUT_SUMMARY_FILENAME = "checker_results_summary.csv"


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
    """Compute the median payout/revenue ratio per offer+code across the dataset."""
    valid = df[df["ratio"].notna()].copy()
    if valid.empty:
        return pd.DataFrame(columns=["Offer Name", "Code", "expected_ratio"])
    grouped = (
        valid.groupby(["Offer Name", "Code"])["ratio"]
        .median()
        .reset_index()
        .rename(columns={"ratio": "expected_ratio"})
    )
    return grouped


def filter_scope(df: pd.DataFrame) -> pd.DataFrame:
    """Filter records by affiliate id, month, and optional year."""
    mask = df["affiliate_id"].astype(str) == str(TARGET_AFFILIATE_ID)
    mask &= df["date"].dt.month == int(TARGET_MONTH)
    if TARGET_YEAR is not None:
        mask &= df["date"].dt.year == int(TARGET_YEAR)
    scoped = df[mask].copy()
    return scoped


def summarize_codes(order_details: pd.DataFrame) -> pd.DataFrame:
    """Aggregate order-level records back to per-code totals for a quick overview."""
    if order_details.empty:
        return pd.DataFrame(
            columns=["id", "offer name", "code", "payout", "revenue", "sale amount", "matched or not"]
        )

    summary = (
        order_details.groupby(["id", "offer name", "code"], dropna=False)
        .agg(
            payout=("payout", "sum"),
            revenue=("revenue", "sum"),
            sale_amount=("sale amount", "sum"),
            all_matched=("matched or not", lambda col: all(val == "matched" for val in col)),
        )
        .reset_index()
    )

    summary.rename(columns={"sale_amount": "sale amount"}, inplace=True)

    summary["matched or not"] = summary.pop("all_matched").map({True: "matched", False: "not matched"})
    summary = summary[["id", "offer name", "code", "payout", "revenue", "sale amount", "matched or not"]]
    summary.sort_values(["offer name", "code"], inplace=True)
    summary.reset_index(drop=True, inplace=True)
    return summary


def summarize_orders(scoped: pd.DataFrame, baseline: pd.DataFrame) -> pd.DataFrame:
    """Return per-order payout validation with date and record id."""
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

    has_revenue = detail["Revenue"] != 0
    detail.loc[has_revenue, "actual_ratio"] = detail.loc[has_revenue, "Payout"] / detail.loc[has_revenue, "Revenue"]
    detail.loc[~has_revenue, "actual_ratio"] = pd.NA

    def mark_match(row) -> str:
        if row["Revenue"] == 0:
            return "matched" if abs(row["Payout"]) <= 1e-6 else "not matched"
        if pd.isna(row["expected_ratio"]):
            return "matched"
        if pd.isna(row["actual_ratio"]):
            return "not matched"
        if math.isclose(
            row["actual_ratio"],
            row["expected_ratio"],
            rel_tol=0.0,
            abs_tol=RATIO_TOLERANCE + 1e-9,
        ):
            return "matched"
        return "not matched"

    detail["matched or not"] = detail.apply(mark_match, axis=1)

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
    result["expected ratio"] = detail["expected_ratio"]
    result["actual ratio"] = detail["actual_ratio"]

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
        "expected ratio",
        "actual ratio",
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

    if order_details.empty:
        print(
            f"No records found for affiliate {TARGET_AFFILIATE_ID} "
            f"in month {TARGET_MONTH}"
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
