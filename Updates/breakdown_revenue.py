import os
import pandas as pd
from datetime import datetime

# ====== CONFIG ======
INPUT_PATH = "/Users/digizagoperation/Desktop/Digizag/Updates/Input data"
OUTPUT_PATH = "/Users/digizagoperation/Desktop/Digizag/Updates/Output Data"
INPUT_CSV = "Digizag.csv"
OUTPUT_CSV = "breakdown revenue checkeer.csv"


def to_datetime_mixed(s):
    return pd.to_datetime(s, format="mixed", errors="coerce")


def main() -> None:
    in_path = os.path.join(INPUT_PATH, INPUT_CSV)
    if not os.path.exists(in_path):
        print(f"Input file not found: {in_path}")
        return

    df = pd.read_csv(in_path)
    df.columns = [c.strip() for c in df.columns]

    offer_col = None
    for c in ["Offer Name", "offer name", "offer", "Offer"]:
        if c in df.columns:
            offer_col = c
            break
    date_col = None
    for c in ["Date", "date", "Order Date", "order date"]:
        if c in df.columns:
            date_col = c
            break
    revenue_col = None
    for c in ["Revenue", "revenue"]:
        if c in df.columns:
            revenue_col = c
            break

    if not offer_col or not date_col or not revenue_col:
        print(f"Missing required columns. Found: {list(df.columns)}")
        return

    df["__date"] = to_datetime_mixed(df[date_col])
    df = df.dropna(subset=["__date"]).copy()

    df["__revenue"] = pd.to_numeric(df[revenue_col], errors="coerce").fillna(0.0)

    today = datetime.now().date()
    df = df[(df["__date"].dt.year == today.year) & (df["__date"].dt.month == today.month)]
    if df.empty:
        print("No rows for the current month.")
        return

    df["__day"] = df["__date"].dt.day
    grouped = (
        df.groupby([offer_col, "__day"])["__revenue"]
          .sum()
          .unstack(fill_value=0.0)
          .reset_index()
    )

    grouped = grouped.rename(columns={offer_col: "offer"})
    grouped = grouped.sort_values(["offer"])

    out_path = os.path.join(OUTPUT_PATH, OUTPUT_CSV)
    grouped.to_csv(out_path, index=False)
    print(f"Saved revenue breakdown -> {out_path}")


if __name__ == "__main__":
    main()
