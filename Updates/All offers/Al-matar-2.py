import os
import re
from datetime import datetime

import pandas as pd

# =======================
# CONFIG (Al Matar 2)
# =======================
REPORT_PREFIX = "Al Matar - Digizag Data - Backend"
OUTPUT_CSV = "al_matar_2.csv"

# =======================
# PATHS
# =======================
script_dir = os.path.dirname(os.path.abspath(__file__))
updates_dir = os.path.dirname(script_dir)
input_dir = os.path.join(updates_dir, "Input data")
if not os.path.isdir(input_dir):
    input_dir = os.path.join(updates_dir, "input data")
output_dir = os.path.join(updates_dir, "output data")
os.makedirs(output_dir, exist_ok=True)

output_file = os.path.join(output_dir, OUTPUT_CSV)

# =======================
# HELPERS
# =======================
def find_matching_report(directory: str, prefix: str) -> str:
    """
    Find a .xlsx or .csv in `directory` whose base filename starts with `prefix`
    (case-insensitive). Prefers exact '<prefix>.<ext>' if present; otherwise newest.
    """
    prefix_lower = prefix.lower()
    candidates = []
    for fname in os.listdir(directory):
        if fname.startswith("~$"):
            continue
        fname_lower = fname.lower()
        if not (fname_lower.endswith(".xlsx") or fname_lower.endswith(".csv")):
            continue
        base = os.path.splitext(fname_lower)[0]
        if base.startswith(prefix_lower):
            candidates.append(os.path.join(directory, fname))

    if not candidates:
        available = [
            f
            for f in os.listdir(directory)
            if f.lower().endswith(".xlsx") or f.lower().endswith(".csv")
        ]
        raise FileNotFoundError(
            f"No .xlsx/.csv file starting with '{prefix}' found in: {directory}\n"
            f"Available files: {available}"
        )

    for ext in (".xlsx", ".csv"):
        exact = [
            p for p in candidates if os.path.basename(p).lower() == (prefix_lower + ext)
        ]
        if exact:
            return exact[0]

    return max(candidates, key=os.path.getmtime)


def col_by_letter(df: pd.DataFrame, letter: str) -> str:
    """Return actual column name by Excel letter (A=0, B=1, ...)."""
    idx = ord(letter.upper()) - ord("A")
    if idx < 0 or idx >= len(df.columns):
        raise IndexError(f"Column letter {letter} out of range for columns: {list(df.columns)}")
    return df.columns[idx]


def parse_route(route_val: str):
    """
    Parse a route like 'KSA-KSA', 'KSA > KSA', 'SAUDI ARABIA to UAE'.
    Return (origin, dest) uppercase or (None, None).
    """
    if pd.isna(route_val):
        return None, None
    s = str(route_val).strip()
    if not s:
        return None, None
    s_norm = re.sub(r"\s*(?:-|–|—|>|to|/|→)\s*", "-", s, flags=re.IGNORECASE)
    parts = [p.strip().upper() for p in s_norm.split("-") if p.strip()]
    if len(parts) >= 2:
        return parts[0], parts[1]
    return None, None


def classify_category(row, product_col: str, route_col: str) -> str:
    pt = str(row.get(product_col, "")).strip().lower()
    if "hotel" in pt:
        return "hotels"
    if "flight" in pt or "air" in pt:
        origin, dest = parse_route(row.get(route_col, ""))
        if origin and dest:
            return "domestic" if origin == dest else "international"
        return "international"
    return "other"

# =======================
# LOAD MAIN REPORT
# =======================
print(f"Run time: {datetime.now()}")
input_file = find_matching_report(input_dir, REPORT_PREFIX)

if input_file.lower().endswith(".xlsx"):
    df = pd.read_excel(input_file)
else:
    df = pd.read_csv(input_file)

# Resolve columns by LETTER as requested:
# A: Date, C: Status (must be "success"), G: Product Type (hotel/flight), L: Flight Route - Country
date_col = col_by_letter(df, "A")
status_col = col_by_letter(df, "C")
product_col = col_by_letter(df, "G")
route_col = col_by_letter(df, "L")

# Parse dates and filter to successful orders
df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
df = df.dropna(subset=[date_col])
df = df[df[status_col].astype(str).str.lower().str.strip().eq("success")].copy()

# Categorize and group by month
df["category"] = df.apply(classify_category, axis=1, args=(product_col, route_col))
df["month"] = df[date_col].dt.strftime("%Y-%m")

summary = (
    df.groupby(["month", "category"])
    .size()
    .reset_index(name="orders")
)
summary["total_orders"] = summary.groupby("month")["orders"].transform("sum")
summary["pct_of_month"] = (summary["orders"] / summary["total_orders"] * 100).round(2)

# Sort months and categories for readability
cat_order = ["domestic", "hotels", "international", "other"]
summary["category"] = pd.Categorical(summary["category"], categories=cat_order, ordered=True)
summary = summary.sort_values(["month", "category"]).reset_index(drop=True)

summary.to_csv(output_file, index=False)

print(f"Using report file: {input_file}")
print(f"Saved summary: {output_file}")
