import os
import sys
import pandas as pd

# Remote CSV shared from Trevor.io
DATA_URL = (
    "https://app.trevor.io/share/view/4251e798-bb38-4c9d-a3bb-0c289ffcc5c8/1d/"
    "Promo_Digizag_Co_for_Dashboard.csv?seed=68"
)
OUTPUT_NAME = "ajeer.csv"


def fetch_remote_csv(url: str) -> pd.DataFrame:
    """Load the remote CSV into a DataFrame."""
    try:
        return pd.read_csv(url)
    except Exception as exc:  # pragma: no cover - network issues
        raise RuntimeError(f"Failed to fetch remote CSV from {url}") from exc


def normalize_date_column(df: pd.DataFrame) -> pd.DataFrame:
    """Rename and standardize the first column that looks like a date."""

    def parse_mixed_dates(series: pd.Series) -> pd.Series:
        """Best-effort parsing across common date formats."""

        parsers = [
            lambda s: pd.to_datetime(s, format="%d-%b-%Y", errors="coerce"),
            lambda s: pd.to_datetime(s, format="%d-%b-%y", errors="coerce"),
            lambda s: pd.to_datetime(s, errors="coerce", dayfirst=True),
            lambda s: pd.to_datetime(s, errors="coerce"),
        ]

        for parser in parsers:
            parsed = parser(series)
            if parsed.notna().any():
                return parsed

        return pd.to_datetime(series, errors="coerce")

    for column in df.columns:
        if "date" not in str(column).lower():
            continue

        parsed_dates = parse_mixed_dates(df[column])
        if parsed_dates.notna().any():
            df[column] = parsed_dates.dt.strftime("%Y-%m-%d")
            if column != "date":
                df.rename(columns={column: "date"}, inplace=True)
            break

    return df


def main() -> None:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(script_dir, "..", "output data")
    os.makedirs(output_dir, exist_ok=True)

    df = fetch_remote_csv(DATA_URL)
    df = normalize_date_column(df)

    output_path = os.path.join(output_dir, OUTPUT_NAME)
    df.to_csv(output_path, index=False)

    print(f"Saved {len(df)} rows to {output_path}")


if __name__ == "__main__":
    try:
        main()
    except Exception as err:  # pragma: no cover - CLI usage
        print(err, file=sys.stderr)
        sys.exit(1)
