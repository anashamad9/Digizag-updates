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


def main() -> None:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(script_dir, "..", "output data")
    os.makedirs(output_dir, exist_ok=True)

    df = fetch_remote_csv(DATA_URL)

    output_path = os.path.join(output_dir, OUTPUT_NAME)
    df.to_csv(output_path, index=False)

    print(f"Saved {len(df)} rows to {output_path}")


if __name__ == "__main__":
    try:
        main()
    except Exception as err:  # pragma: no cover - CLI usage
        print(err, file=sys.stderr)
        sys.exit(1)
