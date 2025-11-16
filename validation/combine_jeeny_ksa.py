"""
Combine all Jeeny KSA CSV files into a single CSV.

The script targets `validation/jeeny-ksa` by default and writes the
combined data to `jeeny_ksa_monthly.csv` in the same folder.
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Combine Jeeny KSA CSV files into one file.",
    )
    default_source = Path(__file__).with_name("jeeny-ksa")
    parser.add_argument(
        "--source",
        type=Path,
        default=default_source,
        help=f"Folder containing the daily CSV files (default: {default_source})",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output CSV path (default: <source>/jeeny_ksa_monthly.csv)",
    )
    return parser.parse_args()


def combine_csvs(source_dir: Path, output_path: Path) -> None:
    csv_files = sorted(
        file for file in source_dir.glob("*.csv") if file.is_file()
    )
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {source_dir}")

    header = None
    rows_written = 0

    with output_path.open("w", newline="", encoding="utf-8") as out_file:
        writer = None
        for csv_path in csv_files:
            with csv_path.open("r", newline="", encoding="utf-8-sig") as in_file:
                reader = csv.reader(in_file)
                try:
                    file_header = next(reader)
                except StopIteration:
                    # Skip empty files, but keep processing the rest.
                    continue

                if header is None:
                    header = file_header
                    writer = csv.writer(out_file)
                    writer.writerow(header)
                elif file_header != header:
                    raise ValueError(
                        f"Header mismatch in {csv_path}."
                        f" Expected {header} but found {file_header}."
                    )

                assert writer is not None  # mypy/type checking friendliness
                for row in reader:
                    if not any(cell.strip() for cell in row):
                        continue  # skip blank lines
                    writer.writerow(row)
                    rows_written += 1

    print(
        f"Combined {len(csv_files)} files into {output_path} "
        f"({rows_written} rows)."
    )


def main() -> None:
    args = parse_args()
    source_dir = args.source.expanduser().resolve()
    if not source_dir.exists():
        raise FileNotFoundError(f"Source folder does not exist: {source_dir}")

    output_path = (
        args.output.expanduser().resolve()
        if args.output
        else source_dir / "jeeny_ksa_monthly.csv"
    )
    combine_csvs(source_dir, output_path)


if __name__ == "__main__":
    main()
