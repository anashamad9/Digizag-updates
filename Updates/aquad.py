from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Set, Tuple

import pandas as pd

# ==========================================
# Configuration
# ==========================================
# Default affiliate IDs to inspect when no CLI arguments are supplied.
DEFAULT_AFFILIATE_IDS: Sequence[str] = (
    "8486",
    "6706",
    "5566",
    "2345",
    "6729",
    "6675",
    "189",
    "14590",
    "14823",
)

# Candidate output folders that hold the per-offer CSV exports.
OUTPUT_FOLDER_NAMES: Sequence[str] = ("output data", "Output Data")

# Columns we care about (normalized header -> candidate aliases).
COLUMN_CANDIDATES: Dict[str, Tuple[str, ...]] = {
    "offer": ("offer", "offerid", "offer_id", "campaignid", "campaign_id"),
    "offer_name": ("offername", "offer_name", "offer title", "advertiser", "campaign"),
    "affiliate_id": (
        "affiliateid",
        "affiliate_id",
        "publisherid",
        "publisher_id",
        "affid",
        "affiliate",
        "id",
    ),
    "coupon": (
        "coupon",
        "couponcode",
        "coupon_code",
        "code",
        "promo",
        "promocode",
        "voucher",
        "vouchercode",
    ),
}

# Fallback workbooks to consult when CSV exports do not include a target affiliate.
FALLBACK_WORKBOOKS: Tuple[Dict[str, object], ...] = (
    {
        "path_parts": ("Input data", "Offers Coupons.xlsx"),
        "sheets": {
            "6th Street": {"offer_id": "1325", "offer_name": "6th Street"},
            "Bath & Body Works": {"offer_id": "1130", "offer_name": "Bath & Body Works"},
            "Mumzworld": {"offer_id": "1192", "offer_name": "Mumzworld"},
            "Namshi": {"offer_id": "1189", "offer_name": "Namshi"},
            "Noon GCC": {"offer_id": "1166", "offer_name": "Noon GCC"},
            "Noon Egypt": {"offer_id": "1282", "offer_name": "Noon Egypt"},
        },
        "status_candidates": ("status", "statues"),
        "allowed_statuses": ("active",),
    },
)


def _normalize_header(name: str) -> str:
    """Normalize a header to alphanumeric lowercase (no spaces or symbols)."""
    return "".join(ch for ch in str(name).lower() if ch.isalnum())


def _normalize_id(value) -> str:
    """Return a clean string ID (strip whitespace, convert floats like 8486.0 -> '8486')."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return ""
    try:
        num = float(text)
        if num.is_integer():
            return str(int(num))
    except ValueError:
        return text
    return str(num)


def _normalize_coupon(value) -> str:
    """Normalize coupon codes by stripping whitespace and removing 'nan' placeholders."""
    if value is None:
        return ""
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return ""
    return text


def _pick_column(columns_map: Dict[str, str], role: str) -> str:
    """Return actual column name for a given role ('offer', 'affiliate_id', 'coupon', etc.)."""
    for candidate in COLUMN_CANDIDATES.get(role, ()):
        normalized = _normalize_header(candidate)
        actual = columns_map.get(normalized)
        if actual:
            return actual
    return ""


def _resolve_optional_column(columns_map: Dict[str, str], candidates: Sequence[str]) -> str:
    """Return the first matching column for provided candidate names, or empty string."""
    for candidate in candidates:
        normalized = _normalize_header(candidate)
        actual = columns_map.get(normalized)
        if actual:
            return actual
    return ""


def _iter_output_directories(base_dir: Path) -> Iterable[Path]:
    """Yield existing output directories (case-insensitive variants allowed)."""
    seen: Set[Path] = set()
    for folder_name in OUTPUT_FOLDER_NAMES:
        candidate = base_dir / folder_name
        if candidate.exists() and candidate not in seen:
            seen.add(candidate)
            yield candidate


def _load_offer_name_lookup(output_dirs: Iterable[Path]) -> Dict[str, str]:
    """Load offer id -> offer name from any 'Partnership Teams View...' CSV file."""
    lookup: Dict[str, str] = {}
    for directory in output_dirs:
        pattern = "Partnership Teams View_Performance Overview_Table*.csv"
        for csv_path in directory.glob(pattern):
            try:
                df = pd.read_csv(csv_path, dtype=str)
            except Exception:
                continue
            cols = {_normalize_header(col): col for col in df.columns}
            id_col = cols.get("offerid") or cols.get("offer") or cols.get("offer_id")
            name_col = cols.get("offername") or cols.get("offer_name")
            if not (id_col and name_col):
                continue
            for _, row in df[[id_col, name_col]].dropna(how="all").iterrows():
                offer_id = _normalize_id(row[id_col])
                offer_name = str(row[name_col]).strip()
                if offer_id and offer_name:
                    lookup[offer_id] = offer_name
    return lookup


@dataclass
class CouponRecord:
    offer_id: str
    affiliate_id: str
    coupon: str
    offer_name: str = ""
    source: str = ""


def _load_coupon_records(output_dirs: Iterable[Path]) -> List[CouponRecord]:
    """Load and normalize coupon data from every CSV within the provided directories."""
    records: List[CouponRecord] = []
    for directory in output_dirs:
        for csv_path in sorted(directory.glob("*.csv")):
            try:
                df = pd.read_csv(csv_path, dtype=str, na_filter=False)
            except Exception:
                continue

            normalized_headers = {_normalize_header(col): col for col in df.columns}
            offer_col = _pick_column(normalized_headers, "offer")
            affiliate_col = _pick_column(normalized_headers, "affiliate_id")
            coupon_col = _pick_column(normalized_headers, "coupon")
            offer_name_col = _pick_column(normalized_headers, "offer_name")

            if not (offer_col and affiliate_col and coupon_col):
                continue

            subset_cols = [offer_col, affiliate_col, coupon_col]
            if offer_name_col:
                subset_cols.append(offer_name_col)

            working = df[subset_cols].copy()
            for _, row in working.iterrows():
                offer_id = _normalize_id(row[offer_col])
                affiliate_id = _normalize_id(row[affiliate_col])
                coupon = _normalize_coupon(row[coupon_col])
                offer_name = str(row[offer_name_col]).strip() if offer_name_col else ""

                if not (offer_id and affiliate_id and coupon):
                    continue

                records.append(
                    CouponRecord(
                        offer_id=offer_id,
                        affiliate_id=affiliate_id,
                        coupon=coupon,
                        offer_name=offer_name,
                        source=csv_path.name,
                    )
                )
    return records


def summarize_coupons(
    records: Sequence[CouponRecord],
    target_ids: Sequence[str],
    offer_lookup: Dict[str, str],
) -> Dict[str, Dict[str, Dict[str, List[str]]]]:
    """
    Build nested mapping:
        affiliate_id -> offer_id -> {'offer_name': str, 'coupons': [code, ...]}
    """
    normalized_targets = [
        clean_id for clean_id in (_normalize_id(t) for t in target_ids) if clean_id
    ]
    targets = set(normalized_targets)
    summary: Dict[str, Dict[str, Dict[str, List[str]]]] = {
        affiliate_id: {} for affiliate_id in normalized_targets
    }

    for record in records:
        if record.affiliate_id not in targets:
            continue

        offer_map = summary.setdefault(record.affiliate_id, {})
        offer_entry = offer_map.setdefault(
            record.offer_id,
            {
                "offer_name": record.offer_name
                or offer_lookup.get(record.offer_id, ""),
                "coupons": [],
            },
        )

        if record.offer_name and not offer_entry["offer_name"]:
            offer_entry["offer_name"] = record.offer_name
        elif not offer_entry["offer_name"]:
            offer_entry["offer_name"] = offer_lookup.get(record.offer_id, "")

        if record.coupon not in offer_entry["coupons"]:
            offer_entry["coupons"].append(record.coupon)

    # Sort coupons for deterministic output.
    for offer_map in summary.values():
        for offer_entry in offer_map.values():
            offer_entry["coupons"].sort()

    return summary


def _load_fallback_coupon_codes(
    base_dir: Path, target_ids: Set[str]
) -> Dict[str, Dict[str, Dict[str, List[str]]]]:
    """Load supplemental coupon data for affiliates that are missing from CSV exports."""
    if not target_ids:
        return {}

    fallback_summary: Dict[str, Dict[str, Dict[str, Set[str]]]] = {}

    for workbook in FALLBACK_WORKBOOKS:
        path_parts = workbook.get("path_parts")
        if not path_parts:
            continue

        path = base_dir.joinpath(*path_parts)
        if not path.exists():
            continue

        sheets_meta = workbook.get("sheets")
        if not isinstance(sheets_meta, dict):
            continue

        status_candidates = tuple(workbook.get("status_candidates", ()))
        allowed_statuses = {
            str(value).strip().lower()
            for value in workbook.get("allowed_statuses", ())
            if isinstance(value, str)
        }

        try:
            xls = pd.ExcelFile(path)
        except Exception:
            continue

        for sheet_name, meta in sheets_meta.items():
            if sheet_name not in xls.sheet_names:
                continue

            try:
                df = xls.parse(sheet_name, dtype=str)
            except Exception:
                continue

            columns_map = {_normalize_header(col): col for col in df.columns}
            code_col = _pick_column(columns_map, "coupon")
            aff_col = _pick_column(columns_map, "affiliate_id")
            if not (code_col and aff_col):
                continue

            status_col = ""
            if status_candidates:
                status_col = _resolve_optional_column(columns_map, status_candidates)

            offer_id = _normalize_id(meta.get("offer_id", ""))
            if not offer_id:
                continue

            offer_name = str(meta.get("offer_name", "")).strip()

            for _, row in df.iterrows():
                aff_value = _normalize_id(row.get(aff_col))
                if not aff_value or aff_value not in target_ids:
                    continue

                if status_col and allowed_statuses:
                    status_value = str(row.get(status_col, "")).strip().lower()
                    if status_value and status_value not in allowed_statuses:
                        continue

                coupon_value = _normalize_coupon(row.get(code_col))
                if not coupon_value:
                    continue

                offers_map = fallback_summary.setdefault(aff_value, {})
                entry = offers_map.setdefault(
                    offer_id, {"offer_name": offer_name, "coupons": set()}
                )

                if offer_name and not entry["offer_name"]:
                    entry["offer_name"] = offer_name

                entry["coupons"].add(coupon_value)

    normalized_fallback: Dict[str, Dict[str, Dict[str, List[str]]]] = {}
    for affiliate_id, offers_map in fallback_summary.items():
        normalized_fallback[affiliate_id] = {}
        for offer_id, entry in offers_map.items():
            normalized_fallback[affiliate_id][offer_id] = {
                "offer_name": entry["offer_name"],
                "coupons": sorted(entry["coupons"]),
            }
    return normalized_fallback


def _merge_coupon_summaries(
    summary: Dict[str, Dict[str, Dict[str, List[str]]]],
    supplement: Dict[str, Dict[str, Dict[str, List[str]]]],
    offer_lookup: Dict[str, str],
) -> None:
    """Merge fallback coupon data into the main summary in place."""
    if not supplement:
        return

    for affiliate_id, offers in supplement.items():
        offer_map = summary.setdefault(affiliate_id, {})

        for offer_id, entry in offers.items():
            offer_entry = offer_map.setdefault(
                offer_id,
                {
                    "offer_name": entry.get("offer_name", "")
                    or offer_lookup.get(offer_id, ""),
                    "coupons": [],
                },
            )

            if entry.get("offer_name") and not offer_entry["offer_name"]:
                offer_entry["offer_name"] = entry["offer_name"]
            elif not offer_entry["offer_name"]:
                offer_entry["offer_name"] = offer_lookup.get(offer_id, "")

            for coupon in entry.get("coupons", []):
                if coupon not in offer_entry["coupons"]:
                    offer_entry["coupons"].append(coupon)

            offer_entry["coupons"].sort()


def print_summary(summary: Dict[str, Dict[str, Dict[str, List[str]]]]) -> None:
    """Pretty-print the coupon summary to stdout."""
    if not summary:
        print("No coupon data found for the requested affiliate IDs.")
        return

    def sort_key(raw: str) -> tuple:
        if raw.isdigit():
            return (0, int(raw))
        return (1, raw)

    for affiliate_id in sorted(summary.keys(), key=sort_key):
        print(f"Affiliate {affiliate_id}")
        offers = summary[affiliate_id]
        if not offers:
            print("  (no coupons)")
            continue

        for offer_id in sorted(offers.keys(), key=sort_key):
            entry = offers[offer_id]
            offer_name = entry.get("offer_name", "").strip()
            label = f"{offer_id} - {offer_name}" if offer_name else offer_id
            coupons = ", ".join(entry.get("coupons", [])) or "(none)"
            print(f"  Offer {label}: {coupons}")
        print()


def write_csv(summary: Dict[str, Dict[str, Dict[str, List[str]]]], destination: Path) -> None:
    """Persist the affiliate/offer coupon summary to a CSV file."""
    rows: List[Dict[str, str]] = []

    def sort_key(raw: str) -> tuple:
        if raw.isdigit():
            return (0, int(raw))
        return (1, raw)

    for affiliate_id in sorted(summary.keys(), key=sort_key):
        for offer_id in sorted(summary[affiliate_id].keys(), key=sort_key):
            entry = summary[affiliate_id][offer_id]
            coupons = entry.get("coupons", [])
            rows.append(
                {
                    "affiliate_id": affiliate_id,
                    "offer_id": offer_id,
                    "offer_name": entry.get("offer_name", ""),
                    "coupons": ", ".join(coupons),
                }
            )

    df = pd.DataFrame(rows, columns=["affiliate_id", "offer_id", "offer_name", "coupons"])
    destination.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(destination, index=False)


def main(argv: Sequence[str] | None = None) -> None:
    argv = argv if argv is not None else sys.argv[1:]

    # Allow overriding target affiliate IDs via CLI arguments.
    target_ids = list(argv) if argv else list(DEFAULT_AFFILIATE_IDS)
    normalized_targets = [
        _normalize_id(value) for value in target_ids if _normalize_id(value)
    ]

    script_dir = Path(__file__).resolve().parent
    output_dirs = list(_iter_output_directories(script_dir))
    if not output_dirs:
        print("Could not locate any output data directories.")
        return

    offer_lookup = _load_offer_name_lookup(output_dirs)
    records = _load_coupon_records(output_dirs)
    summary = summarize_coupons(records, target_ids, offer_lookup)
    fallback = _load_fallback_coupon_codes(script_dir, set(normalized_targets))
    _merge_coupon_summaries(summary, fallback, offer_lookup)
    print_summary(summary)

    if summary:
        output_path = Path(__file__).resolve().parent / "Aquaaaaaaaaad.csv"
        write_csv(summary, output_path)
        print(f"Saved CSV: {output_path.name}")
    else:
        print("Skipped CSV export because no data was found.")


if __name__ == "__main__":
    main()
