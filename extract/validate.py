"""
extract/validate.py

Validates raw bank and credit card transaction CSVs before they are loaded
into DuckDB. Designed to be run standalone or imported by load_csv.py.

Checks performed:
    - File existence and non-zero size
    - Required columns are present
    - No duplicate transaction IDs
    - No nulls in critical columns
    - Date column is parseable
    - Amount column is numeric and within expected bounds
    - Transaction type values are in the allowed set
    - No future-dated transactions
    - Row count is within expected range

Usage:
    # Validate both files and print a report
    python extract/validate.py

    # Import and call from load_csv.py
    from extract.validate import validate_file, ValidationResult
"""

import os
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

RAW_DATA_DIR = Path(os.getenv("RAW_DATA_DIR", "./data"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

BANK_FILE = RAW_DATA_DIR / "sample_bank.csv"
CARD_FILE = RAW_DATA_DIR / "sample_card.csv"

# Columns that must be present in each source file
BANK_REQUIRED_COLUMNS = {
    "transaction_id",
    "date",
    "description",
    "amount",
    "transaction_type",
}

CARD_REQUIRED_COLUMNS = {
    "transaction_id",
    "post_date",
    "merchant_name",
    "amount",
    "transaction_type",
}

# Columns that must have no null values
BANK_NON_NULLABLE = ["transaction_id", "date", "description", "amount", "transaction_type"]
CARD_NON_NULLABLE = ["transaction_id", "post_date", "merchant_name", "amount", "transaction_type"]

# Allowed values for transaction_type (case-insensitive)
VALID_TRANSACTION_TYPES = {"debit", "credit"}

# Amount bounds — transactions outside this range are flagged as suspicious
AMOUNT_MIN = -50_000.00
AMOUNT_MAX =  50_000.00

# Row count bounds — flag files that look unexpectedly empty or huge
ROW_COUNT_MIN = 1
ROW_COUNT_MAX = 100_000


# ---------------------------------------------------------------------------
# Result container
# ---------------------------------------------------------------------------

@dataclass
class ValidationResult:
    """
    Holds the outcome of validating a single CSV file.

    Attributes:
        source      : human-readable source name ("bank" or "card")
        passed      : True only if zero errors were found
        errors      : list of blocking problems — load should be aborted
        warnings    : list of non-blocking issues — load may continue
        row_count   : number of data rows in the file (0 if unreadable)
    """
    source: str
    passed: bool = True
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    row_count: int = 0

    def add_error(self, message: str) -> None:
        self.errors.append(message)
        self.passed = False

    def add_warning(self, message: str) -> None:
        self.warnings.append(message)

    def summary(self) -> str:
        status = "PASSED" if self.passed else "FAILED"
        lines = [
            f"── {self.source.upper()} [{status}] ─────────────────────────",
            f"   Rows       : {self.row_count}",
            f"   Errors     : {len(self.errors)}",
            f"   Warnings   : {len(self.warnings)}",
        ]
        for e in self.errors:
            lines.append(f"   [ERROR]   {e}")
        for w in self.warnings:
            lines.append(f"   [WARNING] {w}")
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Individual check functions
# ---------------------------------------------------------------------------

def check_file_exists(path: Path, result: ValidationResult) -> bool:
    """Returns False immediately if the file is missing or empty."""
    if not path.exists():
        result.add_error(f"File not found: {path}")
        return False
    if path.stat().st_size == 0:
        result.add_error(f"File is empty (0 bytes): {path}")
        return False
    return True


def check_required_columns(
    df: pd.DataFrame,
    required: set[str],
    result: ValidationResult,
) -> bool:
    """Returns False if any required column is missing."""
    missing = required - set(df.columns)
    if missing:
        result.add_error(f"Missing required column(s): {sorted(missing)}")
        return False
    extra = set(df.columns) - required
    if extra:
        result.add_warning(f"Unexpected extra column(s) (will be ignored): {sorted(extra)}")
    return True


def check_row_count(df: pd.DataFrame, result: ValidationResult) -> None:
    result.row_count = len(df)
    if len(df) < ROW_COUNT_MIN:
        result.add_error(f"File has {len(df)} data row(s) — minimum expected is {ROW_COUNT_MIN}")
    elif len(df) > ROW_COUNT_MAX:
        result.add_warning(
            f"File has {len(df)} rows — exceeds expected maximum of {ROW_COUNT_MAX}. "
            "Consider splitting the file."
        )


def check_duplicate_ids(df: pd.DataFrame, id_col: str, result: ValidationResult) -> None:
    dupes = df[id_col].duplicated()
    if dupes.any():
        dupe_ids = df.loc[dupes, id_col].tolist()
        result.add_error(
            f"Found {dupes.sum()} duplicate transaction ID(s): {dupe_ids[:5]}"
            + (" (showing first 5)" if len(dupe_ids) > 5 else "")
        )


def check_nulls(
    df: pd.DataFrame,
    non_nullable_cols: list[str],
    result: ValidationResult,
) -> None:
    for col in non_nullable_cols:
        if col not in df.columns:
            continue
        null_count = df[col].isnull().sum()
        if null_count > 0:
            row_indices = df.index[df[col].isnull()].tolist()
            result.add_error(
                f"Column '{col}' has {null_count} null value(s) at row(s): "
                f"{row_indices[:5]}"
                + (" (showing first 5)" if len(row_indices) > 5 else "")
            )


def check_dates(df: pd.DataFrame, date_col: str, result: ValidationResult) -> None:
    """Check that dates are parseable and not in the future."""
    try:
        parsed = pd.to_datetime(df[date_col], infer_datetime_format=True, errors="coerce")
    except Exception as e:
        result.add_error(f"Date column '{date_col}' could not be parsed: {e}")
        return

    unparseable = parsed.isnull().sum()
    if unparseable > 0:
        bad_rows = df.index[parsed.isnull()].tolist()
        result.add_error(
            f"Column '{date_col}' has {unparseable} unparseable date(s) at row(s): "
            f"{bad_rows[:5]}"
            + (" (showing first 5)" if len(bad_rows) > 5 else "")
        )

    future_mask = parsed > datetime.now()
    if future_mask.any():
        future_count = future_mask.sum()
        result.add_warning(
            f"Column '{date_col}' has {future_count} future-dated transaction(s). "
            "This may be intentional (pending transactions) but worth confirming."
        )


def check_amounts(df: pd.DataFrame, result: ValidationResult) -> None:
    """Check that amounts are numeric and within expected bounds."""
    numeric = pd.to_numeric(df["amount"], errors="coerce")

    unparseable = numeric.isnull().sum()
    if unparseable > 0:
        bad_rows = df.index[numeric.isnull()].tolist()
        result.add_error(
            f"Column 'amount' has {unparseable} non-numeric value(s) at row(s): "
            f"{bad_rows[:5]}"
            + (" (showing first 5)" if len(bad_rows) > 5 else "")
        )

    out_of_bounds = numeric[(numeric < AMOUNT_MIN) | (numeric > AMOUNT_MAX)]
    if not out_of_bounds.empty:
        result.add_warning(
            f"{len(out_of_bounds)} amount(s) outside expected range "
            f"[{AMOUNT_MIN:,.2f}, {AMOUNT_MAX:,.2f}]: "
            f"{out_of_bounds.tolist()[:5]}"
            + (" (showing first 5)" if len(out_of_bounds) > 5 else "")
        )

    zero_amounts = (numeric == 0).sum()
    if zero_amounts > 0:
        result.add_warning(f"{zero_amounts} transaction(s) with an amount of $0.00")


def check_transaction_types(df: pd.DataFrame, result: ValidationResult) -> None:
    """Check that all transaction_type values are in the allowed set."""
    normalised = df["transaction_type"].astype(str).str.strip().str.lower()
    invalid_mask = ~normalised.isin(VALID_TRANSACTION_TYPES)
    if invalid_mask.any():
        invalid_values = normalised[invalid_mask].unique().tolist()
        result.add_error(
            f"Unexpected transaction_type value(s): {invalid_values}. "
            f"Allowed values: {sorted(VALID_TRANSACTION_TYPES)}"
        )


# ---------------------------------------------------------------------------
# File-level validators
# ---------------------------------------------------------------------------

def validate_bank_file(path: Path) -> ValidationResult:
    result = ValidationResult(source="bank")

    if not check_file_exists(path, result):
        return result

    try:
        df = pd.read_csv(path)
    except Exception as e:
        result.add_error(f"Could not read CSV: {e}")
        return result

    if not check_required_columns(df, BANK_REQUIRED_COLUMNS, result):
        return result

    check_row_count(df, result)
    check_duplicate_ids(df, "transaction_id", result)
    check_nulls(df, BANK_NON_NULLABLE, result)
    check_dates(df, "date", result)
    check_amounts(df, result)
    check_transaction_types(df, result)

    return result


def validate_card_file(path: Path) -> ValidationResult:
    result = ValidationResult(source="card")

    if not check_file_exists(path, result):
        return result

    try:
        df = pd.read_csv(path)
    except Exception as e:
        result.add_error(f"Could not read CSV: {e}")
        return result

    if not check_required_columns(df, CARD_REQUIRED_COLUMNS, result):
        return result

    check_row_count(df, result)
    check_duplicate_ids(df, "transaction_id", result)
    check_nulls(df, CARD_NON_NULLABLE, result)
    check_dates(df, "post_date", result)
    check_amounts(df, result)
    check_transaction_types(df, result)

    return result


def validate_file(path: Path, source: str) -> ValidationResult:
    """
    Convenience wrapper for import by load_csv.py.

    Args:
        path   : path to the CSV file
        source : "bank" or "card"

    Returns:
        ValidationResult with passed=True if all checks pass
    """
    if source == "bank":
        return validate_bank_file(path)
    elif source == "card":
        return validate_card_file(path)
    else:
        result = ValidationResult(source=source)
        result.add_error(f"Unknown source '{source}' — expected 'bank' or 'card'")
        return result


# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

logger.remove()
logger.add(sys.stderr, level=LOG_LEVEL, format="{time:HH:mm:ss} | {level} | {message}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    logger.info("Running pre-load CSV validation")
    logger.info(f"Raw data dir: {RAW_DATA_DIR}")

    results = [
        validate_bank_file(BANK_FILE),
        validate_card_file(CARD_FILE),
    ]

    print()
    print("=" * 54)
    print("  CSV VALIDATION REPORT")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 54)
    for result in results:
        print()
        print(result.summary())
    print()
    print("=" * 54)

    all_passed = all(r.passed for r in results)
    total_errors = sum(len(r.errors) for r in results)
    total_warnings = sum(len(r.warnings) for r in results)

    print(f"  Result   : {'ALL CHECKS PASSED' if all_passed else 'VALIDATION FAILED'}")
    print(f"  Errors   : {total_errors}")
    print(f"  Warnings : {total_warnings}")
    print("=" * 54)
    print()

    if not all_passed:
        logger.error("Validation failed — fix the errors above before loading to DuckDB")
        sys.exit(1)
    else:
        logger.info("Validation passed — safe to run load_csv.py")


if __name__ == "__main__":
    main()
