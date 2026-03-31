"""
extract/load_csv.py

Reads bank and credit card transaction CSVs, standardises column names
and data types to a common schema, validates the data, and writes to DuckDB.

Usage:
    python extract/load_csv.py

Environment variables (set in .env):
    DUCKDB_PATH     - path to the DuckDB file (created if it does not exist)
    RAW_DATA_DIR    - folder containing the source CSVs
    LOG_LEVEL       - DEBUG | INFO | WARNING | ERROR (default: INFO)
    FULL_REFRESH    - if "true", drops and recreates tables before loading
"""

import os
import sys
from pathlib import Path

import duckdb
import pandas as pd
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "./data/finance.duckdb")
RAW_DATA_DIR = Path(os.getenv("RAW_DATA_DIR", "./data"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
FULL_REFRESH = os.getenv("FULL_REFRESH", "false").lower() == "true"

BANK_FILE = RAW_DATA_DIR / "sample_bank.csv"
CARD_FILE = RAW_DATA_DIR / "sample_card.csv"

# The single shared schema that both sources are mapped to before loading.
# All dbt staging models read from these raw tables.
COMMON_SCHEMA = [
    "transaction_id",
    "date",
    "description",
    "amount",
    "transaction_type",
    "source",         # "bank" or "card" — added during extraction
]

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logger.remove()
logger.add(sys.stderr, level=LOG_LEVEL, format="{time:HH:mm:ss} | {level} | {message}")

log_file = os.getenv("LOG_FILE", "")
if log_file:
    Path(log_file).parent.mkdir(parents=True, exist_ok=True)
    logger.add(log_file, level=LOG_LEVEL, rotation="1 week", retention="4 weeks")


# ---------------------------------------------------------------------------
# Column mapping
# ---------------------------------------------------------------------------

# Each source CSV uses different column names.
# These maps rename them to the common schema before loading.

BANK_COLUMN_MAP = {
    "transaction_id": "transaction_id",
    "date":           "date",
    "description":    "description",
    "amount":         "amount",
    "transaction_type": "transaction_type",
}

CARD_COLUMN_MAP = {
    "transaction_id": "transaction_id",
    "post_date":      "date",           # card uses post_date, bank uses date
    "merchant_name":  "description",    # card uses merchant_name, bank uses description
    "amount":         "amount",
    "transaction_type": "transaction_type",
}


# ---------------------------------------------------------------------------
# Extract functions
# ---------------------------------------------------------------------------

def read_bank_csv(path: Path) -> pd.DataFrame:
    """Read and standardise the bank transaction CSV."""
    logger.info(f"Reading bank CSV: {path}")

    df = pd.read_csv(path)
    logger.debug(f"Raw bank columns: {list(df.columns)}")
    logger.debug(f"Raw bank rows: {len(df)}")

    df = df.rename(columns=BANK_COLUMN_MAP)
    df = df[list(BANK_COLUMN_MAP.values())]
    df["source"] = "bank"

    return df


def read_card_csv(path: Path) -> pd.DataFrame:
    """Read and standardise the credit card transaction CSV."""
    logger.info(f"Reading card CSV: {path}")

    df = pd.read_csv(path)
    logger.debug(f"Raw card columns: {list(df.columns)}")
    logger.debug(f"Raw card rows: {len(df)}")

    df = df.rename(columns=CARD_COLUMN_MAP)
    df = df[list(CARD_COLUMN_MAP.values())]
    df["source"] = "card"

    return df


# ---------------------------------------------------------------------------
# Transform functions
# ---------------------------------------------------------------------------

def standardise_types(df: pd.DataFrame) -> pd.DataFrame:
    """Cast columns to consistent data types across both sources."""

    df["date"] = pd.to_datetime(df["date"], infer_datetime_format=True)
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    df["transaction_id"] = df["transaction_id"].astype(str).str.strip()
    df["description"] = df["description"].astype(str).str.strip().str.upper()
    df["transaction_type"] = df["transaction_type"].astype(str).str.strip().str.lower()
    df["source"] = df["source"].astype(str).str.strip().str.lower()

    return df


def validate(df: pd.DataFrame, source_name: str) -> bool:
    """
    Run basic data quality checks. Returns True if all checks pass.
    Logs a warning for each issue found rather than raising immediately,
    so all problems are visible in a single run.
    """
    passed = True

    null_counts = df[COMMON_SCHEMA].isnull().sum()
    for col, count in null_counts.items():
        if count > 0:
            logger.warning(f"[{source_name}] {count} null value(s) in column '{col}'")
            passed = False

    if len(df) == 0:
        logger.error(f"[{source_name}] DataFrame is empty — no rows to load")
        passed = False

    dup_ids = df["transaction_id"].duplicated().sum()
    if dup_ids > 0:
        logger.warning(f"[{source_name}] {dup_ids} duplicate transaction_id(s) found")
        passed = False

    invalid_amounts = df["amount"].isna().sum()
    if invalid_amounts > 0:
        logger.warning(f"[{source_name}] {invalid_amounts} row(s) with unparseable amount")
        passed = False

    if passed:
        logger.info(f"[{source_name}] Validation passed — {len(df)} rows ready to load")

    return passed


# ---------------------------------------------------------------------------
# Load function
# ---------------------------------------------------------------------------

def load_to_duckdb(df: pd.DataFrame, table_name: str, conn: duckdb.DuckDBPyConnection) -> None:

    full_table_name = f"personal_finance.raw.{table_name}"

    if FULL_REFRESH:
        conn.execute(f"DROP TABLE IF EXISTS {full_table_name}")

    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {full_table_name} (
            transaction_id   VARCHAR,
            date             TIMESTAMP,
            description      VARCHAR,
            amount           DOUBLE,
            transaction_type VARCHAR,
            source           VARCHAR
        )
    """)

    existing = conn.execute(f"SELECT COUNT(*) FROM {full_table_name}").fetchone()[0]

    if existing > 0 and not FULL_REFRESH:
        existing_ids = conn.execute(
            f"SELECT transaction_id FROM {full_table_name}"
        ).df()["transaction_id"].tolist()

        new_rows = df[~df["transaction_id"].isin(existing_ids)]
        df = new_rows

    if len(df) > 0:
        conn.execute(f"INSERT INTO {full_table_name} SELECT * FROM df")
        logger.info(f"[{full_table_name}] Loaded {len(df)} row(s)")
    else:
        logger.info(f"[{full_table_name}] Nothing new to load")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    logger.info("Starting extract and load")
    logger.info(f"DuckDB path : {DUCKDB_PATH}")
    logger.info(f"Raw data dir: {RAW_DATA_DIR}")
    logger.info(f"Full refresh: {FULL_REFRESH}")

    # Ensure the output directory exists
    Path(DUCKDB_PATH).parent.mkdir(parents=True, exist_ok=True)

    sources = [
        (BANK_FILE, "bank", read_bank_csv, "raw_bank_transactions"),
        (CARD_FILE, "card", read_card_csv, "raw_card_transactions"),
    ]

    all_valid = True

    with duckdb.connect(DUCKDB_PATH) as conn:
        conn.execute("USE personal_finance")
        for file_path, source_name, reader_fn, table_name in sources:

            if not file_path.exists():
                logger.error(f"Source file not found: {file_path}")
                all_valid = False
                continue

            try:
                df = reader_fn(file_path)
                df = standardise_types(df)

                if not validate(df, source_name):
                    all_valid = False
                    logger.error(
                        f"[{source_name}] Validation failed — skipping load for this source"
                    )
                    continue

                load_to_duckdb(df, table_name, conn)

            except Exception as e:
                logger.error(f"[{source_name}] Unexpected error: {e}")
                all_valid = False

    if all_valid:
        logger.info("Extract and load completed successfully")
    else:
        logger.error("Extract and load completed with errors — review warnings above")
        sys.exit(1)


if __name__ == "__main__":
    main()
