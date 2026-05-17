import logging
import os
from pathlib import Path

from dotenv import load_dotenv
import snowflake.connector


load_dotenv()

logger = logging.getLogger(__name__)

BATCH_DIR = Path("data/stream_batches")
PROCESSED_DIR = BATCH_DIR / "_uploaded"


def get_connection() -> snowflake.connector.SnowflakeConnection:
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
    )


def upload_batches() -> None:
    """
    Upload all unprocessed NDJSON batch files from BATCH_DIR to the Snowflake
    stage. Files are moved to PROCESSED_DIR after a confirmed successful upload
    so re-runs never re-upload already-staged data.
    """
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    batch_files = sorted(BATCH_DIR.glob("*.ndjson"))

    if not batch_files:
        logger.info("No new batch files found in %s — nothing to upload.", BATCH_DIR)
        return

    logger.info("Found %d batch file(s) to upload.", len(batch_files))

    uploaded = 0
    failed = 0

    conn = get_connection()

    try:
        cursor = conn.cursor()

        for file_path in batch_files:

            put_command = (
                f"PUT file://{file_path.resolve()} "
                f"@TRANSACTION_STAGE "
                f"AUTO_COMPRESS=TRUE "
                f"OVERWRITE=FALSE;"   # Don't overwrite — idempotent by default
            )

            try:
                logger.info("Uploading → %s", file_path.name)
                cursor.execute(put_command)

                # PUT returns one row per file with a status column.
                # Statuses: UPLOADED, SKIPPED (already exists), FAILED.
                result = cursor.fetchone()
                status = result[6] if result else "UNKNOWN"     # col index 6 = status

                if status == "FAILED":
                    logger.error(
                        "PUT reported FAILED for %s — leaving in place for retry.",
                        file_path.name,
                    )
                    failed += 1
                    continue

                # Move to processed dir so this file is skipped on the next run
                dest = PROCESSED_DIR / file_path.name
                file_path.rename(dest)
                uploaded += 1

                logger.info(
                    "Staged %s (status=%s) → moved to %s",
                    file_path.name,
                    status,
                    PROCESSED_DIR,
                )

            except snowflake.connector.Error as exc:
                logger.error(
                    "Upload failed for %s: %s — skipping, will retry next run.",
                    file_path.name,
                    exc,
                )
                failed += 1

        cursor.close()

    finally:
        conn.close()

    logger.info(
        "Upload complete. uploaded=%d failed=%d", uploaded, failed
    )


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    upload_batches()