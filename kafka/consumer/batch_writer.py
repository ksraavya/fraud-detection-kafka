import json
import logging
import uuid

from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict


logger = logging.getLogger(__name__)

BATCH_OUTPUT_DIR = Path("data/stream_batches")


def _ensure_output_dir() -> None:
    """Create the batch output directory if it doesn't exist."""
    BATCH_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def write_batch(events: List[Dict]) -> Path:
    """
    Persist a batch of transaction events as NDJSON (one JSON object per line).

    The filename includes a UTC timestamp and a UUID suffix to guarantee
    uniqueness even when called multiple times within the same second
    (e.g. during burst-mode streaming).

    A partial file is cleaned up automatically if the write fails mid-way,
    so the caller never commits Kafka offsets against a corrupt file.

    Parameters
    ----------
    events : List[Dict]
        Transaction event dicts to write.

    Returns
    -------
    Path
        Absolute path of the written file.

    Raises
    ------
    OSError
        If the file cannot be created or written to. Partial file is deleted
        before raising so the caller can safely retry.
    """
    _ensure_output_dir()

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    uid = uuid.uuid4().hex[:8]          # 8 hex chars — enough to prevent collision
    file_name = f"batch_{timestamp}_{uid}.ndjson"
    file_path = BATCH_OUTPUT_DIR / file_name

    try:
        with open(file_path, "w", encoding="utf-8") as batch_file:
            for event in events:
                batch_file.write(json.dumps(event) + "\n")

    except OSError as exc:
        # Clean up the partial file so the caller never sees a corrupt batch
        if file_path.exists():
            file_path.unlink(missing_ok=True)
            logger.error(
                "Write failed — partial file removed: %s | error: %s",
                file_path,
                exc,
            )
        raise

    return file_path