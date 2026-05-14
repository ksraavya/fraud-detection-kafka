import copy
import json
import logging
import random
import uuid

from datetime import datetime, timezone, timedelta
from typing import Generator, Dict

import pandas as pd

from jsonschema import validate, ValidationError


logger = logging.getLogger(__name__)

SCHEMA_PATH = "kafka/schemas/transaction_schema.json"


def load_schema() -> Dict:
    """
    Load and return the transaction JSON schema from disk.
    Raises FileNotFoundError with a clear message if the path is wrong.
    """
    try:
        with open(SCHEMA_PATH, "r") as schema_file:
            return json.load(schema_file)
    except FileNotFoundError:
        raise FileNotFoundError(
            f"Transaction schema not found at '{SCHEMA_PATH}'. "
            "Check SCHEMA_PATH is correct relative to your working directory."
        )


def inject_anomaly(event: Dict) -> Dict:
    """
    Return a *copy* of the event with synthetic anomaly behaviour injected.
    Does not mutate the original dict — safe for concurrent / batch use.

    Anomaly types
    -------------
    high_amount     — multiplies amount to simulate unusually large transfers
    rapid_repeat    — elevates transaction_velocity to flag burst activity
    merchant_spike  — routes transaction to a known high-risk merchant ID
    """
    event = copy.deepcopy(event)

    anomaly_type = random.choice([
        "high_amount",
        "rapid_repeat",
        "merchant_spike",
    ])

    event["synthetic_anomaly"] = anomaly_type
    event["is_synthetic_anomaly"] = 1

    if anomaly_type == "high_amount":
        event["amount"] = round(event["amount"] * random.randint(15, 40), 2)

    elif anomaly_type == "rapid_repeat":
        # Simulates a card being used many times in a short window.
        # Downstream consumer / dbt model should flag velocity >= threshold.
        event["transaction_velocity"] = random.randint(15, 40)

    elif anomaly_type == "merchant_spike":
        event["merchant_id"] = "HIGH_RISK_MERCHANT"

    return event


def load_transactions(
    file_path: str,
    sample_size: int = 1000,
    anomaly_probability: float = 0.03,
) -> Generator[Dict, None, None]:
    """
    Read the PaySim CSV, optionally sample rows, inject anomalies, validate
    each event against the JSON schema, and yield valid events one at a time.

    Parameters
    ----------
    file_path : str
        Path to the PaySim CSV file.
    sample_size : int | None
        Number of rows to sample randomly. Pass None to use the full dataset.
    anomaly_probability : float
        Fraction of events that receive a synthetic anomaly (0.0 – 1.0).

    Yields
    ------
    Dict
        A validated transaction event ready to be produced to Kafka.
    """
    if not 0.0 <= anomaly_probability <= 1.0:
        raise ValueError(
            f"anomaly_probability must be between 0.0 and 1.0, "
            f"got {anomaly_probability}"
        )

    schema = load_schema()
    base_time = datetime.now(timezone.utc)  # utcnow() is deprecated in 3.12+

    df = pd.read_csv(file_path)

    if sample_size is not None:
        if sample_size > len(df):
            logger.warning(
                "sample_size=%d exceeds dataset length=%d — using full dataset",
                sample_size,
                len(df),
            )
            sample_size = len(df)

        # Random sample so we don't always bias toward the start of the file
        start_idx = random.randint(0, len(df) - sample_size)
        df = df.iloc[start_idx:start_idx + sample_size]

    skipped = 0

    for _, row in df.iterrows():

        event_timestamp = (
            base_time + timedelta(hours=int(row["step"]))
        ).isoformat()

        now_iso = datetime.now(timezone.utc).isoformat()

        event = {
            # ── Operational Metadata ──────────────────────────────────────────
            "event_id":          str(uuid.uuid4()),
            "event_created_at":  now_iso,
            "event_timestamp":   event_timestamp,
            "pipeline_source":   "paysim_dataset",
            "ingestion_mode":    "streaming",

            # ── Transaction Data ──────────────────────────────────────────────
            "transaction_id":    str(uuid.uuid4()),
            "step":              int(row["step"]),
            "transaction_type":  row["type"],
            "amount":            round(float(row["amount"]), 2),
            "customer_id":       row["nameOrig"],
            "merchant_id":       row["nameDest"],
            "old_balance_orig":  float(row["oldbalanceOrg"]),
            "new_balance_orig":  float(row["newbalanceOrig"]),
            "old_balance_dest":  float(row["oldbalanceDest"]),
            "new_balance_dest":  float(row["newbalanceDest"]),
            "is_fraud":          int(row["isFraud"]),

            # ── Enrichment Fields (defaults) ──────────────────────────────────
            "synthetic_anomaly":     None,
            "is_synthetic_anomaly":  0,
            "transaction_velocity":  1,
        }

        if random.random() < anomaly_probability:
            event = inject_anomaly(event)

        try:
            validate(instance=event, schema=schema)
            yield event

        except ValidationError as exc:
            # Log with enough context to debug the offending row without
            # crashing the stream. Increment a counter so callers can detect
            # systematic schema drift (e.g. a CSV column rename).
            skipped += 1
            logger.error(
                "Schema validation failed (skipped=%d) for "
                "transaction_id=%s: %s",
                skipped,
                event.get("transaction_id", "UNKNOWN"),
                exc.message,
            )

    if skipped:
        logger.warning(
            "load_transactions complete — %d event(s) failed schema "
            "validation and were dropped. Review schema or CSV columns.",
            skipped,
        )