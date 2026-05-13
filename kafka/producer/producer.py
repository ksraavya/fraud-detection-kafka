import json
import logging
import random
import time

from confluent_kafka import Producer, KafkaException

from transaction_generator import load_transactions


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

logger = logging.getLogger(__name__)


TOPIC_NAME = "raw_transactions"

# How often to log progress inside the hot loop (every N events).
LOG_EVERY_N = 50

KAFKA_CONFIG = {
    "bootstrap.servers": "localhost:9092",

    # Durability: wait for all in-sync replicas to acknowledge
    "acks": "all",

    # Idempotent delivery — prevents duplicates on retry
    "enable.idempotence": True,

    # Explicit retry config (idempotence requires retries > 0;
    # making it visible here keeps behaviour documented and predictable)
    "retries": 5,
    "retry.backoff.ms": 300,

    # Small batching window — acceptable latency for a streaming pipeline
    "linger.ms": 10,

    # Snappy gives ~40-60 % size reduction on JSON with negligible CPU cost
    "compression.type": "snappy",
}


def delivery_report(err, msg):
    """
    Kafka delivery callback — fired asynchronously by producer.poll().
    Logs failures at ERROR so they surface in any log aggregator.
    """
    if err is not None:
        logger.error(
            "Delivery failed for key=%s: %s",
            msg.key().decode("utf-8") if msg.key() else "N/A",
            err,
        )
    else:
        logger.debug(
            "Delivered → topic=%s partition=[%d] offset=%d",
            msg.topic(),
            msg.partition(),
            msg.offset(),
        )


def get_stream_delay() -> float:
    """
    Return a randomised inter-message delay that mimics realistic
    transaction arrival patterns:
      - 75 % normal   → 0.1 - 0.4 s
      - 15 % burst    → 0.01 - 0.05 s
      - 10 % quiet    → 1.0 - 2.0 s
    """
    stream_mode = random.choices(
        population=["normal", "burst", "quiet"],
        weights=[0.75, 0.15, 0.10],
    )[0]

    if stream_mode == "burst":
        return random.uniform(0.01, 0.05)
    elif stream_mode == "quiet":
        return random.uniform(1.0, 2.0)
    return random.uniform(0.1, 0.4)


def main():
    producer = Producer(KAFKA_CONFIG)

    transaction_stream = load_transactions(
        file_path="data/raw/PaySim Dataset.csv",
        sample_size=1000,
        anomaly_probability=0.03,
    )

    logger.info("Starting transaction stream → topic: %s", TOPIC_NAME)

    sent = 0
    failed = 0

    try:
        for event in transaction_stream:
            # Encode the partition key to bytes
            key_bytes = event["customer_id"].encode("utf-8")

            try:
                producer.produce(
                    topic=TOPIC_NAME,
                    key=key_bytes,
                    value=json.dumps(event).encode("utf-8"),
                    callback=delivery_report,
                )
                sent += 1

            except BufferError:
                # Local producer queue is full — poll to drain callbacks
                # then retry once before dropping the event
                logger.warning(
                    "Producer queue full — draining and retrying "
                    "transaction_id=%s",
                    event["transaction_id"],
                )
                producer.poll(1)

                try:
                    producer.produce(
                        topic=TOPIC_NAME,
                        key=key_bytes,
                        value=json.dumps(event).encode("utf-8"),
                        callback=delivery_report,
                    )
                    sent += 1
                except KafkaException:
                    logger.error(
                        "Retry failed — dropping transaction_id=%s",
                        event["transaction_id"],
                    )
                    failed += 1

            except KafkaException as exc:
                logger.error(
                    "Produce error for transaction_id=%s: %s",
                    event["transaction_id"],
                    exc,
                )
                failed += 1

            # poll(0) is non-blocking; use a small timeout so we actually
            # drain the callback queue rather than just checking it.
            producer.poll(0.01)

            delay = get_stream_delay()

            # Log every N events to avoid flooding stdout at high throughput
            if sent % LOG_EVERY_N == 0:
                logger.info(
                    "Progress: sent=%d failed=%d | latest → "
                    "transaction_id=%s amount=%.2f anomaly=%s delay=%.2fs",
                    sent,
                    failed,
                    event["transaction_id"],
                    event["amount"],
                    event["synthetic_anomaly"],
                    delay,
                )

            time.sleep(delay)

    except KeyboardInterrupt:
        logger.info("Stream interrupted by user.")

    finally:
        logger.info("Flushing producer buffer...")
        producer.flush()
        logger.info(
            "Stream complete. sent=%d failed=%d", sent, failed
        )


if __name__ == "__main__":
    main()