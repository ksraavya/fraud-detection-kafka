import json
import logging

from confluent_kafka import Consumer, KafkaException, KafkaError

from batch_writer import write_batch


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

logger = logging.getLogger(__name__)


TOPIC_NAME = "raw_transactions"
BATCH_SIZE = 100

KAFKA_CONFIG = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "transaction_batch_consumers",
    "auto.offset.reset": "earliest",
    # Commit offsets manually — only after the batch is durably persisted
    "enable.auto.commit": False,
}


def flush_batch(consumer: Consumer, batch: list) -> None:
    """
    Persist the current batch to disk, commit offsets, then clear the batch.
    Commit is intentionally placed after a successful write — if write_batch()
    raises, offsets are not advanced and the batch will be reprocessed.
    """
    if not batch:
        return

    file_path = write_batch(batch)

    # Only commit once we know the data landed on disk
    consumer.commit()

    logger.info(
        "Persisted batch of %d events → %s", len(batch), file_path
    )

    batch.clear()


def main():
    consumer = Consumer(KAFKA_CONFIG)
    consumer.subscribe([TOPIC_NAME])

    logger.info("Subscribed to topic → %s", TOPIC_NAME)

    batch = []
    total_consumed = 0
    total_batches = 0
    total_errors = 0

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                error = msg.error()

                # PARTITION_EOF is informational — the consumer has caught up
                # to the end of a partition. Not a real error; just continue.
                if error.code() == KafkaError._PARTITION_EOF:
                    logger.debug(
                        "Reached end of partition %s [%d]",
                        msg.topic(),
                        msg.partition(),
                    )
                    continue

                # For all other errors, log and increment counter.
                # Only raise for fatal errors that cannot be recovered from.
                total_errors += 1
                logger.error(
                    "Consumer error (total_errors=%d): %s", total_errors, error
                )

                if error.fatal():
                    raise KafkaException(error)

                continue

            try:
                event = json.loads(msg.value().decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                total_errors += 1
                logger.error(
                    "Failed to deserialize message at offset %d "
                    "(total_errors=%d): %s",
                    msg.offset(),
                    total_errors,
                    exc,
                )
                continue

            batch.append(event)
            total_consumed += 1

            if len(batch) >= BATCH_SIZE:
                flush_batch(consumer, batch)
                total_batches += 1

    except KeyboardInterrupt:
        logger.info("Consumer interrupted by user.")

    finally:
        # Flush any remaining events that didn't fill a complete batch
        if batch:
            logger.info(
                "Flushing tail batch of %d remaining event(s)...", len(batch)
            )
            try:
                flush_batch(consumer, batch)
                total_batches += 1
            except Exception as exc:
                logger.error("Failed to flush tail batch: %s", exc)

        consumer.close()

        logger.info(
            "Consumer closed. total_consumed=%d total_batches=%d total_errors=%d",
            total_consumed,
            total_batches,
            total_errors,
        )


if __name__ == "__main__":
    main()