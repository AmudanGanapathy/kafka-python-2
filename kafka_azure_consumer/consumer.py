"""Kafka consumer that uploads messages to Azure Blob Storage."""

import json  # For decoding JSON messages
import logging  # Standard logging library
import os  # For removing temporary files
import subprocess  # Used to invoke azcopy
from dataclasses import dataclass
from typing import Optional

from kafka import KafkaConsumer  # Kafka client library

# Configure root logger to output debug statements
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class ConsumerConfig:
    """Configuration options for :class:`KafkaAzureConsumer`."""

    # Address of Kafka brokers, e.g. 'localhost:9092'
    bootstrap_servers: str
    # Topic to subscribe to
    topic: str
    # Destination Azure Blob container SAS URL
    azure_container_url: str
    # Location of the azcopy binary
    azcopy_path: str = "azcopy"


class KafkaAzureConsumer:
    """Consume JSON messages from Kafka and upload them to Azure Blob storage."""

    def __init__(self, config: ConsumerConfig):
        """Initialize the consumer with the provided configuration."""

        # Store configuration for later use
        self.config = config
        # Will hold the KafkaConsumer instance once connected
        self.consumer: Optional[KafkaConsumer] = None

    def connect(self) -> None:
        """Create a ``KafkaConsumer`` instance and subscribe to the topic."""

        try:
            # Instantiate kafka consumer using provided configuration.
            # The value_deserializer converts bytes to JSON objects.
            self.consumer = KafkaConsumer(
                self.config.topic,
                bootstrap_servers=self.config.bootstrap_servers,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            )
            # Informational log message on success
            logger.info("Connected to Kafka topic %s", self.config.topic)
        except Exception:  # pragma: no cover - difficult to simulate in unit tests
            # Log unexpected exceptions and re-raise them
            logger.exception("Failed to connect to Kafka")
            raise

    def upload_file(self, source: str, destination: str) -> None:
        """Upload a local file to Azure using ``azcopy``."""

        # Build the azcopy command. ``--overwrite=true`` ensures existing blobs
        # are replaced so repeated uploads are safe.
        cmd = [self.config.azcopy_path, "copy", source, destination, "--overwrite=true"]
        try:
            # Invoke azcopy; ``check=True`` raises an exception if the command
            # exits with a non-zero status.
            subprocess.run(cmd, check=True)
            logger.info("Uploaded %s to %s", source, destination)
        except subprocess.CalledProcessError:
            # Log and propagate any azcopy errors to the caller
            logger.exception("azcopy failed for %s", source)
            raise

    def consume_and_upload(self) -> None:
        """Consume messages indefinitely and upload each record as a file."""

        # Lazily create the Kafka consumer if it hasn't been created yet
        if self.consumer is None:
            self.connect()
        # ``assert`` for static type checkers; ``self.consumer`` is not ``None``
        assert self.consumer is not None

        # Begin iterating over messages from Kafka
        for message in self.consumer:
            try:
                # Message values are JSON objects thanks to the deserializer
                data = message.value
                # Persist each message to a uniquely named file
                file_name = f"message_{message.offset}.json"
                with open(file_name, "w", encoding="utf-8") as handle:
                    json.dump(data, handle)

                # After writing locally, upload the file to Azure
                self.upload_file(file_name, self.config.azure_container_url)
                os.remove(file_name)
            except Exception:
                # Log any exception but continue consuming subsequent messages
                logger.exception(
                    "Error processing message offset %s", message.offset
                )

