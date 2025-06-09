import json
import unittest
from unittest import mock

from kafka_azure_consumer.consumer import ConsumerConfig, KafkaAzureConsumer


class KafkaAzureConsumerTestCase(unittest.TestCase):
    """Unit tests for KafkaAzureConsumer."""

    @mock.patch("kafka_azure_consumer.consumer.KafkaConsumer")
    def test_connect_creates_consumer(self, mock_consumer_cls):
        config = ConsumerConfig(
            bootstrap_servers="localhost:9092",
            topic="test",
            azure_container_url="https://example.com/container",
            azcopy_path="azcopy",
        )
        consumer = KafkaAzureConsumer(config)
        consumer.connect()
        mock_consumer_cls.assert_called_once()
        args, kwargs = mock_consumer_cls.call_args
        self.assertIn(config.topic, args)
        self.assertEqual(kwargs["bootstrap_servers"], config.bootstrap_servers)
        self.assertTrue(callable(kwargs["value_deserializer"]))

    @mock.patch("subprocess.run")
    def test_upload_file_invokes_azcopy(self, mock_run):
        config = ConsumerConfig(
            bootstrap_servers="b",
            topic="t",
            azure_container_url="dest",
            azcopy_path="azcopy",
        )
        consumer = KafkaAzureConsumer(config)
        consumer.upload_file("src.json", "dest")
        mock_run.assert_called_once()
        cmd, = mock_run.call_args[0]
        self.assertEqual(cmd[0], config.azcopy_path)
        self.assertIn("--overwrite=true", cmd)

    @mock.patch("kafka_azure_consumer.consumer.open", new_callable=mock.mock_open)
    @mock.patch.object(KafkaAzureConsumer, "upload_file")
    def test_consume_and_upload_processes_messages(self, mock_upload, mock_open):
        # Mock consumer to yield two messages
        fake_messages = [
            mock.Mock(value={"a": 1}, offset=0),
            mock.Mock(value={"b": 2}, offset=1),
        ]
        config = ConsumerConfig(
            bootstrap_servers="b",
            topic="t",
            azure_container_url="dest",
        )
        consumer = KafkaAzureConsumer(config)
        consumer.consumer = fake_messages  # directly assign iterable
        consumer.consume_and_upload()
        self.assertEqual(mock_upload.call_count, 2)
        self.assertEqual(mock_open.call_count, 2)

    def test_import_without_kafka_package_uses_stub(self):
        """Module should provide a stub ``KafkaConsumer`` when ``kafka`` is absent."""

        import importlib
        import sys

        # Ensure a clean import of the consumer module
        sys.modules.pop("kafka_azure_consumer.consumer", None)

        with mock.patch.dict(sys.modules, {"kafka": None}):
            module = importlib.import_module("kafka_azure_consumer.consumer")
            importlib.reload(module)

        config = module.ConsumerConfig(
            bootstrap_servers="b",
            topic="t",
            azure_container_url="dest",
        )
        consumer = module.KafkaAzureConsumer(config)
        # Should succeed using the stub KafkaConsumer
        consumer.connect()
        self.assertIsInstance(consumer.consumer, module.KafkaConsumer)


if __name__ == "__main__":
    unittest.main()

