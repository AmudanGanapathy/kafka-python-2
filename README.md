# Kafka Python Consumer to Azure

This repository provides a small example application that consumes JSON
messages from a Kafka topic and uploads each message to Azure Blob Storage
using `azcopy`.

## Running the consumer

```
python -m kafka_azure_consumer.consumer
```

Configure the connection details by editing the `ConsumerConfig` instance in
your own script.

## Running tests

Tests use the built in `unittest` framework:

```
python -m unittest discover -v
```
