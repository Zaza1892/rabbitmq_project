# RabbitMQ Device Listener

A pub/sub pipeline that listens for device events on RabbitMQ, looks up which tenants a device belongs to via an API, and stores the result in Postgres.

## What it does

1. A message containing a `device_id` is published to a RabbitMQ queue (`device_events`)
2. A listener (`consumer.py`) picks up the message and extracts the `device_id`
3. It calls a tenant-lookup API with that `device_id`
4. The API returns a list of `tenant_ids`
5. The result is saved to a Postgres table, along with the original `message_id` and a timestamp

## Components

- **`consumer.py`** — the listener/consumer that reads from RabbitMQ, calls the API, and saves to Postgres
- **`mock_api.py`** — a mock/pretend API standing in for the real tenant-lookup service, used for local testing
- **`producer_test.py`** — a test script that publishes fake messages onto the queue, for testing the listener
- **`test_mock_api.py`** — an automated test for the mock API
- **`init.sql`** — creates the `device_lookups` table on first Postgres startup
- **`docker-compose.yml`** — runs RabbitMQ, Postgres, the mock API, and the consumer together
- **`Dockerfile`** — builds the consumer container
- **`Dockerfile.mockapi`** — builds the mock API container

