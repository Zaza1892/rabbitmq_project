# RabbitMQ Device Listener

A pub/sub pipeline that listens for device events on RabbitMQ, looks up which tenants a device belongs to via an API, and stores the result in Postgres.

## What it does

1. A message containing a device_id and message_id is published to a RabbitMQ queue called device_events
2. A listener (consumer.py) picks up the message and extracts the device_id
3. It calls a tenant-lookup API with that device_id
4. The API returns a list of tenant_ids
5. The result is saved to a Postgres table, along with the original message_id, the full raw payload, and a timestamp

## Components

- consumer.py: the listener/consumer that reads from RabbitMQ, calls the API, and saves to Postgres
- mock_api.py: a mock/pretend API standing in for the real tenant-lookup service, used for local testing. It's deterministic, meaning the same device_id always returns the same tenant_ids.
- producer_test.py: a test script that publishes fake messages onto the queue. Supports a device-id flag to send a fixed device_id instead of a random one.
- test_mock_api.py: an automated test for the mock API
- init.sql: creates the device_lookups table on first Postgres startup
- docker-compose.yml: runs RabbitMQ, Postgres, the mock API, and the consumer together, with healthchecks so the consumer waits until each dependency is genuinely ready
- Dockerfile: builds the consumer container
- Dockerfile.mockapi: builds the mock API container

## Prerequisites

- Docker Desktop installed and running
- Python 3.11+ with a virtual environment set up
- Dependencies installed using pip install -r requirements.txt

## Running it


Before running Docker Compose for the first time, copy the example environment file:

copy .env.example .env

Then start everything:

docker compose up --build -d

This starts all 4 containers: RabbitMQ, Postgres, the mock API, and the consumer. Docker healthchecks ensure the consumer only starts once RabbitMQ, Postgres, and the mock API are all confirmed healthy.
## Sending a test message

python producer_test.py

Or with a fixed device_id, useful for testing repeatable behavior:

python producer_test.py --device-id device-123

## Verifying it worked

Check the consumer processed the message:

docker logs consumer --tail 20

You should see Message received, Tenant lookup result, and Saved to database.

Note: avoid running docker logs consumer without tail if the consumer has been retrying or looping.
## Checking the database

Connect via terminal:

docker exec -it postgres psql -U appuser -d device_lookups

Then inspect the data:

SELECT * FROM device_lookups;

Or connect with a GUI tool like DBeaver, using:
Host: localhost
Port: 5432
Database: device_lookups
Username: appuser
Password: apppassword

## Running automated tests

pytest -s

This runs the full test suite: test_mock_api.py (verifies the mock API's responses and validation behavior) and consumer_test.py (verifies message handling, error classification, retries, and database insert behavior). test_producer.py verifies generated message IDs are full length.

## Error handling behavior

Permanently broken messages, such as invalid JSON or missing required fields, are rejected and not requeued, since retrying won't fix them.

Temporary failures, such as the API or database being down, or other unexpected errors, are rejected with requeue, so RabbitMQ will redeliver the message for a later retry.

Duplicate messages, meaning the same message_id delivered more than once, are silently skipped at the database level via a unique constraint, so redelivery from RabbitMQ won't create duplicate rows.

## Known limitations and things not yet implemented


Postgres data does not currently persist across docker compose down, since no volume is configured for the database yet.

Database credentials are stored in a local .env file, which is not committed to version control. A .env.example file with placeholder values is included so the shape of the required settings is visible; copy it to .env before running the project.

Database connections in consumer.py open a fresh connection per message rather than using a pooled connection. This is acceptable at low volume, but worth revisiting for higher throughput.

The tenant-lookup API is currently mocked. Swap the API_URL environment variable in docker-compose.yml to point at the real API once it's available.