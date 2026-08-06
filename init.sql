/*
init.sql

Creates the device_lookups table on first Postgres startup.

Each row represents one processed device event: the original
message_id and device_id from the RabbitMQ payload, the tenant_ids
returned by the tenant-lookup API, the full raw_payload as received,
and a timestamp of when it was processed.

message_id has a unique constraint (defined inline below)
to prevent duplicate rows if RabbitMQ redelivers the same message.
*/

CREATE TABLE device_lookups (
     id SERIAL PRIMARY KEY,
     message_id TEXT NOT NULL UNIQUE,
     device_id TEXT NOT NULL,
     tenant_ids JSONB NOT NULL,
     created_at TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
     raw_payload JSONB,
     ai_analysis TEXT
);