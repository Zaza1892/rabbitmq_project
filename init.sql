/*
init.sql

Creates the device_lookups table on first Postgres startup.

Each row represents one processed device event: the original
message_id and device_id from the RabbitMQ payload, the tenant_ids
returned by the tenant-lookup API, the full raw_payload as received,
and a timestamp of when it was processed.

message_id has a unique constraint (added separately via ALTER TABLE)
to prevent duplicate rows if RabbitMQ redelivers the same message.
*/

CREATE TABLE device_lookups (
    -- this creates a table named "device_lookups"
    id SERIAL PRIMARY KEY,
    --auto-incrementing unique id for each row
    message_id TEXT NOT NULL UNIQUE,
    --the original message_id from the RabbitMQ payload
    device_id TEXT NOT NULL,
    --the device_id  extracted
    tenant_ids JSONB NOT NULL,
    -- the list of tenant_ids, stored as a JSON colummn
    created_at TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    -- automatically records the date/time this row was inserted
    raw_payload JSONB
);