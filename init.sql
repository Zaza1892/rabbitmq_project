CREATE TABLE device_lookups (
    -- this creates a table named "device_lookups"
    id SERIAL PRIMARY KEY,
    --auto-incrementing unique id for each row
    message_id TEXT NOT NULL,
    --the original message_id from the RabbitMQ payload
    device_id TEXT NOT NULL,
    --the device_id  extracted
    tenant_ids JSONB NOT NULL,
    -- the list of tenant_ids, stored as a JSON colummn
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
    -- automatically records the date/time this row was inserted
);