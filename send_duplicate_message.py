"""
send_duplicate_message.py

A one-off testing script (not part of the automated pytest suite) used
to manually verify that duplicate messages are handled correctly.

Sends the exact same message_id every time it's run, so you can run it
twice in a row and confirm that the second insert gets silently skipped
by the ON CONFLICT DO NOTHING clause in consumer.py's save_to_db,
instead of creating a duplicate row.
"""


import pika
import json

connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
channel = connection.channel()
channel.queue_declare(queue="device_events", durable=True)

message = {"message_id": "duplicate-test-001", "device_id": "device-dup-test"}

channel.basic_publish(
    exchange="",
    routing_key="device_events",
    body=json.dumps(message)
)

print("Sent:", message)
connection.close()