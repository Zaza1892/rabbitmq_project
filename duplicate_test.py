import pika
import json

connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
channel = connection.channel()
channel.queue_declare(queue="device_events", durable=True)

# setting a fied msg so we can use it as duplicate example
message = {"message_id": "duplicate-test-001", "device_id": "device-dup-test"}

channel.basic_publish(
    exchange="",
    routing_key="device_events",
    body=json.dumps(message)
)

print("Sent:", message)
connection.close()