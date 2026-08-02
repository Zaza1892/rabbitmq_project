import pika
import json
import uuid
import argparse


def main():
    """
    Publishes a single test message onto the "device_events" queue.

    By default, both message_id and device_id are randomly generated.
    Pass --device-id to send a fixed device_id instead, which is useful
    for testing repeatable behavior (e.g. confirming the mock API
    returns the same tenants for the same device every time).
    """

    parser = argparse.ArgumentParser()
    parser.add_argument("--device-id", type=str, default=None, help="Fixed device_id  ")
    args = parser.parse_args()

    connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
    channel = connection.channel()

    channel.queue_declare(queue="device_events", durable=True)

    message = {
        "message_id": str(uuid.uuid4()),
        "device_id": (
            args.device_id if args.device_id else f"device-{uuid.uuid4().hex[:6]}"
        ),
    }

    channel.basic_publish(
        exchange="",
        routing_key="device_events",
        body=json.dumps(message),
        properties=pika.BasicProperties(
            delivery_mode=2
        ),  # marked as persistent so it survives a RabbitMq restart
    )

    print("Sent:", message)

    connection.close()


if __name__ == "__main__":
    main()
