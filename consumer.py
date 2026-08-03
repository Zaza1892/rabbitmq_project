import pika
import json
import requests
import os
import psycopg2
from datetime import datetime, timezone
import time
from psycopg2 import pool

AI_API_URL = os.getenv("AI_API_URL", "http://192.168.1.99:8888/v1/chat/completions")


RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
API_URL = os.getenv("API_URL", "http://127.0.0.1:8000/lookup")
MAX_RETRIES = 5
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "device_lookups")
DB_USER = os.getenv("DB_USER", "appuser")
DB_PASSWORD = os.getenv("DB_PASSWORD", "apppassword")
db_pool = None


def get_db_pool():

    global db_pool
    if db_pool is None:
        db_pool = psycopg2.pool.SimpleConnectionPool(
            1, 10, host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
        )
    return db_pool


def save_to_db(message_id, device_id, tenant_ids, raw_payload, ai_analysis):
    """
     Inserts one row into device_lookups. Uses ON CONFLICT DO NOTHING
     so that redelivered messages (same message_id) don't create
     duplicate rows.

    Uses a connection pool instead of opening a brand new connection
     every time, reusing a small set of already-open connections for
     better performance at higher message volumes.
    """

    pool = get_db_pool()
    conn = pool.getconn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO device_lookups (message_id, device_id, tenant_ids, created_at,raw_payload, ai_analysis) VALUES (%s,%s,%s, %s, %s, %s) ON CONFLICT (message_id) DO NOTHING",
                    (
                        message_id,
                        device_id,
                        json.dumps(tenant_ids),
                        datetime.now(timezone.utc),
                        json.dumps(raw_payload),
                        ai_analysis,
                    ),
                )

    finally:
        pool.putconn(conn)


def analyzeContent(device_id, tenant_ids, raw_payload):
    prompt = (
        f"A device event was just processed. Device ID: {device_id}."
        f"Tenants found: {tenant_ids}. Raw payload: {raw_payload}."
        f"Does anything here look unusual, missing, or potentially broken?"
        f"Answer briefly in plain english."
    )

    try:
        response = requests.post(
            AI_API_URL,
            json={
                "model": "gemma-4",
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=10,
        )
        response.raise_for_status()
        result = response.json()
        return result["choices"][0]["message"]["content"]

    except Exception as e:
        print(f"AI analysis failed, continuing without it :{e}")
        return None


def handle_messages(channel, method, properties, body):
    """
    Runs once per message received from RabbitMQ.

    Happy path: parses the payload, extracts device_id and message_id,
    calls the tenant-lookup API, saves the result to Postgres, then acks.

    Error handling: permanently broken messages (bad JSON, missing
    fields) are rejected without requeue, since retrying won't fix them.
    Everything else (API down, DB down, unexpected errors) is rejected
    WITH requeue, since those failures might just be temporary.
    """

    try:

        payload = json.loads(body)

        retry_count = 0
        if properties and properties.headers:
            retry_count = properties.headers.get("x-retry-count", 0)
        print("Message received: ", payload)

        device_id = payload["device_id"]
        message_id = payload["message_id"]

    except (json.JSONDecodeError, KeyError) as e:
        #  message itself is bad JSON, or missing a required field , retrying won't ever fix this, so scrap it
        print(f"Bad message,: {e}. Body: {body}")
        channel.basic_reject(delivery_tag=method.delivery_tag, requeue=False)
        return

    try:
        response = requests.post(API_URL, json={"device_id": device_id}, timeout=5)
        response.raise_for_status()

        result = response.json()
        tenant_ids = result["tenants"]
        print("Tenant lookup result:", result)

        ai_analysis = analyzeContent(device_id, tenant_ids, payload)
        if ai_analysis:
            print("AI analysis:", ai_analysis)

        save_to_db(message_id, device_id, tenant_ids, payload, ai_analysis)
        print("Saved to database.")

        channel.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        # covers API failures, bad/unexpected API responses, database issues, or any other unexpected error
        # the original RabbitMQ message was fine, so this is retried rather than dropped
        if retry_count < MAX_RETRIES:
            print(f"Temp error,retrying(attempt{retry_count + 1}/{MAX_RETRIES}):")
            channel.basic_publish(
                exchange="",
                routing_key="device_events",
                body=body,
                properties=pika.BasicProperties(
                    headers={"x-retry-count": retry_count + 1}
                ),
            )
            channel.basic_ack(delivery_tag=method.delivery_tag)

        else:
            # retries exhausted, move it to the dead-letter queue instead of retrying forever
            print(f"max retries exceeded ,moving to dead letter queue: {e}")
            channel.basic_publish(
                exchange="",
                routing_key="device_events_dead",
                body=body,
                properties=properties,
            )
            channel.basic_ack(delivery_tag=method.delivery_tag)


def main():
    """
    Connects to RabbitMQ (retrying up to 10 times if it's not ready yet),
    then starts consuming messages from the device_events queue forever.

    Wrapped in main() and guarded by "if __name__ == '__main__'" so this
    file can be safely imported elsewhere (e.g. by pytest) without
    actually triggering a real RabbitMQ connection as a side effect.
    """

    connected = False
    attempts = 0
    while not connected and attempts < 10:  #
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=RABBITMQ_HOST)
            )
            connected = True
        except pika.exceptions.AMQPConnectionError:
            attempts += 1
            print(f"RabbitMQ not ready yet, retrying... (attempt {attempts})")
            time.sleep(5)
    if not connected:
        print("Could not connect after 10 attempts, exiting")
        exit(1)

    channel = connection.channel()

    channel.queue_declare(queue="device_events", durable=True)
    channel.queue_declare(queue="device_events_dead", durable=True)

    channel.basic_consume(queue="device_events", on_message_callback=handle_messages)

    print("listening for messages on device events")
    channel.start_consuming()


if __name__ == "__main__":
    main()
