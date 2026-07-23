import pika # talks to rabbitmq
import json # converts json text and python data 
import requests # make http calls to the mock api
import os
import psycopg2 # talks to postgres
from datetime import datetime ,timezone # lets us record the current date/time
import time 

############################################################

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
API_URL = os.getenv("API_URL", "http://127.0.0.1:8000/lookup") 

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "device_lookups")
DB_USER = os.getenv("DB_USER", "appuser")
DB_PASSWORD = os.getenv("DB_PASSWORD", "apppassword")

############################################################

def save_to_db(message_id, device_id, tenant_ids,raw_payload):

    """
    Inserts one row into device_lookups. Uses ON CONFLICT DO NOTHING
    so that redelivered messages (same message_id) don't create
    duplicate rows.

    Uses "with" (context managers) for both the connection and cursor,
    so they're guaranteed to close automatically even if an error
    happens partway through, instead of relying on manual .close() calls.
    """


    with psycopg2.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )  as conn:
        with conn.cursor() as cur:
      

         cur.execute(
         "INSERT INTO device_lookups (message_id, device_id, tenant_ids, created_at,raw_payload) VALUES (%s,%s, %s, %s, %s) ON CONFLICT (message_id) DO NOTHING",
          #  if this message_id already exists in the table, skip it instead of throwing an error       
          (message_id, device_id, json.dumps(tenant_ids), datetime.now(timezone.utc),json.dumps(raw_payload))
        # ^ json.dumps converts our python list into a JSON string, which JSONB can store
     )

    conn.commit()  
      
############################################################

def handle_messages(channel,method,properties,body):  

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
    
    payload=json.loads(body) 
    print("Message received: ",payload)

    device_id=payload["device_id"]  
    message_id=payload["message_id"]  

    response=requests.post(   
        API_URL,  
        json={"device_id": device_id},   
        timeout=5 # set a time so that it does not hang forever
    )
    response.raise_for_status() 

    result=response.json()
    tenant_ids = result["tenants"] 
    print("Tenant lookup result:",result) #

    save_to_db(message_id, device_id, tenant_ids,payload)
    print("Saved to database.")

    channel.basic_ack(delivery_tag=method.delivery_tag)


 except (json.JSONDecodeError, KeyError) as e:
        #  message itself is bad JSON, or missing a required field , retrying won't ever fix this, so scrap it 
        print(f"Bad message,: {e}. Body: {body}")
        channel.basic_reject(delivery_tag=method.delivery_tag, requeue=False)

 except Exception as e:
        #  issues like API down, database down, unexpected errors , will requeue and let rabbitmq retry 
        print(f"Temp error , will retry: {e}")
        channel.basic_reject(delivery_tag=method.delivery_tag, requeue=True)
    
############################################################

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
 while not connected and attempts < 10: # 
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST)) # try to connect
        connected = True  
    except pika.exceptions.AMQPConnectionError:  
        attempts += 1  
        print(f"RabbitMQ not ready yet, retrying... (attempt {attempts})")  
        time.sleep(5)  
 if not connected:
    print("Could not connect after 10 attempts, exiting")
    exit(1) 


 channel=connection.channel()  

 channel.queue_declare(queue="device_events",durable=True)  

 channel.basic_consume(queue="device_events",on_message_callback=handle_messages)

 print("listening for messages on device events") 
 channel.start_consuming() 


if __name__ == "__main__":
    main()

############################################################
