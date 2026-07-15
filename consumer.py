import pika #pika is the library that lets python talk to rabbitmq
import json #library that converts json text into python data and back
import requests # library to make http calles to the mockk api
import os
import psycopg2 # library that lets python talk to postgres
from datetime import datetime # lets us record the current date/time
import time 


RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
API_URL = os.getenv("API_URL", "http://127.0.0.1:8000/lookup")

#  read db connection details from environment variables
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "device_lookups")
DB_USER = os.getenv("DB_USER", "appuser")
DB_PASSWORD = os.getenv("DB_PASSWORD", "apppassword")

def save_to_db(message_id, device_id, tenant_ids,raw_payload):
    # this function opens a connection, inserts one row, then closes the connection
    conn = psycopg2.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    ) # open a connection to postgres using our env vars

    cur = conn.cursor() # a "cursor" lets us execute SQL commands

    cur.execute(
        "INSERT INTO device_lookups (message_id, device_id, tenant_ids, created_at,raw_payload) VALUES (%s,%s, %s, %s, %s)",
        # ^ %s are placeholders - psycopg2 safely inserts our actual values in place of them
        (message_id, device_id, json.dumps(tenant_ids), datetime.utcnow(),json.dumps(raw_payload))
        # ^ json.dumps converts our python list into a JSON string, which JSONB can store
    )

    conn.commit() # actually save the change to the database
    cur.close() # close the cursor
    conn.close() # close the connection

def handle_messages(channel,method,properties,body): # this function runs each time a messages arrives on the queue
    payload=json.loads(body)#convert message bytes into a python dictionary
    print("Message recieved: ",payload)# print statement to see it worked

    device_id=payload["device_id"] # pull device_id out of the payload
    message_id=payload["message_id"] # pull message_id out of the payload too, need it for the db

    response=requests.post(  # call mock api over http
        API_URL, # mock apis address and route
        json={"device_id":device_id} # send device_id as the request body
    )

    result=response.json()# convert apis json response into a python dictiornary
    tenant_ids = result["tenants"] # pull just the tenant_ids list out of the api response

    print("Tenant lookup result:",result) #log what the api gave back

    save_to_db(message_id, device_id, tenant_ids,payload) #  save this record to postgres
    print("Saved to database.") # confirm it saved

    channel.basic_ack(delivery_tag=method.delivery_tag) # tell rabbitMQ , handled the message



connected = False # tracks whether successfully connected yet
attempts = 0 # counts how many times tried

while not connected and attempts < 10: # try up to 10 times
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST)) # try to connect
        connected = True # if the line above didn't crash,, connected
    except pika.exceptions.AMQPConnectionError: # kept seeing this error 
        attempts += 1 # count this failed attempt
        print(f"RabbitMQ not ready yet, retrying... (attempt {attempts})") # log to see when it is  retrying
        time.sleep(5) # wait 5 seconds before trying again

channel=connection.channel() # the chanel is in the connection where i  actually send and recieve the messages

channel.queue_declare(queue="device_events",durable=True) # ensure the queue exists

channel.basic_consume(queue="device_events",on_message_callback=handle_messages)#Tell RabbitMQ that whenever a message lands on this queue , it calles a handle message with it

print("listening for messages on device events")# a message that prints so i know that the scprit is active
channel.start_consuming()# this is the actual listening part