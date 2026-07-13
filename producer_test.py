import pika #library to talk to RabbitMQ
import json #converts python dictionaery into json text 

connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost")) #connect to rabbitmq on this machine 
channel = connection.channel() #open chanel to send messages on 

channel.queue_declare(queue="device_events",durable=True) # make sure queue exists and is same name as consumer.py

message ={"message_id": "msg-001", #fake id for this message
          "device_id": "device-999" # the device id our consumer will read out
          }

channel.basic_publish( 
    exchange="",# empty string which gets sent directly to queue  
    routing_key="device_events",# the queue name we're sending to 
    body=json.dumps(message)  # convert python dict into json before sending 

)

print("Sent:",message) #confirm what we sent 


connection.close() # close connection once done