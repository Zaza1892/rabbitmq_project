from fastapi import FastAPI  # library used to build a simple web api 
from pydantic import BaseModel # lets me define what format incoming requests must have 
import random # used to generate made up tenant ids 
import hashlib # used to turn a device id into a repeatable number, so the same device always gets the same tenant 

############################################################

app= FastAPI()  ## create actual api application object , this is what uvicorn run 

class DeviceRequest(BaseModel): #defines what a incoming request looks like 
    device_id:str

############################################################

@app.post("/lookup") # this registers a new route,when a post request to lookup is sent , run the function below
def lookup_tenants(request: DeviceRequest):

    tenants = random.randint(1,3) #decide if the device belongs to 1 2 or 3 tenants 
    tenant_id = [f"tenant-{random.randint(1000,9999)}" for _ in range(tenants)] #list of fake tenant ids one for each number decided above 



    return{
        "device_id": request.device_id, # send back the same device_id we were given, so the caller knows which device this result is for

        "tenants":tenant_id #send back randomly generate list of tenanty ids
    }

############################################################
