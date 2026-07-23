from fastapi import FastAPI  
from pydantic import BaseModel  
import random 
import hashlib # used to turn a device id into a repeatable number, so the same device always gets the same tenant 

############################################################

app= FastAPI()  
@app.get("/health")
def health():

    """
    Basic health check endpoint. Used by Docker's healthcheck to
    confirm this service is actually up and responding, not just
    that the container has started.
    """

    return {"status":"ok"}
    
class DeviceRequest(BaseModel): 
    device_id:str

############################################################

@app.post("/lookup")  
def lookup_tenants(request: DeviceRequest):

    """
    Mock tenant-lookup endpoint. Given a device_id, returns a made-up
    list of tenant_ids standing in for a real lookup service.

    Deterministic: the same device_id always produces the same
    tenant_ids, by seeding the random generator with a hash of the
    device_id itself, rather than using Python's global random state.
    """


    device_seed = int(hashlib.sha256(request.device_id.encode()).hexdigest(),16)
    #^ seed the random generator using the device id , so the device id always produces the same result
    random_generator = random.Random(device_seed)

    num_tenants = random_generator.randint(1,3)  
    tenant_id = [f"tenant-{random_generator.randint(1000,9999)}" for _ in range(num_tenants)] 



    return{
        "device_id": request.device_id, 
        "tenants":tenant_id 
                }

############################################################
