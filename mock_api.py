from fastapi import FastAPI  
from pydantic import BaseModel  
import random 
import hashlib # used to turn a device id into a repeatable number, so the same device always gets the same tenant 

app= FastAPI()  

@app.get("/health")
def health():
    return {"status":"ok"}
    
class DeviceRequest(BaseModel): 
    device_id:str


@app.post("/lookup")  
def lookup_tenants(request: DeviceRequest):

    device_seed = int(hashlib.sha256(request.device_id.encode()).hexdigest(),16)
    #^ seed the random generator using the device id , so the device id always produces the same result
    random_generator = random.Random(device_seed)

    num_tenants = random_generator.randint(1,3)  
    tenant_id = [f"tenant-{random_generator.randint(1000,9999)}" for _ in range(num_tenants)] 



    return{
        "device_id": request.device_id, 
        "tenants":tenant_id 
                }
