from fastapi.testclient import TestClient
from mock_api import app
 
client = TestClient(app)
 
def test_lookup_returns_tenants():



    """
    Sends a normal, valid request to /lookup.
    Confirms the response succeeds, includes a "tenants" field,
    and echoes back the same device_id that was sent.
    """



    response = client.post("/lookup", json={"device_id": "device-999"})
    print("Response received:", response.json())   
    assert response.status_code == 200
    data = response.json()
    assert "tenants" in data
    assert data["device_id"] == "device-999"


 
def test_tenants_is_a_list():

    """
    Confirms the "tenants" field in the response is genuinely a list,
    not a single value or some other type.
    """


    response=client.post("/lookup",json={"device_id":"device-999"})
    data=response.json()
    assert isinstance(data["tenants"],list)





def test_tenant_id_format():


    """
    Confirms every tenant ID returned follows the expected format:
    starting with "tenant-", followed only by digits.
    """


    response=client.post("/lookup",json={"device_id":"device-999"})
    data=response.json()
    for tenant_id in data ["tenants"]:
        assert tenant_id.startswith("tenant-")
        assert tenant_id.split("tenant-")[1].isdigit()

def test_missing_id():

    """
    Sends a request with no device_id at all.
    Confirms the API correctly rejects it with a 422 validation error,
    instead of crashing or silently accepting bad input.
    """ 

    response=client.post("/lookup",json={})
    assert response.status_code ==422
    
