from fastapi.testclient import TestClient
#   call  API directly in code, without actually running a server

from mock_api import app
# import the actual FastAPI app object from  mock_api.py file

client = TestClient(app)
# create a test client that can send fake requests to  app

def test_lookup_returns_tenants():
#  pytest finds this automatically because it starts with "test_"
    response = client.post("/lookup", json={"device_id": "device-999"})
    #  send a fake POST request, just like consumer.py does

    print("Response received:", response.json())
    # this will actually print the real generated tenant_ids to see

    assert response.status_code == 200
    # check the API responded successfully (200 = OK)

    data = response.json()
    # convert the response into a python dictionary

    assert "tenants" in data
    # check the response  includes a "tenants" key

    assert data["device_id"] == "device-999"
    # check it echoed back the same device_id sent