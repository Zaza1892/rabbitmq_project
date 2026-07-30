import uuid 

def test_verify_full_length_id_created():
    generated_id = str(uuid.uuid4())
    assert len (generated_id) ==36