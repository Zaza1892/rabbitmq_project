import json 
from unittest.mock import MagicMock,patch
import consumer


def test_handle_malformed_messages():
    test_chanel = MagicMock()
    test_method = MagicMock(delivery_tag=1)
    test_body = f"not valid json"
    
    with patch("consumer.save_to_db")as mock_save:
        consumer.handle_messages(test_chanel,test_method,None,test_body)

    mock_save.assert_not_called()
    test_chanel.basic_reject.assert_called_once_with(delivery_tag=1, requeue=False)


def test_handle_messages_missing_device_id():
    test_chanel = MagicMock()
    test_method = MagicMock(delivery_tag=1)
    test_body = json.dumps({"message_id":"msg-1"}).encode()

    with patch("consumer.save_to_db")as mock_save:
        consumer.handle_messages(test_chanel,test_method,None,test_body)


    mock_save.assert_not_called()
    test_chanel.basic_reject.assert_called_once_with(delivery_tag=1, requeue=False)

def test_api_call_fail():
    test_chanel = MagicMock()
    test_method = MagicMock(delivery_tag=1)
    test_body = json.dumps({"message_id":"msg-1","device_id":"device-1"}).encode()

    with patch("consumer.requests.post", side_effect=Exception("API down")), patch("consumer.save_to_db") as mock_save:
     consumer.handle_messages(test_chanel,test_method,None,test_body)

    mock_save.assert_not_called()
    test_chanel.basic_reject.assert_called_once_with(delivery_tag=1, requeue=True)


def test_db_insert_behaviour():
    with patch("consumer.psycopg2.connect") as test_connect:
        test_cursor=MagicMock()
        test_connect.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = test_cursor

        consumer.save_to_db("msg-1","device-1",["tenant-1234"],{"message_id": "msg-1", "device_id": "device-1"})
        executed_sql = test_cursor.execute.call_args[0][0]
        assert "ON CONFLICT" in executed_sql
