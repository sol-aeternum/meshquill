import os


def on_message(event):
    assert event["schema"] == "meshquill.hook/v1"
    assert event["event"] == "on_message"
    assert isinstance(event["event_id"], str)
    assert isinstance(event["timestamp"], int)
    assert isinstance(event["payload"]["source"], str)
    assert "HOME" not in os.environ
    if event["payload"]["text"] == "crash":
        os._exit(23)
    if event["payload"]["text"] == "raise-secret":
        raise RuntimeError(event["payload"]["text"])
    print("diagnostic output is redirected away from the JSON protocol")
