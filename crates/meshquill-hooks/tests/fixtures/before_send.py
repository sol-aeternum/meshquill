def before_send(event):
    payload = event["payload"]
    if payload["text"] == "modify":
        return {
            "action": "modify",
            "destination": "new-destination",
            "text": "replacement",
        }
    if payload["text"] == "partial":
        return {"action": "modify", "text": "replacement-only"}
    if payload["text"] == "reject":
        return {"action": "reject", "reason": "blocked locally"}
    if payload["text"] == "invalid":
        return {"action": "modify", "destination": ""}
    return {"action": "allow"}
