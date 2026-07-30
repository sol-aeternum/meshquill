"""Minimal trusted-local-code hook for Meshquill.

The example records message metadata beside this file. It deliberately does not
persist message text. Hook code is not sandboxed and runs with the user's OS
permissions.
"""

import json
from pathlib import Path


def on_message(event):
    payload = event["payload"]
    record = {
        "schema": event["schema"],
        "event_id": event["event_id"],
        "source": payload["source"],
        "message_id": payload.get("message_id"),
    }
    output = Path(__file__).with_name("observed-messages.jsonl")
    with output.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(record, separators=(",", ":")) + "\n")


def before_send(event):
    if event["payload"]["destination"].casefold() == "blocked":
        return {"action": "reject", "reason": "destination blocked by local policy"}
    return {"action": "allow"}
