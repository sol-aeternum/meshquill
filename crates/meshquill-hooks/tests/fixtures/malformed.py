import os


def on_message(event):
    del event
    os.write(1, b"not-json")
