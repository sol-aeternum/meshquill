import os


def on_message(event):
    del event
    os.write(2, b"x" * 131072)
