import time


def on_timeout(event):
    del event
    time.sleep(5)
