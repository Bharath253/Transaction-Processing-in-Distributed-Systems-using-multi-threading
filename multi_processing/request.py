# request.py
import time
import random

class Request:
    def __init__(self, request_id):
        self.request_id = request_id

    def process(self):
        print(f"Processing request {self.request_id}")
        time.sleep(random.randint(1, 3))  # Simulate a time-consuming task
        print(f"Request {self.request_id} processed")
