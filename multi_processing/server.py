import socket
import pickle
import time
import sys
from concurrent.futures import ThreadPoolExecutor
from request import Request  # Assuming Request class is defined in request.py
import queue
import random
import threading
import itertools
import atexit
import time
from statistics import mean, quantiles
import psutil

# Create a priority queue
request_queue = queue.PriorityQueue()

# Define the maximum number of worker threads
MAX_WORKERS = 4
counter = itertools.count()
# Global variables to track total waiting time, total turnaround time, and request count
total_waiting_time = 0.0
total_turnaround_time = 0.0
request_count =0
count_lock = threading.Lock()  # Lock for synchronizing access to the counters
request_start_times = {}
request_end_times = {}

# A lock for a hypothetical shared resource
shared_resource_lock = threading.Lock()

# Condition variable for coordination
condition = threading.Condition()

# Shared resource example
shared_resource = {}


def process_request(request, addr):
    with shared_resource_lock:
        print(f"Thread {threading.current_thread().name} processing request {request.request_id}")
        # Modify shared resource
        shared_resource[request.request_id] = request
        request.process()
        # Remove request from shared resource
        del shared_resource[request.request_id]
        print(f"Thread {threading.current_thread().name} finished processing request {request.request_id}")


def send_response(conn, response_data):
    """Send the response size followed by the response data"""
    # Serialize the response with pickle
    pickled_response = pickle.dumps(response_data)
    # Send the size of the pickled response
    conn.sendall(len(pickled_response).to_bytes(4, 'big'))
    # Send the actual pickled response
    conn.sendall(pickled_response)

def handle_requests():
    global total_waiting_time, total_turnaround_time, request_count
    global request_start_times, request_end_times

    while True:
        # Get the next request from the priority queue
        priority, count, conn, addr = request_queue.get()
        if conn is None:  # Stop signal
            request_queue.task_done()
            break
        request_arrival_time = time.perf_counter()
        with condition:
            while len(shared_resource) >= MAX_WORKERS:
                print(f"Thread {threading.current_thread().name} waiting.")
                condition.wait()
            print(f"Thread {threading.current_thread().name} proceeding with request {priority}.")
            
        try:
            data = conn.recv(1024)
            if not data:
                request_queue.task_done()
                continue
            request = pickle.loads(data)
            # Processing starts here
            process_start_time = time.perf_counter()
            request_start_times[request.request_id] = request_arrival_time
            process_request(request, addr)
            request_end_time = time.perf_counter()
            request_end_times[request.request_id] = request_end_time
            process_end_time = time.perf_counter()
            # Calculate waiting time and turnaround time
            waiting_time = process_start_time - request_arrival_time
            turnaround_time = process_end_time - request_arrival_time
           # Update global totals and count in a thread-safe manner
            with count_lock:
                total_waiting_time += waiting_time
                total_turnaround_time += turnaround_time
                request_count += 1
            # Send a response back to the client
            response = f"Request {request.request_id} completed."
            send_response(conn, response)
            print(f"Request {request.request_id} with {priority} from {addr} - Turnaround Time: {turnaround_time} seconds")
        finally:
            conn.close()
            request_queue.task_done()

            with condition:
                print(f"Thread {threading.current_thread().name} notifying others.")
                condition.notify()  # Notify one waiting thread, if any

def server_listener(port, request_queue):
    global counter
    host = '127.0.0.1'
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((host, port))
    server_socket.listen(5)
    print(f"Server listening on port {port}")

    try:
        while True:
            conn, addr = server_socket.accept()
            # I'm using a random priority
            priority = random.randint(1, 100) 
            count = next(counter)  # Get the next unique identifier
            request_queue.put((priority,count, conn, addr))
    except Exception as e:
        print(f"Server encountered an error: {e}")
    finally:
        server_socket.close()
def resource_utilization():
    cpu_usage = psutil.cpu_percent()
    memory_usage = psutil.virtual_memory().percent
    network_usage = psutil.net_io_counters()

    print(f"CPU Usage: {cpu_usage}%")
    print(f"Memory Usage: {memory_usage}%")
    print(f"Bytes Sent: {network_usage.bytes_sent}")
    print(f"Bytes Received: {network_usage.bytes_recv}")

def print_statistics():
    with count_lock:
        if request_count > 0:
            average_waiting = total_waiting_time / request_count
            average_turnaround = total_turnaround_time / request_count
            print(f"All requests processed. Total waiting time: {total_waiting_time:.2f}s, "
                  f"Total turnaround time: {total_turnaround_time:.2f}s ,"
                  f"Average waiting time: {average_waiting:.2f}s, "
                  f"Average turnaround time: {average_turnaround:.2f}s")
            
            processing_times = [end - request_start_times[req_id] for req_id, end in request_end_times.items() if req_id in request_start_times]
            
            if len(processing_times) > 1:
                throughput = len(processing_times) / (max(request_end_times.values()) - min(request_start_times.values()))
                average_latency = mean(processing_times)
                p50_latency = quantiles(processing_times, n=100)[49]
                p95_latency = quantiles(processing_times, n=100)[94]
                p99_latency = quantiles(processing_times, n=100)[98]

                print(f"Throughput: {throughput:.2f} requests/second")
                print(f"Average Latency: {average_latency:.2f} seconds")
                print(f"Latency p50: {p50_latency:.2f} seconds")
                print(f"Latency p95: {p95_latency:.2f} seconds")
                print(f"Latency p99: {p99_latency:.2f} seconds")
            else:
                print("Not enough data to calculate quantiles.")
        else:
            print("No requests were processed.")
    resource_utilization()

    
def start_server(port):
    # Start the thread pool executor
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Start server listener in a new thread
        executor.submit(server_listener, port, request_queue)
        # Start worker threads
        workers = [executor.submit(handle_requests) for _ in range(MAX_WORKERS)]
        
        # Wait for all worker threads to complete
        for worker in workers:
            worker.result()
        # Server shutdown and print averages
        
        

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python server.py PORT_NUMBER")
        sys.exit(1)

    port_number = int(sys.argv[1])
    atexit.register(print_statistics)  # Register the print_statistics function to be called on exit
    start_server(port_number)
