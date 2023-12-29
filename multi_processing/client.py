import socket
import pickle
import time
import random
import threading
from request import Request  # Assuming Request class is defined in request.py

# List of server addresses
SERVERS = [
    ('127.0.0.1', 12345),
    ('127.0.0.1', 12346),
    ('127.0.0.1', 12347), 
]

def recvall(sock, nbytes):
    """Helper function to recv n bytes or return None if EOF is hit"""
    data = b''
    while len(data) < nbytes:
        packet = sock.recv(nbytes - len(data))
        if not packet:
            return None
        data += packet
    return data

def send_request(request_id):
    host, port = random.choice(SERVERS)
    
    start_time = time.perf_counter()  # Start timer

    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        client_socket.connect((host, port))
        request = Request(request_id)
        data = pickle.dumps(request)
        client_socket.sendall(data)
        # Wait for server's acknowledgement
        # Assuming the server sends the length of the pickled response first
        length_data = recvall(client_socket, 4)  # Read length of the pickled object
        if length_data:
            response_length = int.from_bytes(length_data, byteorder='big')
            response_data = recvall(client_socket, response_length)
            if response_data:
                response = pickle.loads(response_data)
                print(response)
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        client_socket.close()
        end_time = time.perf_counter()
        print(f"Request {request_id} to {host} - Response Time: {end_time - start_time} seconds")

def send_requests_in_parallel(request_ids):
    threads = []
    for request_id in request_ids:
        thread = threading.Thread(target=send_request, args=(request_id,))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()

if __name__ == '__main__':
    # Send 10 requests in parallel
    send_requests_in_parallel(range(10))
