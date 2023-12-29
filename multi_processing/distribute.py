import socket
import itertools
import sys
import threading


# List of server addresses (IP, port)
SERVERS = [
    ('127.0.0.1', 12345),
    ('127.0.0.1', 12346),
    ('127.0.0.1', 12347),
]

# Create an iterator that will return the servers in a round-robin fashion
server_iterator = itertools.cycle(SERVERS)

def forward_request(client_conn, server_address):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        server_socket.connect(server_address)

        while True:
            data = client_conn.recv(1024)
            if not data:
                break  # No more data from the client
            try:
                server_socket.sendall(data)
            except BrokenPipeError:
                print("Broken pipe error occurred when sending data to the server.")
                break

            # Forward server's acknowledgement back to the client
            response = server_socket.recv(1024)
            client_conn.sendall(response)
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        server_socket.close()
        client_conn.close()

def start_load_balancer(port):
    load_balancer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    load_balancer_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    load_balancer_socket.bind(('0.0.0.0', port))
    load_balancer_socket.listen(5)

    print(f"Load balancer is listening on port {port}")

    while True:
        client_conn, client_address = load_balancer_socket.accept()
        print(f"Accepted connection from {client_address}")

        # Get the next server from the round-robin iterator
        server_address = next(server_iterator)

        # Forward the request to the server in a new thread
        threading.Thread(target=forward_request, args=(client_conn, server_address)).start()

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python distribute.py PORT_NUMBER")
        sys.exit(1)

    port_number = int(sys.argv[1])
    start_load_balancer(port_number)
