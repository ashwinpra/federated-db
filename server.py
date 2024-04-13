import socket
import selectors
import types
import time

# Define the server settings
SERVER_HOST = "127.0.0.1"
SERVER_PORT = 8000
MAX_CLIENTS = 5
CONN_TIME = 10

results = []

def print_menu():
    #todo: change these 
    print("1. Get average overall yield")
    print("2. Get area under cultivation of crop")
    print("3. Get total area under cultivation")
    print("4. Get total yield of crop")
    print("5. Exit")

def recv_message(conn):
    # message will be in the format "message_length client_id message"
    message = b""
    while True:
        try:
            chunk = conn.recv(1024)
            if not chunk:
                break
            message += chunk
        except socket.error:
            print(f"Client has closed")
        finally: 
            conn.close()

        print(f"message = {message}<eof>")

        if message.startswith(b""):
            msg_length, client_id, msg = message.split(b" ", 2)
            # print(f"len(message)={len(message)}, len(msg_length) = {len(str(msg_length.decode()))}, len(client_id) = {len(str(client_id.decode()))}, msg_length={int(msg_length)}")
            if len(message) - len(str(msg_length.decode())) -len(str(client_id.decode())) - 2 >= int(msg_length):
                break

    print(msg_length, client_id)

    return client_id.decode(), message.decode()

def accept_wrapper(sel, sock):
    conn, addr = sock.accept()
    print(f"Accepted connection from {addr}")
    conn.setblocking(False)
    data = types.SimpleNamespace(msg=b"")
    events = selectors.EVENT_READ | selectors.EVENT_WRITE
    sel.register(conn, events, data=data)

def service_connection(sel, key, mask, query):
    print("service_connection")
    sock = key.fileobj
    data = key.data

    if mask & selectors.EVENT_WRITE:
        # Send the query to the client
        data.msg = f"{len(query)} {query}".encode()
        sock.send(data.msg)
        print(f"Sent query: {query}")

    elif mask & selectors.EVENT_READ:
        # todo: receive the result from the client
        client_id, result = recv_message(sock)
        print(f"Received result: {result} from client {client_id}")
        results.append(result)

        data.msg = b""
    

def main():
    # Create a socket and bind it to the server address
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((SERVER_HOST, SERVER_PORT))
    server_socket.listen(MAX_CLIENTS)

    # Create a selector and register the server socket
    sel = selectors.DefaultSelector()
    sel.register(server_socket, selectors.EVENT_READ, data=None)

    print(f"Server is listening on {SERVER_HOST}:{SERVER_PORT}")

    # Wait for clients to connect for CONN_TIME seconds
    start_time = time.time()
    while time.time() - start_time < CONN_TIME:
        events = sel.select(timeout=1)
        for key, mask in events:
            if key.data is None:
                accept_wrapper(sel, key.fileobj)

    # Stop accepting new connections
    server_socket.close()
    print("Clients have connected")

    # Proceed with the normal flow
    while True:
        query = ""
        print_menu()
        choice = int(input("Enter your choice: "))

        #todo: fix this

        if choice == 1:
            query = "1"

        elif choice == 2:
            crop = (input("Enter the crop: "))
            query = f"2 {crop}"

        elif choice == 3:
            query = "3"

        elif choice == 4:
            crop = input("Enter the crop: ")
            query = f"4 {crop}"

        if choice == 5:
            break

        events = sel.select(timeout=None)
        for key, mask in events:
            print(f"key.data = {key.data}")
            if key.data is not None:
                service_connection(sel, key, mask, query)

    # Print the results
    print("Results:")
    for i, result in enumerate(results):
        print(f"{i+1}. {result}")

    # Close the selector
    sel.close()

if __name__ == "__main__":
    main()





