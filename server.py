import socket
import selectors
import types
import time
from tabulate import tabulate
import pickle


# Define the server settings
SERVER_HOST = "127.0.0.1"
SERVER_PORT = 8000
MAX_CLIENTS = 5
CONN_TIME = 10
TIMEOUT = 5

def print_menu():
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
            print(f"Error: {socket.error}")

        if message.startswith(b""):
            msg_length, client_id, msg = message.split(b" ", 2)
            if len(message) - len(str(msg_length.decode())) -len(str(client_id.decode())) - 2 >= int(msg_length):
                break

    print(msg_length, client_id)

    return client_id.decode(), msg.decode()

def accept_wrapper(sel, sock):
    conn, addr = sock.accept()
    print(f"Accepted connection from {addr}")
    conn.setblocking(False)
    data = types.SimpleNamespace(msg=b"")
    events = selectors.EVENT_READ | selectors.EVENT_WRITE
    sel.register(conn, events, data=data)

def service_send_message(key, mask, query):
    sock = key.fileobj
    data = key.data

    if mask & selectors.EVENT_WRITE:
        data.msg = f"{len(query)} {query}".encode()
        sock.send(data.msg)
        print(f"Sent query: {query}")

def service_recv_message(key, mask, results):
    sock = key.fileobj
    data = key.data

    if mask & selectors.EVENT_READ:
        client_id, result = recv_message(sock)

        attribute_names = result.split(" ")[0:2]
        for i in range(2, len(result.split(" ")), 2):
            # break if it is out of bounds
            if i+2 >= len(result.split(" ")):
                break

            results.append({"client_id": client_id, attribute_names[0]: result.split(" ")[i], attribute_names[1]: result.split(" ")[i+1]})

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
    done = False
    # Proceed with the normal flow
    while True:
        query = ""
        print_menu()
        choice = int(input("Enter your choice: "))

        results = []

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
            query = "5"
            done = True

        events = sel.select(timeout=TIMEOUT)
        for key, mask in events:
            if key.data is not None:
                service_send_message(key, mask, query)

        if done: 
            break

        time.sleep(1)
        
        # wait to receive the results from the clients
        events = sel.select(timeout=TIMEOUT)
        for key, mask in events:
            if key.data is not None:
                service_recv_message(key, mask, results)

        # Print the results
        print("Results:")
        headers = {}
        for key in results[0].keys():
            headers[key] = key

        print(tabulate(results, headers=headers))


    # Close the selector
    sel.close()

if __name__ == "__main__":
    main()





