import socket
import random
import selectors
import types

# Define the client settings
SERVER_HOST = "127.0.0.1"
SERVER_PORT = 8000

def recv_query(sock):
    # Receive the query from the server (in the format "length query") - query is a string
    query = ""
    while True:
        try:
            chunk = sock.recv(1024).decode()
            if not chunk:
                break
            query += chunk
        except socket.error:
            print(f"Server has closed")
        finally:
            sock.close()

        if query.startswith(""):
            msg_length, query = query.split(" ", 1)
            if len(query) >= int(msg_length):
                break

    return query

def main():
    client_id = random.randint(1, 100)
    # Create a socket and connect to the server
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect((SERVER_HOST, SERVER_PORT))
    client_socket.setblocking(False)

    sel = selectors.DefaultSelector()
    events = selectors.EVENT_READ | selectors.EVENT_WRITE
    data = types.SimpleNamespace(msg=b"")
    sel.register(client_socket, events, data=data)

    while True:
        events = sel.select(timeout=None)
        for key, mask in events:
            sock = key.fileobj
            data = key.data

            if mask & selectors.EVENT_READ:
                # Receive the query from the server (in the format "length query")
                query = recv_query(sock)
                print(f"Received query: {query}")

                # Execute the query on the client's database and send the result to the server
                #todo: change this
                result = "uwu"

                data.msg = f"{len(result)} {result}".encode()

            if mask & selectors.EVENT_WRITE:
                if data.msg:
                    sock.send(data.msg)
                    print(f"Sent result: {result}")


if __name__ == "__main__":
    main()