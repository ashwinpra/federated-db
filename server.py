import socket
import selectors
import types
import time
from tabulate import tabulate
import Crypto.Cipher.AES as AES

# Define the server settings
SERVER_HOST = "10.145.254.177"
SERVER_PORT = 8000
MAX_CLIENTS = 5
CONN_TIME = 10
TIMEOUT = 20
num_clients = 0

def pad_message(message):
    # Pad the message to be a multiple of 16 bytes
    return message + " " * (16 - len(message) % 16)

def remove_padding(message):
    # Remove the padding from the message
    return message.rstrip()

def print_menu():
    print("1. Get average overall yield")
    print("2. Get area under cultivation of crop")
    print("3. Get total area under cultivation")
    print("4. Get total yield of crop")
    print("5. Get the yield of crop for a particular year")
    print("6. Get the yield of a crop of the current year")
    print("7. Get the year with the highest yield for a crop")
    print("8. Get the year with the highest yield for all districts")
    print("9. Get the crop with the highest yield for all districts for a particular year")
    print("10. Exit")

def recv_message(conn, cipher):
    # message will be in the format "message"
    message = b""
    while True:
        # receive until the message is complete
        data = conn.recv(1024)
        message += data
        if len(data) < 1024:
            break

    # print("Received message:", message)
    decrypted_message = cipher.decrypt(message)
    decrypted_message = remove_padding(decrypted_message.decode())
    # print("Decrypted message:", decrypted_message)

    return decrypted_message

def accept_wrapper(sel, sock):
    global num_clients 

    conn, addr = sock.accept()
    print(f"Accepted connection from {addr}")

    msg = conn.recv(1024).decode()
    client_id, key, iv = msg.split(" ")

    num_clients += 1

    conn.setblocking(False)
    data = types.SimpleNamespace(msg=b"", client_id=client_id, key=key, iv=iv)
    events = selectors.EVENT_READ | selectors.EVENT_WRITE
    sel.register(conn, events, data=data)

def service_send_message(key, mask, query):
    sock = key.fileobj
    data = key.data
    key = data.key
    iv = data.iv

    cipher = AES.new(key.encode(), AES.MODE_CFB, iv.encode())
    # print("key:", key, "iv:", iv)

    if mask & selectors.EVENT_WRITE:
        data.msg = cipher.encrypt(pad_message(query).encode())
        sock.send(data.msg)
        # print("data.msg:", data.msg)
        # print("Encrypted message:", pad_message(query).encode())
        # print(f"Sent query: {query}")

def service_recv_message(key, mask, results):
    sock = key.fileobj
    data = key.data
    client_id = data.client_id
    key = data.key
    iv = data.iv

    cipher = AES.new(key.encode(), AES.MODE_CFB, iv.encode())

    if mask & selectors.EVENT_READ:
        result = recv_message(sock, cipher)
        attribute_names = result.split(" ")[0:2]
        for i in range(2, len(result.split(" ")), 2):
            # break if it is out of bounds
            if i+1 >= len(result.split(" ")):
                break
            
            results.append({"client_id": client_id, attribute_names[0]: result.split(" ")[i], attribute_names[1]: result.split(" ")[i+1]})

def main():
    global num_clients

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
    print("Connection period over")

    if num_clients == 0:
        print("No clients connected")
        return

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
        elif choice == 5:
            crop = input("Enter the crop: ")
            year = input("Enter the year: ")
            query = f"5 {crop} {year}"
        elif choice == 6:
            crop = input("Enter the crop: ")
            query = f"6 {crop}"
        elif choice == 7:
            crop = input("Enter the crop: ")
            query = f"7 {crop}"
        elif choice == 8:
            query = f"8"
        elif choice == 9:
            year = input("Enter the year: ")
            query = f"9 {year}"
        if choice == 10:
            query = "10"
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
        print("\nResults:")
        print("="*20)
        headers = {}
        if len(results) == 0:
            print("No results found")
            print()
            continue

        for key in results[0].keys():
            headers[key] = key

        print(tabulate(results, headers=headers))
        print()


    # Close the selector
    sel.close()

if __name__ == "__main__":
    main()





