import tkinter as tk
import customtkinter
import numpy as np
import matplotlib
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import matplotlib.pyplot as plt
import socket
import selectors
import types
import time
from tabulate import tabulate
import Crypto.Cipher.AES as AES
from queries import queries

# Define the server settings
SERVER_HOST = "127.0.0.1"
SERVER_PORT = 8001
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


# Create a socket and bind it to the server address
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.bind((SERVER_HOST, SERVER_PORT))
# get the IP and port it bound to

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
server_socket.close()
print("Clients have connected")

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


def get_query_output(choice, crop, year):
    # Proceed with the normal flow
    query = ""
    results = []
    if choice == 1:
        query = "1"
    elif choice == 2:
        query = f"2 {crop}"
    elif choice == 3:
        query = "3"
    elif choice == 4:
        query = f"4 {crop}"
    elif choice == 5:
        query = f"5 {crop} {year}"
    elif choice == 6:
        query = f"6 {crop}"
    elif choice == 7:
        query = f"7 {crop}"
    elif choice == 8:
        query = f"8"
    elif choice == 9:
        query = f"9 {year}"
    if choice == 10:
        query = "10"

    events = sel.select(timeout=TIMEOUT)
    for key, mask in events:
        if key.data is not None:
            service_send_message(key, mask, query)

    time.sleep(1)
    
    # wait to receive the results from the clients
    events = sel.select(timeout=TIMEOUT)
    for key, mask in events:
        if key.data is not None:
            service_recv_message(key, mask, results)

    # Print the results
    print("Results:")
    print(results)
    headers = {}
    for key in results[0].keys():
        headers[key] = key

    print(tabulate(results, headers=headers))

    return results, headers


class QueryInterface(customtkinter.CTk):
    def __init__(self):
        super().__init__()
        self.title("Query Interface")
        self.geometry("800x600")

        # Create a dropdown menu for query selection
        self.query_var = customtkinter.StringVar()
        self.query_frame = customtkinter.CTkFrame(self)
        self.query_frame.pack(padx=20, pady=20)

        self.query_label = customtkinter.CTkLabel(self.query_frame, text="Select Query: ")
        self.query_label.grid(row=0, column=0)

        self.query_options = queries
        self.query_dropdown = customtkinter.CTkComboBox(self.query_frame, variable=self.query_var, values=self.query_options)
        # self.query_dropdown.bind("<<ComboboxSelected>>", self.update_fields)
        self.query_dropdown.grid(row=0, column=1)

        # Create value field
        self.value_frame = customtkinter.CTkFrame(self)
        self.value_frame.pack(padx=20, pady=20)

        self.value_label = customtkinter.CTkLabel(self.value_frame, text="Crop: ")
        self.value_entry = customtkinter.CTkEntry(self.value_frame)
        self.value_label2 = customtkinter.CTkLabel(self.value_frame, text="Year: ")
        self.value_entry2 = customtkinter.CTkEntry(self.value_frame)

        self.value_label.grid(row=0, column=0)
        self.value_entry.grid(row=0, column=1)
        self.value_label2.grid(row=1, column=0)
        self.value_entry2.grid(row=1, column=1)

        # Create a submit button
        self.submit_button = customtkinter.CTkButton(self, text="Submit", command=self.submit_query)
        self.submit_button.pack(padx=20, pady=10)

        # Create a frame for displaying the query result
        self.result_frame = customtkinter.CTkFrame(self, height=300, width=600,border_width=2)
        self.result_frame.pack(padx=20, pady=20)
        self.result_label = customtkinter.CTkLabel(self.result_frame, text="", anchor="w", justify="left")
        self.result_label.place(relx=0.5, rely=0.5, anchor="center")

    def submit_query(self):
        crop = self.value_entry.get()
        year = self.value_entry2.get()
        selected_query = self.query_var.get()
        query = queries.index(selected_query) + 1
        results, headers = get_query_output(query, crop, year)
        self.display_result(tabulate(results, headers=headers))

    def display_result(self, result, error=False):
        if error:
            self.result_label.configure(text=f"Error: {result}")
        else:
            self.result_label.configure(text=f"Result: {result}")


if __name__ == "__main__":
    app = QueryInterface()
    app.mainloop()