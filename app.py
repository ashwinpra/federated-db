import tkinter as tk
import customtkinter
import numpy as np
import matplotlib
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import matplotlib.pyplot as plt

class QueryInterface(customtkinter.CTk):
    def __init__(self):
        super().__init__()
        self.title("Query Interface")
        self.geometry("800x600")

        # Create a dropdown menu for query selection
        self.query_var = customtkinter.StringVar()
        self.query_options = ["Query 1", "Query 2", "Query 3"]
        self.query_dropdown = customtkinter.CTkComboBox(self, variable=self.query_var, values=self.query_options)
        self.query_dropdown.bind("<<ComboboxSelected>>", self.update_range_fields)
        self.query_dropdown.pack(padx=20, pady=20)

        # Create range value fields
        self.range_frame = customtkinter.CTkFrame(self)
        self.range_frame.pack(padx=20, pady=20)

        self.range_min_label = customtkinter.CTkLabel(self.range_frame, text="Range Min:")
        self.range_min_entry = customtkinter.CTkEntry(self.range_frame)
        self.range_max_label = customtkinter.CTkLabel(self.range_frame, text="Range Max:")
        self.range_max_entry = customtkinter.CTkEntry(self.range_frame)

        self.range_min_label.grid(row=0, column=0)
        self.range_min_entry.grid(row=0, column=1)
        self.range_max_label.grid(row=0, column=2)
        self.range_max_entry.grid(row=0, column=3)

        # Create a frame for displaying the query result
        self.result_frame = customtkinter.CTkFrame(self, height=100, width=600,border_width=2)
        self.result_frame.pack(padx=20, pady=20)
        self.result_label = customtkinter.CTkLabel(self.result_frame, text="", anchor="w", justify="left")
        self.result_label.place(relx=0.5, rely=0.5, anchor="center")

        # Create a frame for displaying the plot
        self.plot_frame = customtkinter.CTkFrame(self, height=300, width=600)
        self.plot_frame.pack(padx=20, pady=20)
        self.figure = plt.figure(figsize=(6, 3))
        # self.canvas = FigureCanvasTkAgg(self.figure, master=self.plot_frame)
        # self.canvas.draw()
        # self.canvas.get_tk_widget().pack()

    def update_range_fields(self, event):
        selected_query = self.query_var.get()
        if selected_query == "Query 1":
            self.range_min_entry.config(state="normal")
            self.range_max_entry.config(state="normal")
        elif selected_query == "Query 2":
            self.range_min_entry.config(state="disabled")
            self.range_max_entry.config(state="normal")
        elif selected_query == "Query 3":
            self.range_min_entry.config(state="normal")
            self.range_max_entry.config(state="disabled")

    def display_result(self, result, error=False):
        if error:
            self.result_label.config(text=f"Error: {result}")
        else:
            self.result_label.config(text=f"Result: {result}")

    def display_plot(self, x, y):
        self.figure.clear()
        self.figure.plot(x, y)
        self.canvas.draw()

if __name__ == "__main__":
    app = QueryInterface()
    app.mainloop()