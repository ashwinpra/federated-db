import random
import numpy as np
import psycopg2

# Function to generate random data
def generate_data():
    farmers = ['John', 'Emma', 'Michael', 'Sophia', 'James', 'Olivia', 'William', 'Ava', 'Alexander', 'Isabella']
    crops = ['Wheat', 'Rice', 'Maize', 'Barley', 'Potato', 'Soybean', 'Cotton', 'Sugarcane', 'Tomato', 'Banana']
    
    data = []
    for _ in range(100):  # Generating 100 entries
        farmer = random.choice(farmers)
        crop = random.choice(crops)
        
        # Generating land area from a normal distribution with mean 5 and standard deviation 2
       
        
        for year in range(2014, 2024):  # Generating data for each year from 2014 to 2023
            # Generating yield for the year from a normal distribution
            land = round(max(0.5, min(10, np.random.normal(5, 2))), 2)
            yield_for_year = round(max(0, np.random.normal(land * 75, 25)), 2)  # Assuming mean yield of 75 per hectare
            data.append((farmer, crop, land, year, yield_for_year))
    return data

# Function to insert data into PostgreSQL
def insert_data(data):
    conn = psycopg2.connect(
        dbname="postgres",
        user="postgres",
        password="password",
        host="127.0.0.1",
        port="5432"
    )
    cur = conn.cursor()
    for entry in data:
        cur.execute("INSERT INTO agricultural_data (farmer_name, crop_name, land_area, year, yield) VALUES (%s, %s, %s, %s, %s)", entry)
    conn.commit()
    cur.close()
    conn.close()

# Call functions to generate and insert data
data = generate_data()
insert_data(data)
