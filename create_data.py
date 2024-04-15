import random
import numpy as np
import psycopg2
import sys
from pymongo import MongoClient
import sqlite3

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
def insert_data_postgres(data):
    # conn = psycopg2.connect(
    #     dbname="postgres",
    #     user="postgres",
    #     password="password",
    #     host="127.0.0.1",
    #     port="5432"
    # )

    conn = psycopg2.connect(
        host="10.5.18.70",
        database="21CS30038",
        user="21CS30038",
        password="21CS30038"
    )

    cur = conn.cursor()
    # make a table named agricultural_data
    cur.execute("CREATE TABLE IF NOT EXISTS agricultural_data (farmer_name VARCHAR(50), crop_name VARCHAR(50), land_area FLOAT, year INT, yield FLOAT)")

    # clear the table
    cur.execute("DELETE FROM agricultural_data")
    
    for entry in data:
        cur.execute("INSERT INTO agricultural_data (farmer_name, crop_name, land_area, year, yield) VALUES (%s, %s, %s, %s, %s)", entry)
    conn.commit()
    cur.close()
    conn.close()

# Function to insert data into MongoDB
def insert_data_mongodb(data):
    client = MongoClient('mongodb://localhost:27017/')
    db = client['agricultural_data']
    collection = db['agricultural_data']

    # clear the collection
    collection.delete_many({})

    for entry in data:
        collection.insert_one({
            'farmer_name': entry[0],
            'crop_name': entry[1],
            'land_area': entry[2],
            'year': entry[3],
            'yield': entry[4]
        })

    client.close()


# Function to insert data into SQLite
def insert_data_sqlite(data):
    conn = sqlite3.connect('agricultural_data.db')
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS agricultural_data (farmer_name TEXT, crop_name TEXT, land_area REAL, year INTEGER, yield REAL)")

    # clear the table
    cur.execute("DELETE FROM agricultural_data")

    for entry in data:
        cur.execute("INSERT INTO agricultural_data (farmer_name, crop_name, land_area, year, yield) VALUES (?, ?, ?, ?, ?)", entry)
    conn.commit()
    conn.close()


def main():
    if len(sys.argv) != 2:
        print("Usage: python3 create_data.py <postgres/sqlite/mongo>")
        sys.exit(1)

    db_type = sys.argv[1]
    data = generate_data()

    if db_type == 'postgres':
        insert_data_postgres(data)
    elif db_type == 'sqlite':
        insert_data_sqlite(data)
    elif db_type == 'mongodb':
        insert_data_mongodb(data)

if __name__ == "__main__":
    main()