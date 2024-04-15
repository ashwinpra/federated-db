import socket
import random
import selectors
import types
import sys
import psycopg2
import sqlite3
import pymongo
import os 
import Crypto.Cipher.AES as AES

# Define the client settings
SERVER_HOST = "10.145.254.177"
SERVER_PORT = 8000
enc_key = os.urandom(8).hex()
iv = os.urandom(8).hex()

def pad_message(message):
    # Pad the message to be a multiple of 16 bytes
    return message + " " * (16 - len(message) % 16)

def remove_padding(message):
    # Remove the padding from the message
    return message.rstrip()

def recv_query(sock):
    global enc_key, iv

    d_cipher = AES.new(enc_key.encode(), AES.MODE_CFB, iv.encode())

    # query will be in the format "query"
    query = b""
    while True:
        # receive until the query is complete
        data = sock.recv(1024)
        query += data
        if len(data) < 1024:
            break

    # print("Received query:", query)
    decrypted_query = d_cipher.decrypt(query)
    # print("Decrypted query:", decrypted_query)
    decrypted_query = remove_padding(decrypted_query).decode()

    return decrypted_query

def calculate_average_yield_by_year(connection, db_type):
    if db_type == "postgres":
        try:
            cursor = connection.cursor()
            # SQL query to calculate average yield for each year for the last 10 years (2014-2023)
            query = """
                SELECT year, SUM(yield) / SUM(land_area) AS average_yield
                FROM agricultural_data
                WHERE year >= EXTRACT(YEAR FROM CURRENT_DATE) - 10
                GROUP BY year
                ORDER BY year
            """

            cursor.execute(query)
            average_yield_by_year = cursor.fetchall()

            # Close cursor and connection
            cursor.close()
            connection.close()

            # Return list of tuples containing year and average yield as strings
            return [(str(year), str(average_yield)) for year, average_yield in average_yield_by_year]

        except (Exception, psycopg2.Error) as error:
            print("Error while connecting to PostgreSQL", error)

    elif db_type == "sqlite":
        try:
            # Connect to your SQLite database
            connection = sqlite3.connect("agricultural_data.db")
            cursor = connection.cursor()

            # SQL query to calculate average yield for each year for the last 10 years
            query = """
                SELECT year, SUM(yield) / SUM(land_area) AS average_yield
                FROM agricultural_data
                WHERE year >= strftime('%Y', 'now') - 10
                GROUP BY year
                ORDER BY year
            """

            cursor.execute(query)
            average_yield_by_year = cursor.fetchall()

            # Close cursor and connection
            cursor.close()
            connection.close()

            # Return list of tuples containing year and average yield as strings
            return [(str(year), str(average_yield)) for year, average_yield in average_yield_by_year]

        except sqlite3.Error as error:
            print("Error while connecting to SQLite", error)    

    elif db_type == "mongo":
        try:
            # Connect to your MongoDB database
            db = connection["agricultural_data"]
            collection = db["agricultural_data"]

            for data in collection.find():
                print(data)

            print("Connected to MongoDB")
            # Calculate average yield for each year for the last 10 years
            average_yield_by_year = []
            for year in range(2014, 2024):
                total_yield = 0
                total_area = 0
                for data in collection.find({"year": year}):
                    print(f"total_yield: {total_yield}, total_area: {total_area}")
                    
                    total_yield += data["yield"]
                    total_area += data["land_area"]
                average_yield = total_yield / total_area
                average_yield_by_year.append((str(year), str(average_yield)))

            # Close connection
            connection.close()

            # Return list of tuples containing year and average yield as strings
            return average_yield_by_year

        except pymongo.errors.PyMongoError as error:
            print("Error while connecting to MongoDB", error)

def calculate_total_area_for_crop_by_year(connection, db_type, crop_name):
    if db_type == "postgres":
        try:
            cursor = connection.cursor()

            # SQL query to calculate total area under cultivation for the specified crop by year
            query = """
                SELECT year, SUM(land_area)
                FROM agricultural_data
                WHERE crop_name = %s
                GROUP BY year
                ORDER BY year
            """

            cursor.execute(query, (crop_name,))
            total_area_by_year = cursor.fetchall()

            # Close cursor and connection
            cursor.close()
            connection.close()

            # Return list of tuples containing year and total area as strings
            return [(str(year), str(total_area)) for year, total_area in total_area_by_year]

        except (Exception, psycopg2.Error) as error:
            print("Error while connecting to PostgreSQL", error)

    elif db_type == "sqlite":
        try:
            # Connect to your SQLite database
            connection = sqlite3.connect("agricultural_data.db")
            cursor = connection.cursor()

            # SQL query to calculate total area under cultivation for the specified crop by year
            query = """
                SELECT year, SUM(land_area)
                FROM agricultural_data
                WHERE crop_name = ?
                GROUP BY year
                ORDER BY year
            """

            cursor.execute(query, (crop_name,))
            total_area_by_year = cursor.fetchall()

            # Close cursor and connection
            cursor.close()
            connection.close()

            # Return list of tuples containing year and total area as strings
            return [(str(year), str(total_area)) for year, total_area in total_area_by_year]

        except sqlite3.Error as error:
            print("Error while connecting to SQLite", error)

    elif db_type == "mongo":
        try:
            # Connect to your MongoDB database
            db = connection["agricultural_data"]
            collection = db["agricultural_data"]

            # Calculate total area under cultivation for the specified crop by year
            total_area_by_year = []
            for year in range(2014, 2024):
                total_area = 0
                for data in collection.find({"year": year, "crop_name": crop_name}):
                    total_area += data["land_area"]
                total_area_by_year.append((str(year), str(total_area)))

            # Close connection
            connection.close()

            # Return list of tuples containing year and total area as strings
            return total_area_by_year

        except pymongo.errors.PyMongoError as error:
            print("Error while connecting to MongoDB", error)



def calculate_total_area_by_year(connection, db_type):
    if db_type == "postgres":
        try:
            cursor = connection.cursor()

            # SQL query to calculate total area under cultivation for each year
            query = """
                SELECT year, SUM(land_area)
                FROM agricultural_data
                GROUP BY year
                ORDER BY year
            """

            cursor.execute(query)
            total_area_by_year = cursor.fetchall()

            # Close cursor and connection
            cursor.close()
            connection.close()

            # Return list of tuples containing year and total area as strings
            return [(str(year), str(total_area)) for year, total_area in total_area_by_year]

        except (Exception, psycopg2.Error) as error:
            print("Error while connecting to PostgreSQL", error)

    elif db_type == "sqlite":
        try:
            # Connect to your SQLite database
            connection = sqlite3.connect("agricultural_data.db")
            cursor = connection.cursor()

            # SQL query to calculate total area under cultivation for each year
            query = """
                SELECT year, SUM(land_area)
                FROM agricultural_data
                GROUP BY year
                ORDER BY year
            """

            cursor.execute(query)
            total_area_by_year = cursor.fetchall()

            # Close cursor and connection
            cursor.close()
            connection.close()

            # Return list of tuples containing year and total area as strings
            return [(str(year), str(total_area)) for year, total_area in total_area_by_year]

        except sqlite3.Error as error:
            print("Error while connecting to SQLite", error)
        
    elif db_type == "mongo":
        try:
            # Connect to your MongoDB database
            db = connection["agricultural_data"]
            collection = db["agricultural_data"]

            # Calculate total area under cultivation for each year
            total_area_by_year = []
            for year in range(2014, 2024):
                total_area = 0
                for data in collection.find({"year": year}):
                    total_area += data["land_area"]
                total_area_by_year.append((str(year), str(total_area)))

            # Close connection
            connection.close()

            # Return list of tuples containing year and total area as strings
            return total_area_by_year

        except pymongo.errors.PyMongoError as error:
            print("Error while connecting to MongoDB", error)


def calculate_total_yield_for_crop_by_year(connection, db_type, crop_name):
    if db_type == "postgres":
        try:
            cursor = connection.cursor()

            # SQL query to calculate total yield for the specified crop by year
            query = """
                SELECT year, SUM(yield)
                FROM agricultural_data
                WHERE crop_name = %s
                GROUP BY year
                ORDER BY year
            """

            cursor.execute(query, (crop_name,))
            total_yield_by_year = cursor.fetchall()

            # Close cursor and connection
            cursor.close()
            connection.close()

            # Return list of tuples containing year and total yield as strings
            return [(str(year), str(total_yield)) for year, total_yield in total_yield_by_year]

        except (Exception, psycopg2.Error) as error:
            print("Error while connecting to PostgreSQL", error)

    elif db_type == "sqlite":
        try:
            # Connect to your SQLite database
            connection = sqlite3.connect("agricultural_data.db")
            cursor = connection.cursor()

            # SQL query to calculate total yield for the specified crop by year
            query = """
                SELECT year, SUM(yield)
                FROM agricultural_data
                WHERE crop_name = ?
                GROUP BY year
                ORDER BY year
            """

            cursor.execute(query, (crop_name,))
            total_yield_by_year = cursor.fetchall()

            # Close cursor and connection
            cursor.close()
            connection.close()

            # Return list of tuples containing year and total yield as strings
            return [(str(year), str(total_yield)) for year, total_yield in total_yield_by_year]

        except sqlite3.Error as error:
            print("Error while connecting to SQLite", error)

    elif db_type == "mongo":
        try:
            # Connect to your MongoDB database
            db = connection["agricultural_data"]
            collection = db["agricultural_data"]

            # Calculate total yield for the specified crop by year
            total_yield_by_year = []
            for year in range(2014, 2024):
                total_yield = 0
                for data in collection.find({"year": year, "crop_name": crop_name}):
                    total_yield += data["yield"]
                total_yield_by_year.append((str(year), str(total_yield)))

            # Close connection
            connection.close()

            # Return list of tuples containing year and total yield as strings
            return total_yield_by_year

        except pymongo.errors.PyMongoError as error:
            print("Error while connecting to MongoDB", error)


def calculate_total_yield_for_crop_for_year(connection, db_type, crop_name, year):
    if db_type == "postgres":
        try:
            cursor = connection.cursor()

            # print all the data
            cursor.execute("SELECT * FROM agricultural_data")

            # SQL query to calculate yield for the specified crop for a particular year
            query = """
                SELECT SUM(yield)
                FROM agricultural_data
                WHERE crop_name = %s AND year = %s
            """

            cursor.execute(query, (crop_name, year))
            yield_for_year = cursor.fetchone()[0]

            # Close cursor and connection
            cursor.close()
            connection.close()

            return yield_for_year

        except (Exception, psycopg2.Error) as error:
            print("Error while connecting to PostgreSQL", error)

    elif db_type == "sqlite":
        try:
            # Connect to your SQLite database
            connection = sqlite3.connect("agricultural_data.db")
            cursor = connection.cursor()

            # SQL query to calculate yield for the specified crop for a particular year
            query = """
                SELECT SUM(yield)
                FROM agricultural_data
                WHERE crop_name = ? AND year = ?
            """

            cursor.execute(query, (crop_name, year))
            yield_for_year = cursor.fetchone()[0]

            # Close cursor and connection
            cursor.close()
            connection.close()

            return yield_for_year

        except sqlite3.Error as error:
            print("Error while connecting to SQLite", error)

    elif db_type == "mongo":
        try:
            # Connect to your MongoDB database
            db = connection["agricultural_data"]
            collection = db["agricultural_data"]

            # Calculate yield for the specified crop for a particular year
            yield_for_year = 0
            for data in collection.find({"year": year, "crop_name": crop_name}):
                yield_for_year += data["yield"]

            # Close connection
            connection.close()

            return yield_for_year

        except pymongo.errors.PyMongoError as error:
            print("Error while connecting to MongoDB", error)


def calculate_highest_yield_year_for_crop(connection, db_type, crop_name):
    if db_type == "postgres":
        try:
            cursor = connection.cursor()

            # SQL query to calculate total yield for the specified crop by year
            query = """
                SELECT year, SUM(yield) AS total_yield
                FROM agricultural_data
                WHERE crop_name = %s
                GROUP BY year
                ORDER BY total_yield DESC
                LIMIT 1
            """

            cursor.execute(query, (crop_name,))
            result = cursor.fetchone()
            year_with_highest_yield = result[0] if result else None

            # Close cursor and connection
            cursor.close()
            connection.close()

            return year_with_highest_yield

        except (Exception, psycopg2.Error) as error:
            print("Error while connecting to PostgreSQL", error)

    elif db_type == "sqlite":
        try:
            # Connect to your SQLite database
            connection = sqlite3.connect("agricultural_data.db")
            cursor = connection.cursor()

            # SQL query to calculate total yield for the specified crop by year
            query = """
                SELECT year, SUM(yield) AS total_yield
                FROM agricultural_data
                WHERE crop_name = ?
                GROUP BY year
                ORDER BY total_yield DESC
                LIMIT 1
            """

            cursor.execute(query, (crop_name,))
            result = cursor.fetchone()
            year_with_highest_yield = result[0] if result else None

            # Close cursor and connection
            cursor.close()
            connection.close()

            return year_with_highest_yield

        except sqlite3.Error as error:
            print("Error while connecting to SQLite", error)

    elif db_type == "mongo":
        try:
            # Connect to your MongoDB database
            db = connection["agricultural_data"]
            collection = db["agricultural_data"]

            # Aggregate data to find the year with highest yield for the specified crop
            pipeline = [
                {"$match": {"crop_name": crop_name}},
                {"$group": {"_id": "$year", "total_yield": {"$sum": "$yield"}}},
                {"$sort": {"total_yield": -1}},
                {"$limit": 1}
            ]
            result = list(collection.aggregate(pipeline))
            year_with_highest_yield = result[0]["_id"] if result else None

            # Close connection
            connection.close()

            return year_with_highest_yield

        except pymongo.errors.PyMongoError as error:
            print("Error while connecting to MongoDB", error)


def year_with_highest_yield(connection, db_type):
    if db_type == "postgres":
        try:
            cursor = connection.cursor()

            # SQL query to calculate total yield for all crops by year
            query = """
                SELECT year, SUM(yield) AS total_yield
                FROM agricultural_data
                GROUP BY year
                ORDER BY total_yield DESC
                LIMIT 1
            """

            cursor.execute(query)
            result = cursor.fetchone()
            year_with_highest_yield = [(str(result[0]), str(result[1]))] if result else []

            # Close cursor and connection
            cursor.close()
            connection.close()

            return year_with_highest_yield

        except (Exception, psycopg2.Error) as error:
            print("Error while connecting to PostgreSQL", error)

    elif db_type == "sqlite":
        try:
            # Connect to your SQLite database
            connection = sqlite3.connect("agricultural_data.db")
            cursor = connection.cursor()

            # SQL query to calculate total yield for all crops by year
            query = """
                SELECT year, SUM(yield) AS total_yield
                FROM agricultural_data
                GROUP BY year
                ORDER BY total_yield DESC
                LIMIT 1
            """

            cursor.execute(query)
            result = cursor.fetchone()
            year_with_highest_yield = [(str(result[0]), str(result[1]))] if result else []

            # Close cursor and connection
            cursor.close()
            connection.close()

            return year_with_highest_yield

        except sqlite3.Error as error:
            print("Error while connecting to SQLite", error)

    elif db_type == "mongo":
        try:
            # Connect to your MongoDB database
            db = connection["agricultural_data"]
            collection = db["agricultural_data"]

            # Aggregate data to find the year with highest yield for all crops
            pipeline = [
                {"$group": {"_id": "$year", "total_yield": {"$sum": "$yield"}}},
                {"$sort": {"total_yield": -1}},
                {"$limit": 1}
            ]
            result = list(collection.aggregate(pipeline))
            year_with_highest_yield = [(str(doc["_id"]), str(doc["total_yield"])) for doc in result]

            # Close connection
            connection.close()

            return year_with_highest_yield

        except pymongo.errors.PyMongoError as error:
            print("Error while connecting to MongoDB", error)


def crop_with_highest_yield_for_year(connection, db_type, year):
    if db_type == "postgres":
        try:
            cursor = connection.cursor()

            # SQL query to calculate total yield for each crop for the specified year
            query = """
                SELECT crop_name, SUM(yield) AS total_yield
                FROM agricultural_data
                WHERE year = %s
                GROUP BY crop_name
                ORDER BY total_yield DESC
                LIMIT 1
            """

            cursor.execute(query, (year,))
            result = cursor.fetchone()
            crop_with_highest_yield = [(str(result[0]), str(result[1]))] if result else []

            # Close cursor and connection
            cursor.close()
            connection.close()

            return crop_with_highest_yield

        except (Exception, psycopg2.Error) as error:
            print("Error while connecting to PostgreSQL", error)

    elif db_type == "sqlite":
        try:
            # Connect to your SQLite database
            connection = sqlite3.connect("agricultural_data.db")
            cursor = connection.cursor()

            # SQL query to calculate total yield for each crop for the specified year
            query = """
                SELECT crop_name, SUM(yield) AS total_yield
                FROM agricultural_data
                WHERE year = ?
                GROUP BY crop_name
                ORDER BY total_yield DESC
                LIMIT 1
            """

            cursor.execute(query, (year,))
            result = cursor.fetchone()
            crop_with_highest_yield = [(str(result[0]), str(result[1]))] if result else []

            # Close cursor and connection
            cursor.close()
            connection.close()

            return crop_with_highest_yield

        except sqlite3.Error as error:
            print("Error while connecting to SQLite", error)

    elif db_type == "mongo":
        try:
            # Connect to your MongoDB database
            db = connection["agricultural_data"]
            collection = db["agricultural_data"]

            # Aggregate data to find the crop with highest yield for the specified year
            pipeline = [
                {"$match": {"year": year}},
                {"$group": {"_id": "$crop_name", "total_yield": {"$sum": "$yield"}}},
                {"$sort": {"total_yield": -1}},
                {"$limit": 1}
            ]
            result = list(collection.aggregate(pipeline))
            crop_with_highest_yield = [(str(doc["_id"]), str(doc["total_yield"])) for doc in result]

            # Close connection
            connection.close()

            return crop_with_highest_yield

        except pymongo.errors.PyMongoError as error:
            print("Error while connecting to MongoDB", error)


def process_query(db_type, query_num, crop,year = None):

    if db_type == "postgres":
        # connection = psycopg2.connect(
        #     dbname="postgres",
        #     user="postgres",
        #     password="password",
        #     host="127.0.0.1",
        #     port="5432"
        # )

        connection = psycopg2.connect(
            host="10.5.18.70",
            database="21CS30009",
            user="21CS30009",
            password="21CS30009"
        )
        print("Connected to PostgreSQL")

    elif db_type == "sqlite":
        connection = sqlite3.connect("agricultural_data.db")
        print("Connected to SQLite")

    elif db_type == "mongo":
        connection = pymongo.MongoClient("mongodb://localhost:27017/")
        print("Connected to MongoDB")

    # Execute the query on the client's database and return the result
    if(query_num==1):
        temp = calculate_average_yield_by_year(connection, db_type)
        res = "Year Yield "
        for i in temp:
            res += i[0] + " " + i[1] + " "
    elif(query_num==2):
        temp = calculate_total_area_for_crop_by_year(connection, db_type, crop)
        res = "Year Area "
        for i in temp:
            res += i[0] + " " + i[1] + " "
    elif(query_num==3):
        temp = calculate_total_area_by_year(connection, db_type)
        res = "Year Area "
        for i in temp:
            res += i[0] + " " + i[1] + " "
    elif(query_num==4):
        temp = calculate_total_yield_for_crop_by_year(connection, db_type, crop)
        res = "Year Yield "
        for i in temp:
            res += i[0] + " " + i[1] + " "
    elif(query_num==5):
        temp = calculate_total_yield_for_crop_for_year(connection, db_type, crop, year)
        res = "Crop Yield "
        res += crop + " "+ str(temp) + " "
    elif(query_num==6):
        temp = calculate_total_yield_for_crop_for_year(connection, db_type, crop, 2023)
        res = "Crop Yield "
        res += crop + " "+ str(temp) + " "
    elif(query_num==7):
        temp = calculate_highest_yield_year_for_crop(connection, db_type, crop)
        res = "Crop Year "
        res += crop + " "+ str(temp) + " "
    elif(query_num==8):
        temp = year_with_highest_yield(connection, db_type)
        res = "Year Yield "
        for i in temp:
            res += i[0] + " " + i[1] + " "
    elif query_num==9:
        temp = crop_with_highest_yield_for_year(connection, db_type, year)
        res = "Crop Yield "
        for i in temp:
            res += i[0] + " " + i[1] + " "

    print("Result found")
    return res
        
def main():
    global enc_key, iv

    # take type of database as command line argument 
    if len(sys.argv) != 2:
        print("Usage: python3 client.py <postgres/sqlite/json>")
        sys.exit(1)

    db_type = sys.argv[1]

    client_id = random.randint(1, 100)
    # Create a socket and connect to the server
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect((SERVER_HOST, SERVER_PORT))
    client_socket.setblocking(False)

    print("Client ID:", client_id)

    sel = selectors.DefaultSelector()
    events = selectors.EVENT_READ | selectors.EVENT_WRITE
    data = types.SimpleNamespace(msg=b"")
    sel.register(client_socket, events, data=data)

    # print("key:", enc_key, "iv:", iv)

    # Send the client ID, key and IV to the server
    msg = f"{client_id} {enc_key} {iv}".encode()
    client_socket.send(msg)

    print("Connected to the server")

    done = False

    while True:

        e_cipher = AES.new(enc_key.encode(), AES.MODE_CFB, iv.encode())

        if done:
            break

        events = sel.select(timeout=None)
        for key, mask in events:
            sock = key.fileobj
            data = key.data

            if mask & selectors.EVENT_READ:
                # Receive the query from the server (in the format "length query")
                query = recv_query(sock)
                print("Received query from server")
            
                # Execute the query on the client's database and send the result to the server
                tokenised_query = query.split(" ")
                query_num = int(tokenised_query[0])
                if query_num == 10: 
                    done = True
                    break
                crop = None
                year = None
                if(query_num==2 or query_num==4 or query_num==5 or query_num==6 or query_num==7):
                    crop = tokenised_query[1]
                if(query_num==5):
                    year = tokenised_query[2]
                if(query_num==9):
                    year = tokenised_query[1]
                result = process_query(db_type, query_num, crop, year)
                # print("Result:", result)

                data.msg = e_cipher.encrypt(pad_message(result).encode())

            if mask & selectors.EVENT_WRITE:
                if data.msg:
                    sock.send(data.msg)
                    print("Sent result to server")
                    data.msg = None

    sel.unregister(sock)
    sock.close()



if __name__ == "__main__":
    main()