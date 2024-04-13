import socket
import random
import selectors
import types
import sys
import psycopg2
import sqlite3
import pymongo

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
            print("Server has closed")

        if query.startswith(""):
            try: 
                msg_length, query = query.split(" ", 1)
                if len(query) >= int(msg_length):
                    break
            except:
                print("Server has closed")
                return "Error"

    return query

def calculate_average_yield_by_year(connection, db_type):
    if db_type == "postgres":
        try:
            cursor = connection.cursor()

            # SQL query to calculate average yield for each year for the last 10 years
            query = """
                SELECT year, SUM(yield) / SUM(land_area) AS average_yield
                FROM agricultural_data
                WHERE year >= EXTRACT(YEAR FROM CURRENT_DATE) - 9
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
                WHERE year >= strftime('%Y', 'now') - 9
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

            # Calculate average yield for each year for the last 10 years
            average_yield_by_year = []
            for year in range(2014, 2024):
                total_yield = 0
                total_area = 0
                for data in collection.find({"year": year}):
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


def process_query(db_type, query_num, crop):

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

    elif db_type == "sqlite":
        connection = sqlite3.connect("agricultural_data.db")

    elif db_type == "mongo":
        connection = pymongo.MongoClient("mongodb://localhost:27017/")

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

    return res
        
def main():
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

    done = False

    while True:

        if done:
            break

        events = sel.select(timeout=None)
        for key, mask in events:
            sock = key.fileobj
            data = key.data

            if mask & selectors.EVENT_READ:
                # Receive the query from the server (in the format "length query")
                query = recv_query(sock)
            
                # Execute the query on the client's database and send the result to the server
                tokenised_query = query.split(" ")
                query_num = int(tokenised_query[0])
                if query_num == 5: 
                    done = True
                    break
                crop = None
                if(query_num%2==0):
                    crop = tokenised_query[1]
                result = process_query(db_type, query_num, crop)

                data.msg = f"{len(result)} {client_id} {result}".encode()

            if mask & selectors.EVENT_WRITE:
                if data.msg:
                    sock.send(data.msg)
                    print(f"Sent result: {result}")
                    data.msg = None

    sel.unregister(sock)
    sock.close()



if __name__ == "__main__":
    main()