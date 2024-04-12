import sys 
import json 
import os
import socket

HOST = "127.0.0.1"
PORT = 60000


def main(): 
    if sys.argc < 2: 
        print("Usage: python3 client.py <sqlite/postgres/json>") 
        sys.exit(1)

    database = sys.argv[1]

    if(database == "sqlite"):
        from sqlite import Sqlite 
        db = Sqlite()
    elif(database == "postgres"):
        from postgres import Postgres 
        db = Postgres()
    elif(database == "json"):
        from jsondb import JsonDB 
        db = JsonDB()
    else:
        print("Invalid database type") 
        sys.exit(1)