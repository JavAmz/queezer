import sqlite3
import json
from .adapter_interface import Adapter
from ..utils import eprint

class SQLiteApiAdapter(Adapter):
    def __init__(self, local_db_file):
        try:
            self.connection = sqlite3.connect(local_db_file)
            self.cursor = self.connection.cursor()
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS function_calls (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    request TEXT,
                    response TEXT,
                    tags TEXT,
                    timestamp INTEGER,
                    runtime INTEGER
                )
            ''')
            self.connection.commit()
            eprint("Connected to the self-hosted SQLite database successfully.")
        except sqlite3.Error as e:
            eprint(f"Error connecting to the self-hosted SQLite database: {e}")

    def store(self, response, tags, args, kwargs, timestamp, duration):
        try:
            self.cursor.execute('''
                INSERT INTO function_calls (request, response, tags, timestamp, runtime)
                VALUES (?, ?, ?, ?, ?)
            ''', (json.dumps({"args": args, "kwargs": kwargs}), json.dumps(response), json.dumps(tags), timestamp, duration.microseconds//1000))
            self.connection.commit()
            print("Function call details stored successfully in the SQLite database.")
        except sqlite3.Error as e:
            print(f"Error inserting function call details into the SQLite database: {e}")