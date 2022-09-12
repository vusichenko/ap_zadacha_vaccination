import sqlite3
from typing import Iterable

class DataStorage:
    """
    A class for connecting to the database. Most DB operations are done through this class.
    """
    def __init__(self, db_name: str):
        """
        db_name : str
            name of SQLite database file ("example_file.db")
        """
        self.db_name = db_name
        self.connection = None
        self.cursor = None

    def __del__(self):
        """
        Close connection on delete class instance
        :return:
        """
        self._disconnect()

    def _connect(self):
        if not self.connection:
            self.connection = sqlite3.connect(self.db_name)
            self.cursor = Cursor(self.connection)

        return self.connection

    def execute(self, sql_query, values: Iterable = (), commit: bool = False):
        result = None
        self._connect()
        if self.connection is None:
            print("DB: There is no connection!")
            raise sqlite3.Error

        with self.cursor as cursor:
            try:
                cursor.execute(sql_query, values)
                if commit:
                    self.connection.commit()

                result = cursor.fetchall()
            except Exception as exc:
                print(f"Raise Exception on query: {sql_query}")
                raise exc

        self._disconnect()
        return result

    def _disconnect(self):
        if self.connection is not None:
            self.connection.close()
            self.connection = None


class Cursor:
    def __init__(self, database_connection):
        self.conn = database_connection

    def __enter__(self):
        self.cursor = self.conn.cursor()
        return self.cursor

    def __exit__(self, exc_class, exc, traceback):
        self.cursor.close()
