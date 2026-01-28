import psycopg2
from typing import Optional 

class PostgresConnector:
    def __init__(
        self,
        dbname: str = "db_berlin",
        user: str = "dia_user",
        password: str = "dia",
        host: str = "localhost",
        port: int = 5434,
    ):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.connection: Optional[psycopg2.extensions.connection] = None

    def connect(self):
        if not self.connection:
            self.connection = psycopg2.connect(
                dbname=self.dbname,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )
        return self.connection

    def close(self):
        if self.connection:
            self.connection.close()
            self.connection = None