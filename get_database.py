import cx_Oracle
import os
from dotenv import load_dotenv

load_dotenv()

user = os.getenv('DATA_BASE_USER')
password = os.getenv('DATA_BASE_USER_PASSWORD')
host = os.getenv('DATA_BASE_HOST')
port = os.getenv('DATA_BASE_PORT')
sid = os.getenv('DATA_BASE_SID')

dsn = cx_Oracle.makedsn(host, port, sid)

connection = cx_Oracle.connect(user, password, dsn)

cursor = connection.cursor()

def get_cursor():
    return cursor

def close_connection():
    cursor.close()
    connection.close()
