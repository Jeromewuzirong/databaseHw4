import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

load_dotenv()

# MySQL
MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = os.getenv("MYSQL_PORT", "3306")
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
MYSQL_DB = os.getenv("MYSQL_DB", "sakila")

# SQLite
SQLITE_PATH = os.getenv("SQLITE_PATH", "analytics.db")
MYSQL_URL = f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"
SQLITE_URL = f"sqlite:///{SQLITE_PATH}"

def get_mysql_engine():
    return create_engine(MYSQL_URL)

def get_sqlite_engine():
    return create_engine(SQLITE_URL)

def get_mysql_session():
    engine = get_mysql_engine()
    Session = sessionmaker(bind=engine)
    return Session()

def get_sqlite_session():
    engine = get_sqlite_engine()
    Session = sessionmaker(bind=engine)
    return Session()