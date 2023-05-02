import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


PG_HOST = os.environ.get('PG_HOST')
PG_PORT = os.environ.get('PG_PORT')
PG_DATABASE = os.environ.get('PG_DATABASE')
PG_USERNAME = os.environ.get('PG_USERNAME')
PG_PASSWORD = os.environ.get('PG_PASSWORD')

def engine():
    # DATABASE_URI = 'postgresql://user:password@host:port/database'
    Session = sessionmaker(bind=engine)

    DATABASE_URI = 'postgresql://{user}:{password}@{host}:{port}/{database}'.format(
        user=PG_USERNAME, password=PG_PASSWORD, host=PG_HOST,port=5432,database=PG_DATABASE)
    Engine = create_engine(DATABASE_URI)
    return [
        Engine,
        Session
    ]