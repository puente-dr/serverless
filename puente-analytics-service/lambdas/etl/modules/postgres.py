import os
import psycopg2

PG_HOST = os.environ.get('PG_HOST')
PG_PORT = os.environ.get('PG_PORT')
PG_DATABASE = os.environ.get('PG_DATABASE')
PG_USERNAME = os.environ.get('PG_USERNAME')
PG_PASSWORD = os.environ.get('PG_PASSWORD')

# DATABASE_URI = 'postgresql://{user}:{password}@{host}:{port}/{database}'.format(
#     user=PG_USERNAME, password=PG_PASSWORD, host=PG_HOST, port=PG_PORT, database=PG_DATABASE)
# engine = create_engine(DATABASE_URI)
# metadata = MetaData()


# def createSession():
#     # DATABASE_URI = 'postgresql://user:password@host:port/database'
#     Session = sessionmaker(bind=engine)
#     return Session

def connection():
    conn = psycopg2.connect(
        host=PG_HOST,
        database=PG_DATABASE,
        user=PG_USERNAME,
        password=PG_PASSWORD
    )
    return conn