from silver.create_silver import fill_tables
from silver.create_drop import drop_tables, initialize_tables
from shared_modules.utils import connection

DROP_TABLES = True
INITIALZE = True
GET_DIMENSIONS = True



if __name__=="__main__": 
    conn = connection()
    if DROP_TABLES:
        drop_tables(conn)
    if INITIALZE:
        initialize_tables(conn)
    fill_tables(conn, GET_DIMENSIONS) 
    conn.close()

