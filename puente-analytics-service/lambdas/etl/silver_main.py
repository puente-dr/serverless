from silver.create_silver import fill_tables
from silver.create_drop import drop_tables, initialize_tables

DROP_TABLES = True

if __name__=="__main__": 
    if DROP_TABLES:
        drop_tables()
    initialize_tables()
    fill_tables() 