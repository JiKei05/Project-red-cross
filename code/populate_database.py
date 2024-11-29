# Source: https://pynative.com/python-postgresql-tutorial/
import psycopg2
from psycopg2 import Error
import pandas as pd
from sqlalchemy import create_engine, Column


# this file doens't need to be ran anymore,
# make changes into other files

try:
    # Connect to an test database
    # NOTE:
    # 1. NEVER store credential like this in practice. This is only for testing purpose
    # 2. Replace your "database" name, "user" and "password" that we provide to test the connection to your database
    connection = psycopg2.connect(
        database="postgres",  # TO BE REPLACED
        user="postgres",  # TO BE REPLACED
        password="lhphuc2005",  # TO BE REPLACED
        host="localhost",
        port="5432"
    )

    connection.autocommit = True

    database="postgres"  # TO BE REPLACED
    user="postgres"  # TO BE REPLACED
    password="lhphuc2005"  # TO BE REPLACED
    host="localhost"
    port="5432"

    DIALECT = 'postgresql+psycopg2://'
    db_uri = "%s:%s@%s/%s" % (user, password, host, database)
    engine = create_engine(DIALECT + db_uri)
    conn=engine.connect()


# Create a cursor to perform database operations
    cursor = connection.cursor()
    print("PostgreSQL server information")
    print(connection.get_dsn_parameters(), "\n")
    # Executing a SQL query
    # Fetch result


    #inserting each sheet from data.xlsx into db
    df = pd.read_excel('./data/data.xlsx',sheet_name=None)
    for each in df:
        df[f"{each}"].to_sql(f"{each}",conn,if_exists='replace')

except (Exception, Error) as error:
    print("Error while connecting to PostgreSQL", error)

finally:
    if connection:
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")