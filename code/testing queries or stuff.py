# Source: https://pynative.com/python-postgresql-tutorial/
import psycopg2
from psycopg2 import Error
import pandas as pd
from sqlalchemy import create_engine, Column




try:
    # Connect to an test database
    # NOTE:
    # 1. NEVER store credential like this in practice. This is only for testing purpose
    # 2. Replace your "database" name, "user" and "password" that we provide to test the connection to your database
    connection = psycopg2.connect(
        database="group_15_2024",  # TO BE REPLACED
        user="group_15_2024",  # TO BE REPLACED
        password="87k2xil8cZQv",  # TO BE REPLACED
        host="dbcourse.cs.aalto.fi",
        port="5432",
    )
    connection.autocommit = True

    database="group_15_2024"  # TO BE REPLACED
    user="group_15_2024"  # TO BE REPLACED
    password="87k2xil8cZQv"  # TO BE REPLACED
    host="dbcourse.cs.aalto.fi"
    port="5432"

    DIALECT = 'postgresql+psycopg2://'
    db_uri = "%s:%s@%s/%s" % (user, password, host, database)
    engine = create_engine(DIALECT + db_uri)
    conn=engine.connect()

    # Create a cursor to perform database operations
    cursor = connection.cursor()
    # Print PostgreSQL details
    print("PostgreSQL server information")
    print(connection.get_dsn_parameters(), "\n")
    # Executing a SQL query
    # Fetch result


    cursor.execute("""select * from volunteer;""")
    records = cursor.fetchall()
    for record in records:
        print(record)

    print("You are connected to - ", record, "\n")

except (Exception, Error) as error:
    print("Error while connecting to PostgreSQL", error)

finally:
    if connection:
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")