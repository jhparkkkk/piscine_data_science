import os
import psycopg2
from psycopg2 import sql
import sys

# PostgreSQL connection parameters
PG_USER = "jeepark"
PG_DB = "piscineds"
BASE_DIR = "/app/subject/customer"

def table_exists(conn, cursor, table_name):
    # Query the information schema to check if the table exists
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    table_count = cursor.fetchone()[0]

    return table_count > 1

def create_table_query(table_name: str):
    query = f"""CREATE TABLE IF NOT EXISTS {table_name} (
            event_time TIMESTAMP,
            event_type VARCHAR(255),
            product_id INT,
            price NUMERIC,
            user_id BIGINT,
            user_session UUID
        );"""
    return query

def load_data(conn, cursor, table_name: str):
    # Copy data from the CSV file into the table
    copy_data_query = f"""COPY {table_name} FROM '{os.path.join(BASE_DIR, file + '.csv')}' DELIMITER ',' CSV HEADER;"""
    cursor.execute(copy_data_query)
    conn.commit()
    print(f"Loaded data from {file} into {table_name}")

def create_table_and_load_data(file):
    if os.path.isfile(os.path.join(BASE_DIR, file + '.csv')):
        print(f"File {file} exists in {BASE_DIR}")
        table_name = os.path.splitext(file)[0]
        print(f"table_name: {table_name}")

        # create connection to the PostgreSQL database
        conn = psycopg2.connect(
            user=PG_USER,
            dbname=PG_DB,
            password="mysecretpassword",
            host="localhost",
            port="5432"
        )
        # create a cursor
        cursor = conn.cursor()
        # create table
        cursor.execute(create_table_query(table_name))
        conn.commit()
        print(f"Created table {table_name}")
        if table_exists(conn, cursor, table_name) is False:
            load_data(conn, cursor, table_name)
        # close the cursor and connection
        cursor.close()
        conn.close()
    else:
        print(f"File {file} does not exist in {BASE_DIR}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python script.py <filename>")
    else:
        file = sys.argv[1]
        create_table_and_load_data(file)
