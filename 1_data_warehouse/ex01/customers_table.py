import os
import zipfile
import subprocess
import psycopg2

# PostgreSQL connection parameters
PG_USER = os.environ["POSTGRES_USER"]
PG_DB = os.environ["POSTGRES_DB"]
PG_PASSWORD = os.environ["POSTGRES_PASSWORD"]
BASE_DIR = "/app/postgres/data/subject/customer/"



def handle_files():
    """
    Grants permissions to any files located in /postgres/data
    Unzip `subject.zip`
    Move data_2023_feb.csv to /postgres/data/subject/customer
    """
    print('handle files')
    directory_path = '/app/postgres/data/'
    if not os.path.exists(directory_path) or not os.path.isdir(directory_path):
        print(f"Directory '{directory_path}' does not exist.")
        return
    try:
        # grants permission 
        subprocess.run(['find', directory_path, '-type', 'f', '-exec', 'chmod', '+x', '{}', ';'])
        print(f"Permissions granted to all files in '{directory_path}'.")
        # unzip
        with zipfile.ZipFile(directory_path + 'subject.zip', 'r') as zip_ref:
            zip_ref.extractall(directory_path)
        print("Unzipped subject.zip")
        # move
        os.replace(f"{directory_path}data_2023_feb.csv", f"{BASE_DIR}data_2023_feb.csv")
        print(f"Moved data_2023_feb.csv to {BASE_DIR}")

        subprocess.run(['find', directory_path + 'subject/', '-type', 'f', '-exec', 'chmod', '+x', '{}', ';'])
        print(f"Permissions granted to all files in '{directory_path}subject/'.")
        # delete .zip
        # assert os.path.exists("subject.zip"):
            # os.remove("subject.zip")
    except Exception as e:
        print(f"An error occurred: {e}")


def verify_union_all(tables_names, conn, cursor):
    total_nb_rows = 0
    for name in tables_names:
        cursor.execute(f"""SELECT  COUNT(*)  FROM  {name};""")
        result = cursor.fetchone() 
        total_nb_rows += int(result[0])
    cursor.execute(f"""SELECT  COUNT(*)  FROM  customers;""")
    result = cursor.fetchone() 
    customers_table_total_nb_rows = int(result[0]) 
    print(f"total number of rows: {total_nb_rows}, customer tables has {customers_table_total_nb_rows}")

def full_outer_join_query(tables_names, conn, cursor):
    columns_names = "event_time, event_type, product_id, price, user_id, user_session"
    union_all_query = f"""CREATE TABLE customers
    AS
    SELECT {columns_names} FROM {tables_names[0]}
    UNION ALL
    SELECT {columns_names} FROM {tables_names[1]}
    UNION ALL
    SELECT {columns_names} FROM {tables_names[2]}
    UNION ALL
    SELECT {columns_names} FROM {tables_names[3]}
    UNION ALL
    SELECT {columns_names} FROM {tables_names[4]}
    """
    cursor.execute(union_all_query)
    conn.commit()
    print(f"Full outer join results saved to '{result_table_name}'")
    verify_union_all(tables_names, conn, cursor)

def table_empty(table_name, conn, cursor):
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    table_count = cursor.fetchone()[0]
    return table_count < 1

def copy_data_query(table_name, conn, cursor):
    copy_data_query = f"""COPY {table_name} FROM
'{os.path.join(BASE_DIR, table_name + '.csv')}' DELIMITER ',' CSV HEADER;"""
    cursor.execute(copy_data_query)
    conn.commit()
    print(f"Copied data from {table_name}.csv into {table_name}")


def create_table_query(table_name: str, conn, cursor):
    create_table_query = f"""CREATE TABLE IF NOT EXISTS {table_name} (
            event_time TIMESTAMP,
            event_type VARCHAR(255),
            product_id INT,
            price NUMERIC,
            user_id BIGINT,
            user_session UUID
        );"""
    cursor.execute(create_table_query)
    conn.commit()
    print(f"Created table {table_name}")


def load_data():
    conn = psycopg2.connect(
        user=PG_USER,
        dbname=PG_DB,
        password="mysecretpassword",
        host="localhost",
        port="5432"
    )
    cursor = conn.cursor()
    tables_names = []
    for root, dirs, files in os.walk(BASE_DIR):
        for file in files:
            if file.endswith(".csv"):
                table_name = os.path.splitext(file)[0]
                tables_names.append(table_name)
                create_table_query(table_name, conn, cursor)
            if table_empty(table_name, conn, cursor) is True:
                copy_data_query(table_name, conn, cursor)
    full_outer_join_query(tables_names, conn, cursor)
    cursor.close()
    conn.close()


if __name__ == "__main__":
    handle_files()
    load_data()

