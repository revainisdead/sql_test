"""
SQL Testing

https://realpython.com/python-sql-libraries

\l - all databases
\c - connect to db
\dt - all tables
\ds - all sequences
\password {user} - change password

`sudo -u postgres psql (Optional){db_name}`
or
`sudo su - postgres`
`psql`
"""

import psycopg2
from psycopg2 import OperationalError
from psycopg2.errors import DuplicateDatabase


err_msg = "The error '{}' ocurred"


def create_connection(db_name, user, password, host, port):
    connection = None

    try:
        connection = psycopg2.connect(
            database=db_name,
            user=user,
            password=password,
            host=host,
            port=port,
        )
        print("Connection to PostgreSQL DB successful")

    except OperationalError as e:
        print(err_msg.format(e))

    return connection


def create_database(conn, query):
    conn.autocommit = True
    cursor = conn.cursor()

    try:
        cursor.execute(query)
    except OperationalError as e:
        print(err_msg.format(e))
    except DuplicateDatabase:
        # Was created previously already
        pass


def execute_query(conn, query, data=None):
    conn.autocommit = True
    cursor = conn.cursor()

    try:
        if data is not None:
            cursor.execute(query, data)
        else:
            cursor.execute(query)
        print("Query executed successfully.")
    except OperationalError as e:
        print(err_msg.format(e))


def execute_read_query(conn, query):
    cursor = conn.cursor()

    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except OperationalError as e:
        print(err_msg.format(e))


def main():
    connection = create_connection(
        "postgres",
        "postgres",
        "r0x4h7331",
        "localhost",
        "5432",
    )

    print(connection)

    # Create a new database after connecting to the default postgres database
    create_db_query = "CREATE DATABASE postgres_test"

    create_users_table = """
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            age INTEGER,
            gender TEXT,
            nationality TEXT
        )
    """

    create_posts_table = """
        CREATE TABLE IF NOT EXISTS posts (
            id SERIAL PRIMARY KEY,
            title TEXT NOT NULL,
            description TEXT NOT NULL,
            user_id INTEGER REFERENCES users(id)
        )
    """

    # Testing using varchar as foreign key
    # As suggested in postgres docs here: "postgresql.org/docs/8.3/tutorial-fk.html"

    create_likes_table = """
        CREATE TABLE IF NOT EXISTS likes (
            id SERIAL PRIMARY KEY,
            user_id INTEGER REFERENCES users(id),
            post_id INTEGER REFERENCES posts(id)
        )
    """

    create_comments_table = """
        CREATE TABLE IF NOT EXISTS comments (
            id SERIAL PRIMARY KEY,
            text TEXT NOT NULL,
            user_id INTEGER REFERENCES users(id),
            post_id INTEGER REFERENCES posts(id)
        )
    """

    create_database(connection, create_db_query)

    # Then, create new connection using the new database.
    connection = create_connection(
        "postgres_test",
        "postgres",
        "r0x4h7331",
        "localhost",
        "5432",
    )

    def create_records(length):
        return ", ".join(["%s"] * length)

    # Add data
    users = [
        ("James", 25, "male", "USA"),
        ("Leila", 32, "female", "France"),
        ("Brigitte", 35, "female", "England"),
        ("Mike", 40, "male", "Denmark"),
        ("Elizabeth", 21, "female", "Canada"),
    ]
    user_records = create_records(len(users))

    """
    insert_query = (
        "INSERT INTO users (name, age, gender, nationality) VALUES {} "
        "ON CONFLICT (name) "
        "WHERE ((name)::text = 'James'::text) DO NOTHING;"
    ).format(user_records)
    """

    # Execute
    execute_query(connection, create_users_table)
    execute_query(connection, create_posts_table)
    execute_query(connection, create_likes_table)
    execute_query(connection, create_comments_table)

    # Order:
    # reset counter
    # add new users
    # delete users
    # select all users

    # Reset counter for users table to 6 (after first 5)
    reset_counter = "ALTER SEQUENCE users_id_seq RESTART with 6"
    execute_query(connection, reset_counter)

    insert_query = (
        "INSERT INTO users (name, age, gender, nationality) VALUES {} "
    ).format(user_records)
    print(insert_query)
    execute_query(connection, insert_query, users)

    # Since I'm not sure how to use "on conflict do nothing" yet, delete extra rows.
    del_xtra_users_qry = "DELETE FROM users WHERE id >= 6 and id <= 10"
    execute_query(connection, del_xtra_users_qry)

    # Get users after creating and then deleting new users
    select_users_qry = "SELECT * FROM users"
    users = execute_read_query(connection, select_users_qry)

    for user in users:
        print(user)



if __name__ == "__main__":
    main()
