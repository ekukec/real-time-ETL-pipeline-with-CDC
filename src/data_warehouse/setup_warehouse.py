import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from src.common.config import config

def setup_warehouse():
    print("Setting up warehouse")

    conn = psycopg2.connect(
        host=config.POSTGRES_HOST,
        port=config.POSTGRES_PORT,
        database='postgres',
        user=config.POSTGRES_USER,
        password=config.POSTGRES_PASSWORD
    )

    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    cursor = conn.cursor()
    cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{config.POSTGRES_DB}';")
    exists = cursor.fetchone()

    if not exists:
        cursor.execute(f"CREATE DATABASE {config.POSTGRES_DB};")
        print(f"Created database : {config.POSTGRES_DB}")
    else:
        print("Database already exists")

    cursor.close()
    conn.close()

    conn = psycopg2.connect(
        host=config.POSTGRES_HOST,
        port=config.POSTGRES_PORT,
        database=config.POSTGRES_DB,
        user=config.POSTGRES_USER,
        password=config.POSTGRES_PASSWORD
    )

    cursor = conn.cursor()

    print("Creating tables")

    with open('src/data_warehouse/schema.sql', 'r') as file:
        sql = file.read()
        cursor.execute(sql)

    conn.commit()

    cursor.close()
    conn.close()

    print("Setup complete")

if __name__ == '__main__':
    setup_warehouse()