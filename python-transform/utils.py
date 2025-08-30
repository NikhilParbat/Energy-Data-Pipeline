import psycopg2
from psycopg2.extras import execute_values
from config import Config

def get_db_connection():
    """
    Create a PostgreSQL connection
    """
    conn = psycopg2.connect(
        dbname=Config.POSTGRES_DB,
        user=Config.POSTGRES_USER,
        password=Config.POSTGRES_PASSWORD,
        host=Config.POSTGRES_URI,
        port=Config.POSTGRES_PORT
    )
    return conn


def insert_data(table_name, data, columns):
    """
    Insert multiple rows into PostgreSQL
    :param table_name: str - target table
    :param data: list of tuples
    :param columns: list of column names
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    # Build query dynamically
    insert_query = f"INSERT INTO {table_name} ({','.join(columns)}) VALUES %s"

    try:
        execute_values(cursor, insert_query, data)
        conn.commit()
    except Exception as e:
        print(f"❌ Error inserting data: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()
