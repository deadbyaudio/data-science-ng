import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Description: Loads the data existing in S3 into the staging tables
    
    Arguments:
        cur: the cursor object. 
        conn: the connection object. 
    Returns:
        None
    """
    for query in copy_table_queries:
        print(f'Executing query: {query}...')
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Description: Inserts data extracted from the staging tables into the analytics tables
    
    Arguments:
        cur: the cursor object. 
        conn: the connection object. 
    Returns:
        None
    """

    for query in insert_table_queries:
        print(f'Executing query: {query}...')
        cur.execute(query)
        conn.commit()


def main():
    """
    Description: The program entry point when this file gets executed
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()