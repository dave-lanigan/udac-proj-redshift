import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """This function takes a psycopg2 curser and connection executes the drop tables queries from the sql_queries file
    """
    
    
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """This function takes a psycopg2 curser and connection executes the the create tables queries from the sql_queries file
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Main function that runs both drop_tables and create_tables
    """
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()