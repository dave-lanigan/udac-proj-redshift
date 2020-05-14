import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """This function takes a psycopg2 curser and connection executes the staging tables queries from the sql_queries file.
       The queries are used to copy data from an S3 bucket into two redshift tables.
    """
    
    print("Begin loading staging tables...")
    for i,query in enumerate(copy_table_queries):
        cur.execute(query)
        conn.commit()
        print("table {} loaded..".format(i))
        
    print("Test:")
    cur.execute("select * from staging_events limit 1;")
    print( cur.fetchall()[0] )
    cur.execute("select * from staging_songs limit 1;")
    print( cur.fetchall()[0] )


def insert_tables(cur, conn):
    """This function takes a psycopg2 curser and connection executes the insert tables queries from the sql_queries file.
       The queries are used to copy data from two redshift staging tables into the star-schema tables.
    """
    print("Begin inserts...")
    for i,query in enumerate(insert_table_queries):
        cur.execute(query)
        conn.commit()
        print("query {} complete".format(i))



def main():
    """
    Main function executes load_staging_tables and insert_tables function and prints out the first row of each star-schema table for checking.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    print(config.read("dwh.cfg"))
    
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    #load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    
    print("Test:")
    cur.execute("select * from songplays limit 1;")
    print("Songplays:",cur.fetchall()[0] )
    cur.execute("select * from users limit 1;")
    print("Users:",cur.fetchall()[0] )
    cur.execute("select * from artists limit 1;")
    print("Artists:",cur.fetchall()[0] )
    cur.execute("select * from songs limit 1;")
    print("Songs:",cur.fetchall()[0] )
    cur.execute("select * from time limit 1;")
    print("Time:",cur.fetchall()[0] )
    

    conn.close()


if __name__ == "__main__":
    main()