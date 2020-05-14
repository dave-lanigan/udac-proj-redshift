import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"


# CREATE TABLES

#ei log files
staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events (
                                    artist varchar(256),
                                    auth varchar,
                                    first_name varchar(256),
                                    gender varchar(256),
                                    item_in_session varchar,
                                    last_name varchar,
                                    length double precision,
                                    "level" varchar, 
                                    location varchar,
                                    "method" varchar,
                                    page varchar,
                                    registration double precision,
                                    session_id int,
                                    song varchar,
                                    status int,
                                    ts bigint,
                                    user_agent varchar,
                                    user_id int
                                    );
                                """)

#ei million song files
staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs (
                                    num_songs int,
                                    artist_id varchar,
                                    latitude double precision,
                                    longitude double precision,
                                    location text,
                                    artist_name varchar,
                                    song_id varchar,
                                    title varchar(256),
                                    duration double precision,
                                    "year" int
                                    );
                                """)

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (
                                songplay_id int IDENTITY(1, 1) PRIMARY KEY,
                                start_time bigint NOT NULL, 
                                user_id varchar NOT NULL, 
                                "level" varchar, 
                                song_id varchar, 
                                artist_id varchar, 
                                session_id int, 
                                location text, 
                                user_agent text
                                );
                           """ )

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (
                             user_id varchar PRIMARY KEY, 
                             first_name varchar, 
                             last_name varchar, 
                             gender varchar, 
                             level varchar
                             );
                        """)

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (
                             song_id varchar PRIMARY KEY, 
                             title varchar, 
                             artist_id varchar, 
                             year int, 
                             duration double precision
                            );
                        """)

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (
                               artist_id varchar PRIMARY KEY,
                               name varchar NOT NULL,
                               location text,
                               latitude double precision,
                               longitude double precision
                               );
                           """)

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
                             start_time bigint PRIMARY KEY,
                             hour varchar,
                             day varchar,
                             week varchar,
                             month varchar,
                             year varchar,
                             weekday varchar
                             );
                         """)

# STAGING TABLES

staging_events_copy = ("""copy staging_events from 's3://udacity-dend/log_data'
                            iam_role 'arn:aws:iam::736817705522:role/dwhRole'
                            region 'us-west-2'
                            json 's3://udacity-dend/log_json_path.json'
                            COMPUPDATE OFF
                            """)

staging_songs_copy = ("""copy staging_songs from 's3://udacity-dend/song_data'
                            iam_role 'arn:aws:iam::736817705522:role/dwhRole'
                            region 'us-west-2'
                            json 'auto'
                            COMPUPDATE OFF
                            """)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time,user_id,level,song_id,artist_id,session_id,location,user_agent)
        SELECT
                events.ts, 
                events.user_id, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.session_id, 
                events.location, 
                events.user_agent
                FROM (SELECT * FROM staging_events WHERE page='NextSong') events
            INNER JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration;
    """)


# songplay_table_insert = ("""INSERT INTO songplays (start_time,user_id,level,song_id,artist_id,session_id,location,user_agent)
#                             SELECT
#                                 staging_events.ts,
#                                 staging_events.user_id,
#                                 staging_events.level,
#                                 staging_songs.song_id,
#                                 staging_songs.artist_id,
#                                 staging_events.session_id,
#                                 staging_songs.location,
#                                 staging_events.user_agent

#                             FROM staging_events,staging_songs;
# #                             """)

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level) 
                            SELECT DISTINCT
                                user_id,
                                first_name,
                                last_name,
                                gender,
                                level
                            FROM staging_events
                            WHERE page='NextSong';
                            """)

song_table_insert = ("""INSERT INTO songs ( song_id, title, artist_id, year, duration)
                            SELECT DISTINCT
                                song_id,
                                title,
                                artist_id,
                                year,
                                duration
                            FROM staging_songs;
                        """)

artist_table_insert = ("""INSERT INTO artists (artist_id,name,location,latitude,longitude)
                            SELECT DISTINCT
                                artist_id,
                                artist_name,
                                location,
                                latitude,
                                longitude
                            FROM staging_songs;
                           """)

time_table_insert = ("""INSERT INTO time (start_time,hour,day,week,month,year,weekday)
                            SELECT 
                                ts,
                                extract("hour" from (TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second')),
                                extract("day" from (TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second')),
                                extract("week" from (TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second')),
                                extract("month" from (TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second')),
                                extract("year" from (TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second')),
                                extract("dow" from (TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second')) 
                            FROM staging_events;
                         """)

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
