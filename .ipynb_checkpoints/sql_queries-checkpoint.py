import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN = config.get('IAM_ROLE', 'ARN')

LOG_DATA = config.get('S3', 'LOG_DATA')
SONG_DATA = config.get('S3', 'SONG_DATA')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS tbl_events_staging;"
staging_songs_table_drop = "DROP TABLE IF EXISTS tbl_songs_staging;"
songplay_table_drop = "DROP TABLE IF EXISTS tbl_songplay;"
user_table_drop = "DROP TABLE IF EXISTS tbl_user;"
song_table_drop = "DROP TABLE IF EXISTS tbl_song;"
artist_table_drop = "DROP TABLE IF EXISTS tbl_artist;"
time_table_drop = "DROP TABLE IF EXISTS tbl_time;"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS tbl_events_staging (
        artist TEXT,
        auth TEXT,
        firstName TEXT,
        gender TEXT,
        itemInSession INTEGER,
        lastName TEXT,
        length TEXT,
        level TEXT,
        location TEXT,
        method TEXT,
        page TEXT,
        registration FLOAT,
        sessionId INTEGER,
        song TEXT,
        status INTEGER,
        ts BIGINT,
        userAgent VARCHAR,
        userId INTEGER
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS tbl_songs_staging (
        song_id TEXT,
        title TEXT,
        duration FLOAT,
        year INTEGER,
        num_songs FLOAT,
        artist_id TEXT,
        artist_name TEXT,
        artist_latitude FLOAT,
        artist_longitude FLOAT,
        artist_location TEXT
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS tbl_songplay (
        songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
        start_time TIMESTAMP NOT NULL REFERENCES tbl_time(start_time),
        user_id INTEGER NOT NULL REFERENCES tbl_user(user_id),
        level TEXT,
        song_id TEXT NOT NULL REFERENCES tbl_song(song_id),
        artist_id TEXT,
        session_id INTEGER,
        location TEXT,
        user_agent TEXT
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS tbl_user (
        user_id INTEGER NOT NULL PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        gender TEXT,
        level TEXT
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS tbl_song (
        song_id TEXT PRIMARY KEY,
        title TEXT,
        artist_id TEXT NOT NULL REFERENCES tbl_artist(artist_id),
        year INTEGER,
        duration FLOAT
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS tbl_artist (
        artist_id TEXT PRIMARY KEY,
        name TEXT,
        location TEXT,
        lattitude FLOAT,
        longitude FLOAT
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS tbl_time (
        start_time TIMESTAMP PRIMARY KEY,
        hour INTEGER,
        day INTEGER,
        week INTEGER,
        month INTEGER,
        year INTEGER,
        weekday INTEGER
    );
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY tbl_events_staging
    FROM {}
    iam_role {}
    FORMAT AS JSON {}
    REGION 'us-west-2';
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
    COPY tbl_songs_staging
    FROM {}
    iam_role {}
    FORMAT AS JSON 'auto'
    REGION 'us-west-2';
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO tbl_songplay(start_time,
                         user_id,
                         level,
                         song_id,
                         artist_id,
                         session_id,
                         location,
                         user_agent)
    SELECT TIMESTAMP 'epoch' + tes.ts / 1000 * INTERVAL '1 SECOND',
           tes.userid,
           tes.level,
           tss.song_id,
           tss.artist_id,
           tes.sessionid,
           tes.location,
           tes.useragent
    FROM tbl_events_staging tes
        JOIN tbl_songs_staging tss
            ON tes.song = tss.title AND tes.artist = tss.artist_name
    WHERE tes.page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO tbl_user(user_id, 
                     first_name, 
                     last_name, 
                     gender, 
                     level)
    SELECT DISTINCT userid, 
                    firstname, 
                    lastname, 
                    gender, 
                    level 
    FROM tbl_events_staging
    WHERE userid IS NOT NULL;
""")

song_table_insert = ("""
    INSERT INTO tbl_song(song_id,
                         title,
                         artist_id,
                         year,
                         duration)
    SELECT distinct song_id, 
                    title, 
                    artist_id, 
                    year, 
                    duration 
    FROM tbl_songs_staging;
""")

artist_table_insert = ("""
    INSERT INTO tbl_artist(artist_id,
                           name,
                           location,
                           lattitude,
                           longitude)
    SELECT distinct artist_id,
           artist_name,
           artist_location,
           artist_latitude,
           artist_longitude
    FROM tbl_songs_staging;
""")

time_table_insert = ("""
    INSERT INTO tbl_time(start_time,
                     hour,
                     day,
                     week,
                     month,
                     year,
                     weekday)
    WITH tbl_distinct_start_times AS (
        SELECT DISTINCT TIMESTAMP 'epoch' + ts / 1000 * INTERVAL '1 SECOND' AS start_time, page FROM tbl_events_staging
    )
    SELECT start_time,
           EXTRACT(HOUR FROM start_time),
           EXTRACT(DAY FROM start_time),
           EXTRACT(WEEK FROM start_time),
           EXTRACT(MONTH FROM start_time),
           EXTRACT(YEAR FROM start_time),
           EXTRACT(DOW FROM start_time)
    FROM tbl_distinct_start_times
    WHERE page = 'NextSong'
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, time_table_create, artist_table_create, user_table_create, song_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
