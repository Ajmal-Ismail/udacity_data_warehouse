# Data Warehouse

## Project Overview
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.
As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

## Project Description
In this project, you'll apply what you've learned on data warehouses and AWS to build an ETL pipeline for a database hosted on Redshift. To complete the project, you will need to load data from S3 to staging tables on Redshift and execute SQL statements that create the analytics tables from these staging tables.

## Schema
### Stagging Tables
**tbl_events_staging** - Stage table to load data from logs files.
- artist TEXT,
- auth TEXT,
- firstName TEXT,
- gender TEXT,
- itemInSession INTEGER,
- lastName TEXT,
- length TEXT,
- level TEXT,
- location TEXT,
- method TEXT,
- page TEXT,
- registration FLOAT,
- sessionId INTEGER,
- song TEXT,
- status INTEGER,
- ts BIGINT,
- userAgent VARCHAR,
- userId INTEGER

**tbl_songs_staging** - Stage table to load data from songs data files.
- song_id TEXT,
- title TEXT,
- duration FLOAT,
- year INTEGER,
- num_songs FLOAT,
- artist_id TEXT,
- artist_name TEXT,
- artist_latitude FLOAT,
- artist_longitude FLOAT,
- artist_location TEXT

### Fact Table
**tbl_songplays** - Records in log data associated with song plays i.e. records with page NextSong)
- songplay_id IDENTITY(0,1) PRIMARY KEY, _(uniquely indentifies a song play)_
- start_time TIMESTAMP, _(start time of user song play. Foriegn key of time table)_
- user_id INTEGER,
- level TEXT,
- song_id TEXT,
- artist_id TEXT,
- session_id INTEGER,
- location TEXT INTEGER,
- user_agent TEXT INTEGER

### Dimension Tables
**tbl_user**  - users in the app
- user_id INTEGER PRIMARY KEY,
- first_name TEXT, 
- last_name TEXT,
- gender TEXT,
- level TEXT

**tbl_song**  - songs in music database
- song_id TEXT PRIMARY KEY, 
- title TEXT, 
- artist_id TEXT, 
- year INTEGER, 
- duration FLOAT _(Duration of the song in milliseconds)_

**tbl_artist**  - artists in music database
- artist_id TEXT PRIMARY KEY, 
- name TEXT, 
- location TEXT, 
- latitude FLOAT, 
- longitude FLOAT

**tbl_time**  - timestamps of records in  **songplays**  broken down into specific units
- start_time TIMESTAMP PRIMARY KEY, 
- hour INTEGER, 
- day INTEGER, 
- week INTEGER, 
- month INTEGER, 
- year INTEGER, 
- weekday INTEGER

## How to Run
- Fill in all the blank fields in `dwh.cfg`. You will need to create a redshift cluster and approperiate IAM role.
- Create tables by running `python create_tables.py`
- Populate tables by running `python etl.py`

## Description of Project Files
`create_tables.py` drops and creates tables. Run this file to reset tables before running ETL script.<br />
`etl.py` copys the data from S3 into stage tables and then inserts data into start schema from the stagging tables.<br />
`sql_queries.py` contains all SQL queries.<br />
`README.md` contains discussion about the project.




