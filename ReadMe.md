## Introduction
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

I was tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

## Project Description
In this project, I have applied what I've learned on Spark and data lakes to build an ETL pipeline for a data lake hosted on S3. To complete the project, I uploaded the data from S3, processed the data into analytics tables using Spark, and loaded them back into S3. I deployed this Spark process on a cluster using AWS.

## Process
Here is the descripition of each file used:

dl.cfg: contains credentials (access and secret access keys)
etl.py: reads files from s3, processes them and write parquet files in S3
README.md: explains the completed project in short

## Project Datasets
I worked with two datasets that reside in S3.

### Song Dataset
In this dataset, each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are filepaths to two files in this dataset.

song_data/A/B/C/TRABCEI128F424C983.json
song_data/A/A/B/TRAABJL12903CDCF1A.json

And below is an example of what a single song file, TRAABJL12903CDCF1A.json, looks like.

{
"num_songs": 1,
"artist_id": "ARJIE2Y1187B994AB7", 
"artist_latitude": null,
"artist_longitude": null, 
"artist_location": "",
"artist_name": "Line Renaud",
"song_id": "SOUPIRU12A6D4FA1E1",
"title": "Der Kleine Dompfaff",
"duration": 152.92036,
"year": 0
}

### Log Dataset
The log files in the dataset I worked with are partitioned by year and month. For example, here are filepaths to two files in this dataset.

log_data/2018/11/2018-11-12-events.json
log_data/2018/11/2018-11-13-events.json

Here is an example of how one of the files of log dataset looks like:

{
"artist":"Des'ree",
"auth":"Logged In", 
"firstName":"Kaylee",
"gender":"F",
"itemInSession":1,
"lastName":"Summers", 
"length":246.30812,
"level":"free",
"location":"Phoenix-Mesa-Scottsdale, AZ",
"method":"PUT",
"page":"NextSong",
"registration":1540344794796.0,
"sessionId":139, 
"song":"You Gotta Be",
"status":200,
"ts":1541106106796,
"userAgent":""Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.153 Safari/537.36"", 
"userId":"8"
}

## Schema for Song Play Analysis
Using the song and log datasets, I created a star schema optimized for queries on song play analysis. This includes the following tables.

### Fact Table
songplays - records in log data associated with song plays i.e. records with page NextSong
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

### Dimension Tables
users - users in the app
user_id, first_name, last_name, gender, level
songs - songs in music database
song_id, title, artist_id, year, duration
artists - artists in music database
artist_id, name, location, lattitude, longitude
time - timestamps of records in songplays broken down into specific units
start_time, hour, day, week, month, year, weekday