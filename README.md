# udacity-nd027-data_pipelines
Udacity Data Engeneering Nanodegree Program - My Submission of Project: Data Pipelines

## Summary

The goal of this project is to write an automated, scheduled ETL pipeline via [Apache Airflow](https://airflow.apache.org/) that transfers data from sets in [Amazon S3](https://aws.amazon.com/s3/) into [Amazon Redshift](https://aws.amazon.com/redshift/), loads them into a star schema (1 fact and 4 dimenson tables) and eventually applies some data quality checks.

![grafik](https://user-images.githubusercontent.com/75797587/114273634-232edb80-9a1b-11eb-9adc-aca32cd72340.png)


## Raw Data

The raw data is in Amazon S3 and contains

1. \*song_data\*.jsons ('artist_id', 'artist_latitude', 'artist_location',
       'artist_longitude', 'artist_name', 'duration', 'num_songs',
       'song_id', 'title', 'year') - a subset of real data from the [Million Song Dataset](http://millionsongdataset.com/).
2. \*log_data\*.jsons ('artist', 'auth', 'firstName', 'gender', 'itemInSession',
       'lastName', 'length', 'level', 'location', 'method', 'page',
       'registration', 'sessionId', 'song', 'status', 'ts', 'userAgent',
       'userId') - [simulated](https://github.com/Interana/eventsim) activity logs from a music streaming app based on specified configurations.

## Preliminaries

Make sure you have an AWS secret and access key, a running Amazon Redshift Cluster in region `us-west-2` and a running Airflow Webserver with the parameter `dags_folder` in `/home/user_name/AirflowHome/airflow.cfg` pointing to this repo's code.

### Add AWS and Redshift Connections to Airflow

In the Airflow Webserver GUI go to `Admin -> Connections` and create:


A connection to AWS

- Conn Id: Enter `aws_credentials`.
- Conn Type: Enter `Amazon Web Services`.
- Login: Enter your Access key ID from the IAM User credentials.
- Password: Enter your Secret access key from the IAM User credentials.

A connection to Redshift

- Conn Id: Enter `redshift`.
- Conn Type: Enter `Postgres`.
- Host: Enter the endpoint of your Redshift cluster, excluding the port at the end. You can find this by selecting your cluster in the Clusters page of the Amazon Redshift console. See where this is located in the screenshot below. IMPORTANT: Make sure to NOT include the port at the end of the Redshift endpoint string.
- Schema: Enter `dev`. This is the Redshift database you want to connect to.
- Login: Enter the user name you created when launching your Redshift cluster.
- Password: Enter the password you created when launching your Redshift cluster.
- Port: Enter `5439`.


## DAGs (Directed Acyclic Graphs)

### Create and Delete Tables

Trigger the DAGs `create_tables` to create all tables and trigger the DAGs `delete_tables` to delete them.

### Data Pipeline

Trigger the DAG `sparkify_data_pipeline` import the raw sets from Udacity's S3 above and store data in the following star schema:

### A Fact Table

1. songplays (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) - records in log data associated with song plays

### Four Dimension Tables

1. users (user_id, first_name, last_name, gender, level) - users in the app
1. songs (song_id, title, artist_id, year, duration) - songs in music database
1. artists (artist_id, name, location, latitude, longitude) - artists in music database
1. time (start_time, hour, day, week, month, year, weekday) - timestamps of records in songplays broken down into specific units

## Data Quality Checks

Three checks are applied to all five tables of the star schema:

- Checking table emptyness
- Cheking for duplicate rows
- Checking for missing values

The output is given in the logs of the DAGs task `Run_data_quality_checks`: 

![grafik](https://user-images.githubusercontent.com/75797587/114273078-01ccf000-9a19-11eb-8c78-ea9ec44de5ff.png)

