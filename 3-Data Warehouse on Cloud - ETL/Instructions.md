# Instructions

---
## 0. Raw data
### Event Log data


````json
{"artist": "The Mars Volta",
  "auth": "Logged In",
  "firstName": "Kaylee",
  "gender": "F",
  "itemInSession": 5,
  "lastName": "Summers",
  "length": 380.42077,
  "level": "free",
  "location": "Phoenix-Mesa-Scottsdale, AZ",
  "method": "PUT",
  "page": "NextSong",
  "registration": 1540344794796.0,
  "sessionId": 139,
  "song": "Eriatarka",
  "status": 200,
  "ts": 1541106673796,
  "userAgent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.153 Safari/537.36",
  "userId": "8"}
````


### Song data


## 1. Schema for Song Play Analysis
Using the song and event datasets, you'll need to create a star schema optimized for queries on song play analysis.    
This includes the following tables.

### Fact Table
### Songplays
Records in event data associated with song plays i.e. records with page NextSong
- songplay_id,
- start_time,
- user_id,
- level,
- song_id,
- artist_id, session_id,
- location,
- user_agent

### Dimension Tables
#### Users
users in the app
- user_id,
- first_name,
- last_name,
- gender,
- level

#### songs
songs in music database
- song_id,
- title,
- artist_id,
- year,
- duration

#### artists
Artists in music database
- artist_id,
- name,
- location,
- lattitude,
- longitude

#### time
timestamps of records in songplays broken down into specific units
- start_time,
- hour,
- day,
- week, month,
- year,
- weekday

## 2. Project Structure
### Templates
To get started with the project, go to the workspace on the next page, where you'll find the project template. You can work on your project and submit your work through this workspace.

Alternatively, you can download the template files in the Resources tab in the classroom and work on this project on your local computer.

The project template includes four files:

- create_table.py is where you'll create your fact and dimension tables for the star schema in Redshift.
- etl.py is where you'll load data from S3 into staging tables on Redshift and then process that data into your analytics tables on Redshift.
- sql_queries.py is where you'll define you SQL statements, which will be imported into the two other files above.
- README.md is where you'll provide discussion on your process and decisions for this ETL pipeline.

### Project Steps
Below are steps you can follow to complete each component of this project.

#### Create Table Schemas
Design schemas for your fact and dimension tables:
- Write a SQL CREATE statement for each of these tables in sql_queries.py
- Complete the logic in create_tables.py to connect to the database and create these tables
- Write SQL DROP statements to drop tables in the beginning of create_tables.py if the tables already exist.
  - This way, you can run create_tables.py whenever you want to reset your database and test your ETL pipeline.

#### Set-up Cluster
- Launch a redshift cluster
- create an IAM role that has read access to S3.
- Add redshift database and IAM role info to dwh.cfg.

Test by running create_tables.py and checking the table schemas in your redshift database. You can use Query Editor in the AWS Redshift console for this.

#### Build ETL Pipeline
- Implement the logic in etl.py to load data from S3 to staging tables on Redshift.
- Implement the logic in etl.py to load data from staging tables to analytics tables on Redshift.
- Test by running etl.py after running create_tables.py and running the analytic queries on your Redshift database to compare your results with the expected results.
- Delete your redshift cluster when finished.

#### Document Process
Do the following steps in your README.md file.
- Discuss the purpose of this database in context of the startup, Sparkify, and their analytical goals.
- State and justify your database schema design and ETL pipeline.
- [Optional] Provide example queries and results for song play analysis.

#### Optional
- Add data quality checks
- Create a dashboard for analytic queries on your new database


## Notes
The SERIAL command in Postgres is not supported in Redshift. The equivalent in redshift is IDENTITY(0,1), which you can read more on in the Redshift Create Table Docs.
