# Udacity Data Engineering Project: Data Warehousing on the Cloud
## 1. Introduction
### 1.1. Context
The music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud.    
They have starting using Amazon Web Services (AWS).        
Their data resides in a S3 bucket:
- One directory of JSON logs on user activity on the app
- As well as a directory with JSON metadata on the songs in their app.

### 1.2. Tasks
Using the song and event datasets, you'll need to create a star schema optimized for queries on song play analysis.    
The task is to build an ETL Pipeline that:
1. Extracts their data from S3
2. Staging it in Redshift
3. Transforming data into a set of Dimensional and Fact Tables for their Analytics Team to continue finding Insights to what songs their users are listening to.
4. Prepare Queries on the Materialized views.


---
## 2. Data Analysis
### 2.1. Raw data:
#### 2.1.1. Event Log data example

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

#### 2.1.2. Song data example
```json
{"artist_id":"ARJNIUY12298900C91",
  "artist_latitude":null,
  "artist_location":"",
  "artist_longitude":null,
  "artist_name":"Adelitas Way",
  "duration":213.9424,
  "num_songs":1,
  "song_id":"SOBLFFE12AF72AA5BA",
  "title":"Scream",
  "year":2009}
```

#### 2.1.3. For more details:
See the Jupyter Notebook **Visualizing the JSON input data.ipynb**

### 2.2. Star Schema
#### 2.2.1. Fact Table
##### Songplays
Records in event data associated with song plays i.e. records with page NextSong
- songplay_id,
- start_time,
- user_id,
- level,
- song_id,
- artist_id, session_id,
- location,
- user_agent

#### 2.2.2. Dimension Tables
##### Users
users in the app
- user_id,
- first_name,
- last_name,
- gender,
- level

##### songs
songs in music database
- song_id,
- title,
- artist_id,
- year,
- duration

##### artists
Artists in music database
- artist_id,
- name,
- location,
- lattitude,
- longitude

##### time
timestamps of records in songplays broken down into specific units
- start_time,
- hour,
- day,
- week, month,
- year,
- weekday

### 2.3. [Bonus] Output queries (Example)
#### Top 30 Most played artists
- Select the Top 30 Most played artists in the dataset.    
    - This dataset represents only a fraction of time of the records.    
    - In real productive environment we would be limiting by a time interval (i.e. Last month).    
- This query can be used to propose automated playlists.

#### Most played songs by a particular user
- This query can be used to analyze one user behaviour
- We will pick a sample user for demo

#### Average of song length duration
- This query can be used to analyze if all users listen to the complete song or skip halfway through

## 3. Project Structure
### Code Build
- src: Main Code folder. This is where I put the etl code and pipeline.
    - create_table.py is where I create the fact and dimension tables for the star schema in Redshift.
    - etl.py is where I load the data from S3 into staging tables on Redshift and then process that data into the star schema on Redshift.
    - sql_queries.py is where Idefine the SQL statements, which will be imported into the two other files above.
- dwh.cfg: this file is **Not** in the Github Repo. This is your configuration file.
- create_main.py: this calls the src.create_tables module and creates the tables
- etl.py: This calls the src.etl module and loads the table into the staging and then into the star schema.

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
### External References Used:
- [Using Dist Key and Sort Key (Flydata introduction)](https://www.flydata.com/blog/amazon-redshift-distkey-and-sortkey/)
- [Copy from JSON (AWS documentation)](https://docs.aws.amazon.com/redshift/latest/dg/copy-usage_notes-copy-from-json.html)
- [Rewritten query with the COPY command](https://aws.amazon.com/premiumsupport/knowledge-center/redshift-fix-copy-analyze-statupdate-off/)

