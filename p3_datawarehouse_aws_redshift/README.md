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

### 1.3. ETL Pipeline
1. JSON on S3
2. Staging Tables on RedShift
3. Star Schema on same RedShift Cluster
4. Views on same RedShift Cluster

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
#### Representation:
TODO

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

This Table will:
- Use as primary key the start_time and user_id.
- Generate a songplay ID for each row
- Use a DistStyle by user id (to speed-up queries on users and balance the distribution)
- Use a SortKey by user Id and Start_time (to simplify user and time-related queries)
- For more details on Dist Key and Sort Key, see the references below

#### 2.2.2. Dimension Tables
##### Users
users in the app
- user_id,
- first_name,
- last_name,
- gender,
- level

This table will:
- Use as primary the user id
- Use a DistStyle by User Id (To speed-up queries on users)

##### songs
songs in music database
- song_id,
- title,
- artist_id,
- year,
- duration

This table will:
- Use as primary the song id
- Use a DistStyle by song Id
    - Although here, I am not so sure it is the right choice.
    - Will that result in shuffling of data?
    - To be tested and refined in a productive environement after analyzis of performance
    - But it is certainly too big to big broadcasted using Dist Style ALL

##### artists
Artists in music database
- artist_id,
- name,
- location,
- lattitude,
- longitude

This table will:
- Use as primary the artist id
- Use a DistStyle by artist Id, with same caveats as for song id


##### time
timestamps of records in songplays broken down into specific units
- start_time,
- hour,
- day,
- week, month,
- year,
- weekday

This table will:
- Use as primary the start_time
- Use a dist style ALL to broadcast this dimension into all tables.


### 2.3. [Bonus] Output queries (Example)
#### Top 30 Most played artists
- Select the Top 30 Most played artists in the dataset.    
    - This dataset represents only a fraction of time of the records.    
    - In real productive environment we would be limiting by a time interval (i.e. Last month).    
- This query can be used to propose automated playlists.

#### Most played songs by a particular user
- This query can be used to analyze one user behaviour
- We will pick a sample user for demo

## 3. Project Structure
### Code Build
- src: Main Code folder. This is where I put the etl code and pipeline.
    - create_table.py is where I create the fact and dimension tables for the star schema in Redshift.
    - etl.py is where I load the data from S3 into staging tables on Redshift and then process that data into the star schema on Redshift.
    - sql_queries.py is where Idefine the SQL statements, which will be imported into the two other files above.
- dwh.cfg: this file is **Not** in the Github Repo. This is your configuration file.
- create_main.py: this calls the src.create_tables module and creates the tables
- etl.py: This calls the src.etl module and loads the table into the staging and then into the star schema.


#### Optional
- Add data quality checks
- Create a dashboard for analytic queries on your new database
- [Optional] Provide example queries and results for song play analysis.

### Next steps
* Use UPSERT or MERGE : [Upsert or Merge in Redshift](https://docs.aws.amazon.com/redshift/latest/dg/t_updating-inserting-using-staging-tables-.html)

### External References Used:
* [Using Dist Key for Star Schema](https://aws.amazon.com/blogs/big-data/optimizing-for-star-schemas-and-interleaved-sorting-on-amazon-redshift/)
* [Using Dist Key and Sort Key (Flydata introduction)](https://www.flydata.com/blog/amazon-redshift-distkey-and-sortkey/)
* [Copy from JSON (AWS documentation)](https://docs.aws.amazon.com/redshift/latest/dg/copy-usage_notes-copy-from-json.html)
* [Rewritten query with the COPY command](https://aws.amazon.com/premiumsupport/knowledge-center/redshift-fix-copy-analyze-statupdate-off/)
* [Redshift does not enforce primary key](http://www.sqlhaven.com/amazon-redshift-what-you-need-to-think-before-defining-primary-key/)
