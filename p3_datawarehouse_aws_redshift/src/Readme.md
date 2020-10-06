# Source Code Module
## Purpose
* Create the Redshift cluster
* Create the Redshift tables
* ETL the data from S3 to Redshift

## Structure
* createenv: Module for setting-up the environment (Cluster, roles, security) and the tables
* etl: Module for loading the data from s3 to Redshift

## Architecture Schema
### Users and Roles
![IAM Schema](https://github.com/ogierpaul/Udacity-Data-Engineer-NanoDegree/blob/wip/99-Appendix/IAM_Architecture_Diagram.jpg)

### Tables created

|Name|PrimaryKey|DistStyle or DistKey|SortKey|
|---|---|---|---|
|staging_events|staging_event_id|||
|staging_songs|staging_song_id|||
|songplay|(start_time, user_id)|user_id|(user_id, start_time)|
|user|user_id|user_id|user_id|
|song|song_id|artist_id|(artist_id, year)|
|artists|artist_id|artist_id|artist_id|
|time|start_time|ALL|start_time

### Data Flow Target for ETL
![DataFlow](https://github.com/ogierpaul/Udacity-Data-Engineer-NanoDegree/blob/wip/99-Appendix/DataTransformation.jpg)


## Parameters
TODO