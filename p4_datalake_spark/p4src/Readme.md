# Project: Data Lake
## Introduction
- A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake.
- Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.
- As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables.
- This will allow their analytics team to continue finding insights in what songs their users are listening to.
- You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

## Project Instructions
- In this project, you'll apply what you've learned on Spark and data lakes to build an ETL pipeline for a data lake hosted on S3.
- To complete the project, you will need to load data from S3, process the data into analytics tables using Spark, and load them back into S3.
- You'll deploy this Spark process on a cluster using AWS.

## Resolution
- Run the etl.ipnyb file in order to:
    - Create a spark session
    - Read the data from the S3 bucket
    - Create the normalized tables from the raw data
    - Write the data to S3 as parquet files

## Code Structure
- sparkinit.py: contains the code to init a spark session
- reads3.py: methods to read the logs and song raw data from s3 as spark dataframe
- transform.py: methods to transform the raw data into a star schema
- etl.py: Main Function, contains the configuration path and chains the different operations