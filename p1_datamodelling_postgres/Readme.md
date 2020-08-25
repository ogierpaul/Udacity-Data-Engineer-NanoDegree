# Sparkify project with Postgres
-------
*Udacity Project for Data Engineer Nanodegree*

## Repository structure
* data: sample datasets
* code: python routines for creating the Sparkify database and Postgres
* sql_queries: sample sql_queries
* tests: basic test cases

## Project Objective
### Purpose
* Transform raw event data from flat files into analytics-ready database
#### Data Modelling
* Use a Star Schema with Facts (Transactional) and Dimensions (Master Data)
    * User axis
    * Song axis
    * Artist axis
    * Time axis
* Note that the schema is not 3NF as the Fact table duplicates information from the song and artist

#### Steps
* Create the tables in PostgreSQL (create_tables.py)
* Load the data from the song attributes and fill the song and artist table
* Load the data from the logs and fill the time, user, songplay (fact) table

### Additional contenst
* When loading the data, we call bleach.clean to sanitize the inputs

# Sparkify Code
## Steps
1 - The data should be loaded in the 'data' folder   
2 - Run create_tables.py to create the database and tables   
3 - Run etl.py to load and transform the data   

## Added as a bonus to the project
- Sanitize inputs function with bleach to prevent JS injection attack



