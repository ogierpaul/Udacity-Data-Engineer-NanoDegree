# Udacity Data Engineering Nano-Degree
![Data Engineering Image](https://github.com/ogierpaul/Udacity-Data-Engineer-NanoDegree/blob/master/99-Appendix/data_engineer.jpeg)

## Introduction
- This [Udacity Nanodegree](https://www.udacity.com/course/data-engineer-nanodegree--nd027) teaches how to model data for analytics at scale
- The following concepts are taught:
    - Data Modelling
    - ETL with PostgreSQL and Cassandra
    - Amazon Web Services set-up: IAM, S3, Redshift, EMR instances
    - Data pipelining with Spark
    - Airflow
- After each lesson, the student has to build a project demonstrating his knowledge of the solution
- This repository display my personal propositions

![Udacity Logo](https://de.m.wikipedia.org/wiki/Datei:Udacity_logo.png)

## Context of the projects
- During this course, the student will build solutions for *Sparkify*, a fictional music streaming start-up.
- The data used is based on the [Million Song Dataset](http://millionsongdataset.com/)
- The student will use different techniques to format that data into an analytical-ready dashboard

## Lessons Plan
### Course 1: Data Modeling
#### Introduction to Data Modeling
- Understand the purpose of data modeling

- Identify the strengths and weaknesses of different types of databases and data storage techniques

### Relational Data Models with PostgreSQL
- Understand when to use a relational database

- Understand the difference between OLAP and OLTP databases

- Create normalized data tables (3NF)

- Implement denormalized schemas (e.g. STAR, Snowflake)

#### NoSQL Data Models with Cassandra
- Understand when to use NoSQL databases and how they differ from relational databases

- Create a table for a given use case. Select the appropriate primary key and clustering columns

- Create a NoSQL database in Apache Cassandra


##### Project: Data Modeling with Postgres and Apache Cassandra

### Course 2: Cloud Data Warehouses
#### Introduction to the Data Warehouses
- Understand Data Warehousing architecture

- Run an ETL process to denormalize a database (3NF to Star)

- Create an OLAP cube from facts and dimensions

- Compare columnar vs. row oriented approaches

#### Introduction to the Cloud with AWS
- Understand cloud computing

- Create an AWS account and understand their services

- Set up Amazon S3, IAM, VPC, EC2, RDS PostgreSQL

#### Implementing Data Warehouses on AWS
- Identify components of the Redshift architecture

- Run ETL process to extract data from S3 into Redshift

- Set up AWS infrastructure using Infrastructure as Code (IaC)

- Design an optimized table by selecting the appropriate distribution style and sorting key

##### Project 2: Data Infrastructure on the Cloud

## References used
In addition to the the content provided by the course, I did my own reseearch to come up with solutions.    
Also, I used inspiration from other students of the Udacity Data Engineering NanoDegree, which I quote below.    
*Disclaimer* : I did not copy-paste their code but compared my solution with theirs, and improved mine when I noticed theirs was better.   

- [Naresh Kumar](https://github.com/nareshk1290/Udacity-Data-Engineering)
- [Florencia Silvestre](https://github.com/Flor91/Data-engineering-nanodegree)
- [Sanchit Kumar](https://github.com/san089/Udacity-Data-Engineering-Projects)
- And many thanks to the students on the Udacity Chat



