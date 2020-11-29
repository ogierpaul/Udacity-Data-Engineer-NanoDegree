# Data Exploration Method in order to do BI
## For a dataset
### Data Ingestion
#### Data Structure coherency
- What is the primary key?
  - Are there duplicates?
- What are the fields that are almost always populated?
- What kind of fields are they (Categories/enum, free text, measures)
- What is the measure? Is there any bad data ?
- Are there nested structures? Length / structures of those nested structures?

#### Business Meaning
- What are the entities in the data model (Customers, Suppliers, Products, orders...)? What do they represent in business terms?
  - On what column are those entities IDs?
  - How can I link them to the rest of my data model?
- Is it master data or transactional data?
- What do I want to highlight and keep

### Data visualization
#### Objectives
- Validate the data ingestion step
- Explore the data set and understand it
- Create a Hello World of an app using just this table
- Create a prototype of the future app, (just one sheet, one table, one app)
- Validate with the business user the data

#### Examples
- For measures: What is the dispersion / Distribution of variables? Outliers: Keep or exclude?
- For categories: what is the frequency (value_counts) of each variable? Is there any categories to exclude?


#### Tools:
- Use BI Tool like Qliksense to drill down and explore the dataset
- Use Network visualization tools like neo4j, linkurious to see how data are connected



## The problem with the 0.1%
- In every dataset, in every information system, there is ALWAYS some data that doesn't match
  - i.e. customers without a name, address without a city, orders without amount..., dates from 1900 or from 2047
- The question is, what to do with those values? Exclude? Keep?
