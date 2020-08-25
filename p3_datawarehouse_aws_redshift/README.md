# Readme.md
## Udacity Data Engineering Project: Data Warehousing on the Cloud
### Context
The music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud.    
They have starting using Amazon Web Services (AWS).        
Their data resides in a S3 bucket:
- One directory of JSON logs on user activity on the app
- As well as a directory with JSON metadata on the songs in their app.

The task is to build an ETL Pipeline that:
1. Extracts their data from S3
2. Staging it in Redshift
3. Transforming data into a set of Dimensional and Fact Tables for their Analytics Team to continue finding Insights to what songs their users are listening to.
4. Prepare Queries on the Materialized views.

### External References Used:
- [Using Dist Key and Sort Key (Flydata introduction)](https://www.flydata.com/blog/amazon-redshift-distkey-and-sortkey/)
- [Copy from JSON (AWS documentation)](https://docs.aws.amazon.com/redshift/latest/dg/copy-usage_notes-copy-from-json.html)
- [Rewritten query with the COPY command](https://aws.amazon.com/premiumsupport/knowledge-center/redshift-fix-copy-analyze-statupdate-off/)
