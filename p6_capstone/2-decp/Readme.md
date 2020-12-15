#TODO:
- Clean Readme
- Cleanse tables
- Setup Redshift permissions automatically
- Try to get instance status before rebooting

# jq Transformations
## Purpose
- This directory stores the transformations done using jq
- They are bash scripts (see jq help)
- The goal is to transform a json file with nested arrays into a JSON Lines file readable by Redshift

## Steps
- Creates an EC2 Cluster
- Copy the data from the DECP website to Ec2
- Unzip the data
- execute jq scripts to parse the JSON arrays to JSON Lines files
- copy the json lines files to s3

## Details
### Input: one Zip file with a nested JSON array
- Stored as zip on DECP Website
- Structure: nested arrays:

```json
{"marches": [
  {"marches_attributes":"marches_values"},
  "marches_titulaires": [
      {"titulaires_attributes":"titulaires_values"}
    ] 
  ]
}
```

- As such, this file cannot be ingested in Redshift using `COPY`.

### Output
- Output is stored in AWS S3

#### marches output file
- Note this is a JSON Lines format: each line is a JSON

```json
  {"marches_attributes":"marches_values"}
  {"marches_attributes":"marches_values"}
```

- This File can be ingested in Redshift

#### Titulaires output file
- Same comment as above

```json
  {"titulaires_attributes":"titulaires_values"}
  {"titulaires_attributes":"titulaires_values"}
```

## Directory structure
- decp_parse_with_jq_ec2.py:
    - orchestrates the different steps of the transformation
    - Used to read and pass the parameters
    - To be transformed in Airflow Operators and Tasks
- Creates an Ec2 cluster
- Execute copy_raw_json and unzip it
- Execute jq_script for marches and for titulaires:
    - format jq_transfo_template.sh with jq_transfo_marches.sh
- Execute copy_formatted_jsons.sh

# Loading DECP data into Redshift
## Purpose
- Load flattened JSON Lines files into Redshift staging tables
- Transform those staging tables into tables compatible with the data model
    - Marches_attributes
    - Link Marches-Titulaires
    - Titulaires_Attributes
    
## Directory Structure
- Connect to Redshift
- Create the tables
- Load from s3
- Transform the tables
- Do Data Quality Checks

