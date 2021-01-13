from dendutils.config import get_project_config, concat_path
import os
import json
from dendutils.ec2 import read_format_shell_script
from dendutils.s3 import concat_s3_path
from dendutils.redshift import read_format_sql
import psycopg2.sql as S

class InfoGreffeQueries():
    def __init__(self, config_path):
        self.config = get_project_config(config_path)
        self.json_fname = '1_infogreffe_query.json'
        self.bash_fname = '1_infogreffe_ec2_instructions.sh'
        self.create_fname = '0_infogreffe_create_redshift.sql'
        self.stage_fname = '3_infogreffe_stage_redshift.sql'

    def _s3_path(self):
        s3_bucket = self.config.get("S3", "BUCKET")
        s3_outputfolder = self.config.get("S3", "INFOGREFFE_OUTPUTFOLDER")
        infogreffe_csv = self.config.get("DATA", "INFOGREFFE_CSV")
        s3_key = concat_s3_path(s3_bucket, s3_outputfolder, infogreffe_csv)
        return s3_key

    def copy_params(self):
        params = {
            'arn': self.config.get("IAM", "CLUSTER_IAM_ARN"),
            'inputpath': self._s3_path(),
            'format': 'csv',
            'delimiter': ';',
            'table':'staging_infogreffe',
            'region':self.config.get("REGION", "REGION")
        }
        return params

    def _ec2_dir(self):
        return self.config.get("EC2", "DIR").rstrip('/')

    def _ec2_path(self, fname):
        ec2dir = self._ec2_dir()
        ec2_path = ec2dir + '/' + fname.strip('/')
        return ec2_path

    def _api_query(self):
        infogreffe_url = self.config.get("DATA", "INFOGREFFE_URL")
        querypath = concat_path(__file__, self.json_fname)
        with open(querypath, 'r') as f:
            query_json = json.load(f)
        query_url = infogreffe_url + '?' + "&".join([f"{k}={query_json[k]}" for k in query_json])
        return query_url

    def bash_commands(self):
        # Create params used for the commands
        params = {
            'url': self._api_query(),
            'ec2dir': self._ec2_dir(),
            'fname_ec2': self._ec2_path(fname= self.config.get("DATA", "INFOGREFFE_CSV")),
            'fname_s3': self._s3_path()
        }
        bash_path = concat_path(__file__, self.bash_fname)
        bash_commands = read_format_shell_script(fp=bash_path, **params)
        return bash_commands

    def create_queries(self):
        fp = concat_path(__file__, self.create_fname)
        queries = read_format_sql(fp)
        return queries

    def staging_queries(self):
        fp = concat_path(__file__, self.stage_fname)
        queries = read_format_sql(fp)
        return queries