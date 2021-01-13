from dendutils.config import get_project_config, concat_path
import os
from dendutils.ec2 import read_format_shell_script
from dendutils.s3 import concat_s3_path
from dendutils.redshift import read_format_sql
import psycopg2.sql as S


class SirenContextProvider():
    def __init__(self, config_path):
        self.config = get_project_config(config_path)


    def copy_params(self):

        return params


    def ec2_dir(self):
        return self.config.get("EC2", "DIR").rstrip('/')

    def _ec2_path(self, fname):
        self.ec2_dir() + '/' + fname.strip('/')
        return self.ec2_dir() + '/' + fname.strip('/')

    def s3_path(self):
        return concat_s3_path(self.s3_bucket, self.s3_stagingfolder, self.out_name)

    def bash_commands(self):
        # Create params used for the commands
        params = {
            'ec2dir': self.ec2_dir(),
            'url': self.config.get("DATA", "SIREN_URL"),
            'input_file_ec2': self.input_ec2,
            'csv_name': self.temp_ec2,
            'output_s3': self.marches_s3()
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
