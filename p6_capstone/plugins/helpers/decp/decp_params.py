from dendutils.config import get_project_config, concat_path
import os
from dendutils.ec2 import read_format_shell_script
from dendutils.s3 import concat_s3_path
from dendutils.redshift import read_format_sql
import psycopg2.sql as S


class DecpInputParams():
    def __init__(self, config_path):
        self.config = get_project_config(config_path)
        self.bash_fname = '1_decp_ec2_instructions.sh'
        self.create_fname = '0_decp_create_redshift.sql'
        self.stage_fname = '3_decp_stage_redshift.sql'
        self.jq_marches_fname = 'jq_marches.sh'
        self.jq_titulaires_fname = 'jq_titulaires.sh'
        self.input_ec2 = 'decp_raw.json'
        self.temp_ec2 = 'decp_temp.json'
        self.out_marches = 'marches.json'
        self.out_titulaires = 'titulaires.json'
        self.s3_bucket = self.config.get("S3", "BUCKET")
        self.s3_outputfolder = self.config.get("S3", "STAGING_FOLDER")
        self.s3_configfolder = self.config.get("S3", "DECP_CONFIGFOLDER")
        self.arn = self.config.get("IAM", "CLUSTER_IAM_ARN")
        self.region = self.config.get("REGION", "REGION")

    def _s3_path(self, fname):
        s3_key = concat_s3_path(self.s3_bucket, self.s3_outputfolder, fname)
        return s3_key

    def _copy_params(self):
        params = {
            'arn': self.arn,
            'format': 'json',
            'jsonpath': 'auto',
            'region': self.region
        }
        return params

    def copy_params_marches(self):
        params = self._copy_params().copy()
        params['table'] = 'staging_decp_marches'
        params['inputpath'] = self.marches_s3()
        return params

    def copy_params_titulaires(self):
        params = self._copy_params().copy()
        params['table'] = 'staging_decp_titulaires'
        params['inputpath'] = self.titulaires_s3()
        return params

    def ec2_dir(self):
        return self.config.get("EC2", "DIR").rstrip('/')

    def _ec2_path(self, fname):
        self.ec2_dir() + '/' + fname.strip('/')
        return self.ec2_dir() + '/' + fname.strip('/')

    def marches_ec2(self):
        return self._ec2_path(self.out_marches)

    def titulaires_ec2(self):
        return self._ec2_path(self.out_titulaires)

    def jq_marches_s3(self):
        return concat_s3_path(self.s3_bucket, self.s3_configfolder, self.jq_marches_fname)

    def jq_marches_ec2(self):
        return self._ec2_path(self.jq_marches_fname)

    def jq_titulaires_ec2(self):
        return self._ec2_path(self.jq_titulaires_fname)

    def jq_titulaires_s3(self):
        return concat_s3_path(self.s3_bucket, self.s3_configfolder, self.jq_titulaires_fname)

    def marches_s3(self):
        return concat_s3_path(self.s3_bucket, self.s3_configfolder, self.out_marches)

    def titulaires_s3(self):
        return concat_s3_path(self.s3_bucket, self.s3_configfolder, self.out_marches)

    def bash_commands(self):
        # Create params used for the commands
        params = {
            'ec2dir': self.ec2_dir(),
            'url': self.config.get("DATA", "DECP_URL"),
            'input_file_ec2': self.input_ec2,
            'output_temp_ec2': self.temp_ec2,
            'jq_marches_s3': self.jq_marches_s3(),
            'jq_marches_ec2': self.jq_marches_ec2(),
            'output_marches_ec2': self.marches_ec2(),
            'output_marches_s3': self.marches_s3(),
            'jq_titulaires_s3': self.jq_titulaires_s3(),
            'jq_titulaires_ec2': self.jq_titulaires_ec2(),
            'output_titulaires_ec2': self.titulaires_ec2(),
            'output_titulaires_s3': self.titulaires_s3()
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
