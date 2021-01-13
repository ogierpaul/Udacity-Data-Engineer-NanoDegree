from dendutils.s3 import concat_s3_path
from dendutils.ec2 import read_format_shell_script
import os

class UrltoS3ContextManager():
    """
    Returns the following variables:
    - s3_bucket
    - s3_stagingfolder
    - region
    - arn_cluster_role (To delete)
    - bash_fname
    - out_fname
    """
    def __init__(self, config, file, bash_fname, url, out_fname):
        # Config folder
        self.config = config
        # AWS Params
        self.s3_bucket = self.config.get("S3", "BUCKET")
        self.s3_stagingfolder = self.config.get("S3", "STAGING_FOLDER")
        self.arn = self.config.get("IAM", "CLUSTER_IAM_ARN")
        self.region = self.config.get("REGION", "REGION")
        # Url parameters
        self.url = url
        # Bash file configuration
        self.bash_fname = bash_fname
        self.commands_folder = os.path.dirname(os.path.abspath(file))
        self.fp_bash = os.path.join(os.path.dirname(os.path.abspath(file)), self.bash_fname)
        # Output file parameters
        self.out_fname = out_fname
        pass

    def fp_out_s3(self):
        """

        Returns:
            str: s3 url of object
        """
        return concat_s3_path(self.s3_bucket, self.s3_stagingfolder, self.out_fname)

    def ec2dir(self):
        mydir = '/home/ec2-user/'
        return mydir

    def fp_raw_ec2(self):
        fname = 'inputfile.txt'
        fp = os.path.join(self.ec2dir(), fname)
        return fp

    def fp_out_ec2(self):
        fp = os.path.join(self.ec2dir(), self.out_fname)
        return fp

    def file_format(self):
        f = self.out_fname.split('.')[-1]
        assert f in ['csv', 'json']
        return f

    def bash_commands(self):
        """

        Returns:
            list: list of bash commands (str)
        """
        params = {
            'ec2dir': self.ec2dir(),
            'url': self.url,
            'fp_raw_ec2': self.fp_raw_ec2(),
            'fp_out_ec2': self.fp_out_ec2(),
            'fp_out_s3': self.fp_out_s3()
        }
        commands = read_format_shell_script(fp=self.fp_bash, **params)
        return commands



