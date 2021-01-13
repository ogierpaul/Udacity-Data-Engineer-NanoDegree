# from plugins.operators.base.ec2bashexecutor import Ec2BashExecutor
from p6_capstone.plugins.operators.base.ec2bashexecutor import Ec2BashExecutor
from dendutils.ec2.interact import read_format_shell_script
import os

class SirenToS3(Ec2BashExecutor):
    bash_template_name = '1_siren_ec2_instructions.sh'

    def __init__(self,
                 aws_credentials,
                 ec2_config,
                 tag_key,
                 tag_value,
                 siren_config,
                 *args, **kwargs):
        fp = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            self.bash_template_name
        )
        commands = read_format_shell_script(fp, **siren_config)
        super(SirenToS3, self).__init__(commands = commands, aws_credentials=aws_credentials, ec2_config=ec2_config, tag_key=tag_key, tag_value=tag_value)
        pass


class AwsCredentials():
    def __init__(self):
        self.aws_access_key_id = 'foo'
        self.aws_secret_access_key = 'bar'
        self.region_name = 'us-west-2'

if __name__ == '__main__':
    aws_credentials = AwsCredentials()
    ec2_config = {
        'ImageId':'foo',
        'InstanceType':'foo',
        'KeyName':'foo',
        'IamInstanceProfileName': 'bar',
        'SecurityGroupId':'sgid',
        'StartSleep':40
    }
    tag_key = 'Role'
    tag_value = 'Siren'
    siren_config = {
        'url': 'siren_url',
        'csvname': 'Siren_Stock.csv',
        'output_s3': 's3://output'
    }
    ss3 = SirenToS3(aws_credentials, ec2_config, tag_key, tag_value, siren_config)
    ss3.dummy()







