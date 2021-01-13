from dendutils.ec2 import getOrCreate, execute_shell_commands, terminate_instances
from dendutils.config import get_project_config
import logging
from p6_capstone.plugins.operators import AwsTestHook

class Ec2BashExecutor():
    """
    - Launches an Ec2 instance
    - Execute Bash Commands on Ec2
    - Terminate Ec2 Instance
    """
    def __init__(self,
                 aws_credentials,
                 commands,
                 ec2_config,
                 tag_key,
                 tag_value,
                 retry=10,
                 sleep=30, *args, **kwargs):
        """

        Args:
            aws_credentials: aws_credentials object, has properties aws_access_key_id, aws_secret_access_key, region_name
            commands (list): list of bash commands (string format)
            ec2_config (dict): config of the ec2 instance, has keys: ImageId, InstanceType, KeyName, IamInstanceProfileName, SecurityGroupId, StartSleep
            tag_key: Tag Key (used to find an available instance)
            tag_value: Tag Value
            retry:
            sleep:
            *args:
            **kwargs:
        """
        self.aws_access_key_id = aws_credentials.aws_access_key_id
        self.aws_secret_access_key = aws_credentials.aws_secret_access_key
        self.region_name = aws_credentials.region_name
        self.ImageId = ec2_config['ImageId']
        self.InstanceType = ec2_config['InstanceType']
        self.KeyName = ec2_config['KeyName']
        self.IamInstanceProfileName = ec2_config['IamInstanceProfileName']
        self.SecurityGroupId = ec2_config['SecurityGroupId']
        self.StartSleep = ec2_config['StartSleep']
        self.tag_key = tag_key
        self.tag_value = tag_value
        self.retry = retry
        self.sleep = sleep
        self.params = kwargs
        self.commands = commands

        pass

    def _getOrCreate(self):
        """
        Create and existing instance, or get instance id if there is already one existing matching the tag_key and tag_value
        Returns:
            str: instance_id
        """
        i_id = getOrCreate(region_name=self.region_name,
                           aws_access_key_id=self.aws_access_key_id,
                           aws_secret_access_key=self.aws_secret_access_key,
                           ImageId=self.ImageId,
                           InstanceType=self.InstanceType,
                           KeyName=self.KeyName,
                           IamInstanceProfileName=self.IamInstanceProfileName,
                           SecurityGroupId=self.SecurityGroupId,
                           tag_key=self.tag_key,
                           tag_value=self.tag_value,
                           StartSleep=self.StartSleep)
        return i_id

    def _terminate(self, InstanceId):
        assert isinstance(InstanceId, str)
        terminate_instances(region_name=self.region_name,
                            aws_access_key_id=self.aws_access_key_id,
                            aws_secret_access_key=self.aws_secret_access_key,
                            InstanceIds=[InstanceId],
                            retry=self.retry,
                            sleep=self.sleep)

    def _execute_commands(self, InstanceId, commands):
        """
        Execute list of shell commands with on Ec2 Instance
        Args:
            InstanceId (str): Ec2 InstanceId
            commands (list): list of shell commands

        Returns:
            output (dict): Output of command (with key StandardOutputContent)
        """

        output = execute_shell_commands(region_name=self.region_name,
                                        aws_access_key_id=self.aws_access_key_id,
                                        aws_secret_access_key=self.aws_secret_access_key,
                                        InstanceId=InstanceId,
                                        retry=self.retry,
                                        sleep=self.sleep,
                                        commands=commands
                                        )
        return output

    def execute(self, context=None):
        i_id = self._getOrCreate()
        try:
            output = self._execute_commands(InstanceId=i_id, commands=self.commands)
        except Exception as e:
            print(e)
        self._terminate(i_id)

